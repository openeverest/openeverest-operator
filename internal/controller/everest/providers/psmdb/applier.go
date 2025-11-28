// everest-operator
// Copyright (C) 2022 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package psmdb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/AlekSi/pointer"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	"github.com/percona/percona-server-mongodb-operator/pkg/util/numstr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

const (
	psmdbDefaultConfigurationTemplate = `
      operationProfiling:
        mode: slowOp
`
	encryptionKeySuffix          = "-mongodb-encryption-key"
	defaultBackupStartingTimeout = 120
)

type applier struct {
	*Provider
	ctx context.Context //nolint:containedctx
}

func (p *applier) ResetDefaults() error {
	p.PerconaServerMongoDB.Spec = defaultSpec()
	return nil
}

func (p *applier) Metadata() error {
	if p.PerconaServerMongoDB.GetDeletionTimestamp().IsZero() {
		for _, f := range []string{
			finalizerDeletePSMDBPodsInOrder,
			finalizerDeletePSMDBPVC,
		} {
			controllerutil.AddFinalizer(p.PerconaServerMongoDB, f)
		}

		// remove legacy finalizers.
		for _, f := range []string{
			"delete-psmdb-pods-in-order",
			"delete-psmdb-pvc",
		} {
			controllerutil.RemoveFinalizer(p.PerconaServerMongoDB, f)
		}
	}
	return nil
}

func (p *applier) Paused(paused bool) {
	p.PerconaServerMongoDB.Spec.Pause = paused
}

//nolint:staticcheck
func (p *applier) AllowUnsafeConfig() {
	p.PerconaServerMongoDB.Spec.UnsafeConf = false
	useInsecureSize := p.DB.Spec.Engine.Replicas == 1 || p.DB.Spec.AllowUnsafeConfiguration
	p.PerconaServerMongoDB.Spec.Unsafe = psmdbv1.UnsafeFlags{
		TLS:                    p.DB.Spec.AllowUnsafeConfiguration,
		MongosSize:             useInsecureSize,
		ReplsetSize:            useInsecureSize,
		TerminationGracePeriod: p.DB.Spec.AllowUnsafeConfiguration,
		BackupIfUnhealthy:      p.DB.Spec.AllowUnsafeConfiguration,
	}
	// using deprecated field for backward compatibility
	if p.DB.Spec.AllowUnsafeConfiguration {
		p.PerconaServerMongoDB.Spec.TLS = &psmdbv1.TLSSpec{
			Mode: psmdbv1.TLSModeDisabled,
		}
	}
}

func (p *applier) Engine() error {
	psmdb := p.PerconaServerMongoDB
	database := p.DB
	engine := p.DBEngine

	// Even though we call ResetDefault() as the first step in the mutation process,
	// we still reset the spec here to protect against the defaults being unintentionally changed
	// from a previous mutation.
	psmdb.Spec.Replsets = defaultSpec().Replsets

	// Update CRVersion, if specified.
	currentCRVersion := p.currentPSMDBSpec.CRVersion
	specifiedCRVersion := pointer.Get(p.DB.Spec.Engine.CRVersion)
	psmdb.Spec.CRVersion = currentCRVersion
	if specifiedCRVersion != "" {
		psmdb.Spec.CRVersion = specifiedCRVersion
	}

	if database.Spec.Engine.Version == "" {
		database.Spec.Engine.Version = engine.BestEngineVersion()
	}
	engineVersion, ok := engine.Status.AvailableVersions.Engine[database.Spec.Engine.Version]
	if !ok {
		return fmt.Errorf("engine version %s not available", database.Spec.Engine.Version)
	}

	engineImagePullPolicy := p.currentPSMDBSpec.ImagePullPolicy
	// Set image pull policy explicitly only in case this is a new cluster.
	// This will prevent changing the image pull policy on upgrades and no DB restart will be triggered.
	if common.IsNewDatabaseCluster(p.DB.Status.Status) {
		engineImagePullPolicy = corev1.PullIfNotPresent
	}

	psmdb.Spec.Image = engineVersion.ImagePath
	psmdb.Spec.ImagePullPolicy = engineImagePullPolicy

	psmdb.Spec.Secrets = &psmdbv1.SecretsSpec{
		Users:         database.Spec.Engine.UserSecretsName,
		EncryptionKey: database.Name + encryptionKeySuffix,
		SSLInternal:   psmdb.GetName() + "-ssl-internal",
	}

	psmdb.Spec.VolumeExpansionEnabled = true

	if err := p.configureReplSets(); err != nil {
		return fmt.Errorf("failed to configure replsets: %w", err)
	}

	psmdb.Spec.UpgradeOptions = defaultSpec().UpgradeOptions

	return nil
}

func (p *applier) EngineFeatures() error {
	if pointer.Get(p.DB.Spec.EngineFeatures).PSMDB == nil {
		// Nothing to do.
		return nil
	}

	efApplier := NewEngineFeaturesApplier(p.Provider)
	return efApplier.ApplyFeatures(p.ctx)
}

func (p *applier) configureReplSets() error {
	database := p.DB
	psmdb := p.PerconaServerMongoDB

	// TODO: implement disabling
	if database.Spec.Sharding == nil || !database.Spec.Sharding.Enabled {
		// keep the default configuration
		if err := p.configureReplSetSpec(psmdb.Spec.Replsets[0], rsName(0)); err != nil {
			return fmt.Errorf("failed to configure default replset: %w", err)
		}
		return nil
	}

	psmdb.Spec.Sharding.Enabled = database.Spec.Sharding.Enabled

	// Add replsets if needed to fit the shards number
	// TODO: implement scale down
	shards := int(database.Spec.Sharding.Shards)
	for i := len(psmdb.Spec.Replsets); i < shards; i++ {
		rs0copy := *psmdb.Spec.Replsets[0]
		psmdb.Spec.Replsets = append(psmdb.Spec.Replsets, &rs0copy)
	}

	// configure all replsets
	for i := range shards {
		if err := p.configureReplSetSpec(psmdb.Spec.Replsets[i], rsName(i)); err != nil {
			return fmt.Errorf("failed to configure replset %s: %w", rsName(i), err)
		}
	}

	if psmdb.Spec.Sharding.ConfigsvrReplSet == nil {
		psmdb.Spec.Sharding.ConfigsvrReplSet = &psmdbv1.ReplsetSpec{}
	}
	if err := p.configureConfigsvrReplSet(psmdb.Spec.Sharding.ConfigsvrReplSet); err != nil {
		return fmt.Errorf("failed to configure configsvr replset: %w", err)
	}
	return nil
}

func rsName(i int) string {
	return fmt.Sprintf("rs%v", i)
}

func (p *applier) configureConfigsvrReplSet(configsvr *psmdbv1.ReplsetSpec) error {
	database := p.DB
	configsvr.Size = database.Spec.Sharding.ConfigServer.Replicas

	currentReplSet := p.currentPSMDBSpec.Sharding.ConfigsvrReplSet
	if err := configureStorage(p.ctx, p.C, configsvr, currentReplSet, p.DB); err != nil {
		return fmt.Errorf("failed to configure configsvr replset storage: %w", err)
	}
	return nil
}

func (p *applier) configureReplSetSpec(spec *psmdbv1.ReplsetSpec, name string) error {
	spec.Name = name
	dbEngine := p.DB.Spec.Engine
	if dbEngine.Config != "" {
		spec.Configuration = psmdbv1.MongoConfiguration(dbEngine.Config)
	}
	if spec.Configuration == "" {
		// Config missing from the DatabaseCluster CR and the template (if any), apply the default one
		spec.Configuration = psmdbv1.MongoConfiguration(psmdbDefaultConfigurationTemplate)
	}

	spec.Size = dbEngine.Replicas

	currentReplSet := p.getCurrentReplSet(name)
	if err := configureStorage(p.ctx, p.C, spec, currentReplSet, p.DB); err != nil {
		return fmt.Errorf("failed to configure replset storage: %w", err)
	}

	if !dbEngine.Resources.CPU.IsZero() {
		spec.MultiAZ.Resources.Limits[corev1.ResourceCPU] = dbEngine.Resources.CPU
	}
	if !dbEngine.Resources.Memory.IsZero() {
		spec.MultiAZ.Resources.Limits[corev1.ResourceMemory] = dbEngine.Resources.Memory
	}
	return nil
}

func (p *applier) Proxy() error {
	psmdb := p.PerconaServerMongoDB
	database := p.DB

	// Even though we call ResetDefault() as the first step in the mutation process,
	// we still reset the spec here to protect against the defaults being unintentionally changed
	// from a previous mutation.
	psmdb.Spec.Sharding.Mongos = defaultSpec().Sharding.Mongos

	// if sharding is disabled, expose the default replset directly as usual according to db proxy settings
	if database.Spec.Sharding == nil || !database.Spec.Sharding.Enabled {
		psmdb.Spec.Sharding.Enabled = false
		if err := p.exposeDefaultReplSet(&psmdb.Spec.Replsets[0].Expose); err != nil {
			return err
		}
		return nil
	}

	// otherwise configure psmdb.Spec.Sharding.Mongos according to the db proxy settings
	size := database.Spec.Engine.Replicas
	if database.Spec.Proxy.Replicas != nil {
		size = *database.Spec.Proxy.Replicas
	}

	if psmdb.Spec.Sharding.Mongos == nil {
		psmdb.Spec.Sharding.Mongos = &psmdbv1.MongosSpec{
			MultiAZ: psmdbv1.MultiAZ{
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{},
				},
			},
		}
	}
	psmdb.Spec.Sharding.Mongos.Size = size

	if !database.Spec.Proxy.Resources.CPU.IsZero() {
		psmdb.Spec.Sharding.Mongos.Resources.Limits[corev1.ResourceCPU] = database.Spec.Proxy.Resources.CPU
	}
	if !database.Spec.Proxy.Resources.Memory.IsZero() {
		psmdb.Spec.Sharding.Mongos.Resources.Limits[corev1.ResourceMemory] = database.Spec.Proxy.Resources.Memory
	}
	err := p.exposeShardedCluster(&psmdb.Spec.Sharding.Mongos.Expose)
	if err != nil {
		return err
	}

	// disable direct exposure of replsets since .psmdb.Spec.Sharding.Mongos works like proxy
	psmdb.Spec.Replsets[0].Expose.Enabled = false
	psmdb.Spec.Replsets[0].Expose.ExposeType = corev1.ServiceTypeClusterIP
	return nil
}

func (p *applier) exposeShardedCluster(expose *psmdbv1.MongosExpose) error {
	database := p.DB
	switch database.Spec.Proxy.Expose.Type {
	case everestv1alpha1.ExposeTypeInternal:
		expose.ExposeType = corev1.ServiceTypeClusterIP
		expose.ServiceAnnotations = map[string]string{}
	case everestv1alpha1.ExposeTypeExternal:
		expose.ExposeType = corev1.ServiceTypeLoadBalancer
		expose.LoadBalancerSourceRanges = database.Spec.Proxy.Expose.IPSourceRangesStringArray()

		annotations, err := common.GetAnnotations(p.ctx, p.C, p.DB)
		if err != nil {
			return err
		}

		expose.ServiceAnnotations = annotations
	default:
		return fmt.Errorf("invalid expose type %s", database.Spec.Proxy.Expose.Type)
	}
	return nil
}

func (p *applier) exposeDefaultReplSet(expose *psmdbv1.ExposeTogglable) error {
	database := p.DB
	switch database.Spec.Proxy.Expose.Type {
	case everestv1alpha1.ExposeTypeInternal:
		expose.Enabled = true
		expose.ExposeType = corev1.ServiceTypeClusterIP
		expose.ServiceAnnotations = map[string]string{}
	case everestv1alpha1.ExposeTypeExternal:
		expose.Enabled = true
		expose.ExposeType = corev1.ServiceTypeLoadBalancer
		expose.LoadBalancerSourceRanges = database.Spec.Proxy.Expose.IPSourceRangesStringArray()

		annotations, err := common.GetAnnotations(p.ctx, p.C, p.DB)
		if err != nil {
			return err
		}

		expose.ServiceAnnotations = annotations
	default:
		return fmt.Errorf("invalid expose type %s", database.Spec.Proxy.Expose.Type)
	}
	return nil
}

func (p *applier) Backup() error {
	// Even though we call ResetDefault() as the first step in the mutation process,
	// we still reset the spec here to protect against the defaults being unintentionally changed
	// from a previous mutation.
	p.PerconaServerMongoDB.Spec.Backup = defaultSpec().Backup

	spec, err := p.genPSMDBBackupSpec()
	if err != nil {
		return err
	}
	p.PerconaServerMongoDB.Spec.Backup = spec
	return nil
}

func (p *applier) DataSource() error {
	database := p.DB
	if database.Spec.DataSource == nil {
		// Nothing to do.
		return nil
	}
	if database.Status.Status != everestv1alpha1.AppStateReady ||
		p.PerconaServerMongoDB.Status.BackupVersion == "" {
		// Wait for the cluster to be ready.
		// NOTE: .status.backupVersion is set after psmdb cluster is fully ready and takes some time after that.
		// We need to wait for it to be set to ensure that restores from backups (pbm) work correctly.
		return nil
	}
	return common.ReconcileDBRestoreFromDataSource(p.ctx, p.C, p.DB)
}

func (p *applier) Monitoring() error {
	monitoring, err := common.GetDBMonitoringConfig(p.ctx, p.C, p.DB)
	if err != nil {
		return err
	}
	// Even though we call ResetDefault() as the first step in the mutation process,
	// we still reset the spec here to protect against the defaults being unintentionally changed
	// from a previous mutation.
	p.PerconaServerMongoDB.Spec.PMM = defaultSpec().PMM

	if monitoring.Spec.Type == everestv1alpha1.PMMMonitoringType {
		if err := p.applyPMMCfg(monitoring); err != nil {
			return err
		}
	}
	return nil
}

func (p *applier) PodSchedulingPolicy() error {
	// NOTE: this method shall be called after Engine() and Proxy() methods
	// because it extends the engine and proxy specs with the affinity rules.
	//
	// The following cases are possible:
	// 1. The user did not specify a .spec.podSchedulingPolicyName -> do nothing.
	// 2. The user specified a .spec.podSchedulingPolicyName, but it does not exist -> return error.
	// 3. The user specified a .spec.podSchedulingPolicyName, and it exists, but it is not applicable to the upstream cluster -> return error.
	// 4. The user specified a .spec.podSchedulingPolicyName, and it exists, but affinity configuration is absent there -> do nothing.
	// 5. The user specified a .spec.podSchedulingPolicyName, and it exists, and it is applicable to the upstream cluster ->
	// copy the affinity rules to the upstream cluster spec from policy.

	psmdb := p.PerconaServerMongoDB
	pspName := p.DB.Spec.PodSchedulingPolicyName

	if pspName == "" {
		// Covers case 1.
		return nil
	}

	psp, err := common.GetPodSchedulingPolicy(p.ctx, p.C, pspName)
	if err != nil {
		// Not found or other error.
		// Covers case 2.
		return err
	}

	if psp.Spec.EngineType != everestv1alpha1.DatabaseEnginePSMDB {
		// Covers case 3.
		return fmt.Errorf("requested pod scheduling policy='%s' is not applicable to engineType='%s'",
			pspName, everestv1alpha1.DatabaseEnginePSMDB)
	}

	if !psp.HasRules() {
		// Nothing to do.
		// Covers case 4.
		// The affinity rules will be applied later once admin sets them in policy.
		return nil
	}

	// Covers case 5.
	pspAffinityConfig := psp.Spec.AffinityConfig
	// ReplicaSets
	if pspAffinityConfig.PSMDB.Engine != nil {
		engineAffinityConfig := &psmdbv1.PodAffinity{
			Advanced: pspAffinityConfig.PSMDB.Engine.DeepCopy(),
		}
		for i := range len(psmdb.Spec.Replsets) {
			// Copy Affinity rules to Engine pods spec from policy.
			psmdb.Spec.Replsets[i].MultiAZ.Affinity = engineAffinityConfig
		}
	}

	// check whether sharding is requested for the upstream cluster.
	if p.DB.Spec.Sharding != nil && p.DB.Spec.Sharding.Enabled {
		// Config Server
		if pspAffinityConfig.PSMDB.ConfigServer != nil {
			// Copy Affinity rules to ConfigServer pods spec from policy.
			psmdb.Spec.Sharding.ConfigsvrReplSet.MultiAZ.Affinity = &psmdbv1.PodAffinity{
				Advanced: pspAffinityConfig.PSMDB.ConfigServer.DeepCopy(),
			}
		}

		// Proxy
		if pspAffinityConfig.PSMDB.Proxy != nil {
			// Copy Affinity rules to Mongos pods spec from policy.
			psmdb.Spec.Sharding.Mongos.MultiAZ.Affinity = &psmdbv1.PodAffinity{
				Advanced: pspAffinityConfig.PSMDB.Proxy.DeepCopy(),
			}
		}
	}

	return nil
}

func (p *applier) applyPMMCfg(monitoring *everestv1alpha1.MonitoringConfig) error {
	psmdb := p.PerconaServerMongoDB
	database := p.DB
	ctx := p.ctx
	c := p.C

	psmdb.Spec.PMM = psmdbv1.PMMSpec{
		Enabled: true,
		Image:   monitoring.Status.PMMServerVersion.DefaultPMMClientImage(),
		Resources: getPMMResources(common.IsNewDatabaseCluster(p.DB.Status.Status),
			&p.DB.Spec, &p.currentPSMDBSpec),
	}

	if monitoring.Spec.PMM.Image != "" {
		psmdb.Spec.PMM.Image = monitoring.Spec.PMM.Image
	}

	pmmURL, err := url.Parse(monitoring.Spec.PMM.URL)
	if err != nil {
		return errors.Join(err, errors.New("invalid monitoring URL"))
	}
	psmdb.Spec.PMM.ServerHost = pmmURL.Hostname()

	apiKey, err := common.GetSecretFromMonitoringConfig(ctx, c, monitoring)
	if err != nil {
		return err
	}

	// We avoid setting the controller reference for the Secret here.
	// The PSMDB operator handles Secret cleanup automatically as part of the `delete-psmdb-pvc` finalizer process.
	// If we were to set the controller reference, the Secret would be deleted before the PSMDB cluster, leading to
	// the PSMDB operator recreating the Secret and restarting the DB pods to sync updated user information,
	// which would cause unnecessary restarts.
	setControllerRef := false
	if err := common.CreateOrUpdateSecretData(ctx, c, database, psmdb.Spec.Secrets.Users,
		map[string][]byte{
			monitoring.Status.PMMServerVersion.PMMSecretKeyName(p.DB.Spec.Engine.Type): []byte(apiKey),
		},
		setControllerRef,
	); err != nil {
		return err
	}
	return nil
}

// getPMMResources returns the PMM resources to be used for the DB cluster.
// The logic is as follows:
//  1. If this is a new DB cluster, use the resources specified in the DB spec, if any.
//     Otherwise, use the default resources based on the DB size.
//  2. If this is an existing DB cluster and the DB size has changed, use the resources specified in the DB spec, if any.
//     Otherwise, use the default resources based on the new DB size.
//  3. If this is an existing DB cluster and PMM was enabled before, use the resources specified in the DB spec, if any.
//     Otherwise, use the current PMM resources.
//  4. If this is an existing DB cluster and PMM was not enabled before, use the resources specified in the DB spec, if any.
//     Otherwise, use the default resources based on the DB size.
func getPMMResources(isNewDBCluster bool,
	dbSpec *everestv1alpha1.DatabaseClusterSpec,
	curPsmdbSpec *psmdbv1.PerconaServerMongoDBSpec,
) corev1.ResourceRequirements {
	requestedResources := pointer.Get(dbSpec.Monitoring).Resources

	if isNewDBCluster {
		// This is new DB cluster.
		// DB spec may contain custom PMM resources -> merge them with defaults.
		// If none are specified, default resources will be used.
		return common.MergeResources(requestedResources,
			common.CalculatePMMResources(dbSpec.Engine.Size()))
	}

	// Prepare current DB cluster size
	var currentReplSet psmdbv1.ReplsetSpec
	for _, replset := range curPsmdbSpec.Replsets {
		if replset.Name == rsName(0) {
			currentReplSet = *replset
			break
		}
	}

	currentDBSize := (&everestv1alpha1.Engine{Resources: everestv1alpha1.Resources{
		Memory: *currentReplSet.Resources.Requests.Memory(),
	}}).Size()

	if dbSpec.Engine.Size() != currentDBSize {
		// DB cluster size has changed -> need to update PMM resources.
		// DB spec may contain custom PMM resources -> merge them with defaults.
		return common.MergeResources(requestedResources,
			common.CalculatePMMResources(dbSpec.Engine.Size()))
	}

	if curPsmdbSpec.PMM.Enabled {
		// DB cluster is not new and PMM was enabled before.
		// DB spec may contain new custom PMM resources -> merge them with previously used PMM resources.
		return common.MergeResources(requestedResources, curPsmdbSpec.PMM.Resources)
	}

	// DB cluster is not new and PMM was not enabled before. Now it is being enabled.
	// DB spec may contain custom PMM resources -> merge them with defaults.
	return common.MergeResources(requestedResources,
		common.CalculatePMMResources(dbSpec.Engine.Size()))
}

// Add the storages used by restores.
func (p *applier) addBackupStoragesByRestores(
	restoreList *everestv1alpha1.DatabaseClusterRestoreList,
	storages map[string]psmdbv1.BackupStorageSpec,
) error {
	database := p.DB
	ctx := p.ctx
	c := p.C
	// Add used restore backup storages to the list
	for _, restore := range restoreList.Items {
		// If the restore has already completed, skip it.
		if restore.IsComplete() {
			continue
		}
		// Restores using the BackupSource field instead of the
		// DBClusterBackupName don't need to have the storage defined, skip
		// them.
		if restore.Spec.DataSource.DBClusterBackupName == "" {
			continue
		}

		backup := &everestv1alpha1.DatabaseClusterBackup{}
		key := types.NamespacedName{Name: restore.Spec.DataSource.DBClusterBackupName, Namespace: restore.GetNamespace()}
		err := c.Get(ctx, key, backup)
		if err != nil {
			return errors.Join(err, errors.New("failed to get DatabaseClusterBackup"))
		}

		// Check if we already fetched that backup storage
		if _, ok := storages[backup.Spec.BackupStorageName]; ok {
			continue
		}

		backupStorage, err := common.GetBackupStorage(ctx, c, backup.Spec.BackupStorageName, database.GetNamespace())
		if err != nil {
			return err
		}

		// configures the storage for S3.
		handleStorageS3 := func() {
			storages[backup.Spec.BackupStorageName] = psmdbv1.BackupStorageSpec{
				Type: psmdbv1.BackupStorageS3,
				S3: psmdbv1.BackupStorageS3Spec{
					Bucket:                backupStorage.Spec.Bucket,
					CredentialsSecret:     backupStorage.Spec.CredentialsSecretName,
					Region:                backupStorage.Spec.Region,
					EndpointURL:           backupStorage.Spec.EndpointURL,
					Prefix:                common.BackupStoragePrefix(database),
					InsecureSkipTLSVerify: !pointer.Get(backupStorage.Spec.VerifyTLS),
				},
			}
		}
		// configures the storage for Azure.
		handleStorageAzure := func() {
			storages[backup.Spec.BackupStorageName] = psmdbv1.BackupStorageSpec{
				Type: psmdbv1.BackupStorageAzure,
				Azure: psmdbv1.BackupStorageAzureSpec{
					Container:         backupStorage.Spec.Bucket,
					Prefix:            common.BackupStoragePrefix(database),
					CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
				},
			}
		}

		switch backupStorage.Spec.Type {
		case everestv1alpha1.BackupStorageTypeS3:
			handleStorageS3()
		case everestv1alpha1.BackupStorageTypeAzure:
			handleStorageAzure()
		default:
			return fmt.Errorf("unsupported backup storage type %s for %s", backupStorage.Spec.Type, backupStorage.Name)
		}
	}
	return nil
}

// Add the storages used by the DatabaseClusterBackup objects.
func (p *applier) addBackupStoragesByDatabaseClusterBackups(
	backupList *everestv1alpha1.DatabaseClusterBackupList,
	storages map[string]psmdbv1.BackupStorageSpec,
) error {
	database := p.DB
	ctx := p.ctx
	c := p.C

	for _, backup := range backupList.Items {
		// Check if we already fetched that backup storage
		if _, ok := storages[backup.Spec.BackupStorageName]; ok {
			continue
		}

		backupStorage, err := common.GetBackupStorage(ctx, c, backup.Spec.BackupStorageName, database.GetNamespace())
		if err != nil {
			return err
		}

		// configures the storage for S3.
		handleStorageS3 := func() {
			storages[backup.Spec.BackupStorageName] = psmdbv1.BackupStorageSpec{
				Type: psmdbv1.BackupStorageS3,
				S3: psmdbv1.BackupStorageS3Spec{
					Bucket:                backupStorage.Spec.Bucket,
					CredentialsSecret:     backupStorage.Spec.CredentialsSecretName,
					Region:                backupStorage.Spec.Region,
					EndpointURL:           backupStorage.Spec.EndpointURL,
					Prefix:                common.BackupStoragePrefix(database),
					InsecureSkipTLSVerify: !pointer.Get(backupStorage.Spec.VerifyTLS),
				},
			}
		}
		// configures the storage for Azure.
		handleStorageAzure := func() {
			storages[backup.Spec.BackupStorageName] = psmdbv1.BackupStorageSpec{
				Type: psmdbv1.BackupStorageAzure,
				Azure: psmdbv1.BackupStorageAzureSpec{
					Container:         backupStorage.Spec.Bucket,
					Prefix:            common.BackupStoragePrefix(database),
					CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
				},
			}
		}

		switch backupStorage.Spec.Type {
		case everestv1alpha1.BackupStorageTypeS3:
			handleStorageS3()
		case everestv1alpha1.BackupStorageTypeAzure:
			handleStorageAzure()
		default:
			return fmt.Errorf("unsupported backup storage type %s for %s", backupStorage.Spec.Type, backupStorage.Name)
		}
	}
	return nil
}

func (p *applier) getBackupTasks(
	storages map[string]psmdbv1.BackupStorageSpec,
) ([]psmdbv1.BackupTaskSpec, error) {
	database := p.DB
	c := p.C
	ctx := p.ctx

	tasks := make([]psmdbv1.BackupTaskSpec, 0, len(database.Spec.Backup.Schedules))
	for _, schedule := range database.Spec.Backup.Schedules {
		if !schedule.Enabled {
			continue
		}

		backupStorage, err := common.GetBackupStorage(ctx, c, schedule.BackupStorageName, database.GetNamespace())
		if err != nil {
			return nil, err
		}

		// configures the storage for S3.
		handleStorageS3 := func() {
			storages[schedule.BackupStorageName] = psmdbv1.BackupStorageSpec{
				Type: psmdbv1.BackupStorageS3,
				S3: psmdbv1.BackupStorageS3Spec{
					Bucket:                backupStorage.Spec.Bucket,
					CredentialsSecret:     backupStorage.Spec.CredentialsSecretName,
					Region:                backupStorage.Spec.Region,
					EndpointURL:           backupStorage.Spec.EndpointURL,
					Prefix:                common.BackupStoragePrefix(database),
					InsecureSkipTLSVerify: !pointer.Get(backupStorage.Spec.VerifyTLS),
				},
			}
		}
		// configures the storage for Azure.
		handleStorageAzure := func() {
			storages[schedule.BackupStorageName] = psmdbv1.BackupStorageSpec{
				Type: psmdbv1.BackupStorageAzure,
				Azure: psmdbv1.BackupStorageAzureSpec{
					Container:         backupStorage.Spec.Bucket,
					Prefix:            common.BackupStoragePrefix(database),
					CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
				},
			}
		}

		switch backupStorage.Spec.Type {
		case everestv1alpha1.BackupStorageTypeS3:
			handleStorageS3()
		case everestv1alpha1.BackupStorageTypeAzure:
			handleStorageAzure()
		default:
			return nil, fmt.Errorf("unsupported backup storage type %s for %s", backupStorage.Spec.Type, backupStorage.Name)
		}

		tasks = append(tasks, psmdbv1.BackupTaskSpec{
			Name:        schedule.Name,
			Enabled:     true,
			Schedule:    schedule.Schedule,
			Keep:        int(schedule.RetentionCopies),
			StorageName: schedule.BackupStorageName,
		})
	}
	return tasks, nil
}

func (p *applier) pbmImage() (string, error) {
	engine := p.DBEngine
	database := p.DB

	bestBackupVersion := engine.BestBackupVersion(database.Spec.Engine.Version)
	backupVersion, ok := engine.Status.AvailableVersions.Backup[bestBackupVersion]
	if !ok {
		return "", fmt.Errorf("backup version %s not available", bestBackupVersion)
	}

	currentImage := p.currentPSMDBSpec.Backup.Image
	desiredImage := backupVersion.ImagePath
	// Preserve the current image until the CRVersion has been updated.
	if currentImage != "" && database.Status.RecommendedCRVersion != nil {
		return currentImage, nil
	}
	return desiredImage, nil
}

func (p *applier) genPSMDBBackupSpec() (psmdbv1.BackupSpec, error) {
	database := p.DB
	ctx := p.ctx
	c := p.C

	emptySpec := psmdbv1.BackupSpec{Enabled: false}

	pbmImage, err := p.pbmImage()
	if err != nil {
		return emptySpec, fmt.Errorf("cannot get backup image: %w", err)
	}

	// Initialize backup spec.
	psmdbBackupSpec := psmdbv1.BackupSpec{
		Enabled: true,
		Image:   pbmImage,
		PITR: psmdbv1.PITRSpec{
			Enabled: database.Spec.Backup.PITR.Enabled,
		},
		Configuration: psmdbv1.BackupConfig{
			BackupOptions: &psmdbv1.BackupOptions{
				Timeouts: &psmdbv1.BackupTimeouts{Starting: pointer.ToUint32(defaultBackupStartingTimeout)},
			},
		},

		// XXX: Remove this once templates will be available
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1G"),
				corev1.ResourceCPU:    resource.MustParse("300m"),
			},
		},
	}
	// We preserve the settings for existing DBs, otherwise restarts are seen when upgrading Everest.
	// TODO: Remove this once we figure out how to apply such spec changes without automatic restarts.
	// See: https://perconadev.atlassian.net/browse/EVEREST-1413
	if p.DB.Status.Status == everestv1alpha1.AppStateReady {
		psmdbBackupSpec.Configuration = p.currentPSMDBSpec.Backup.Configuration
	}
	storages := make(map[string]psmdbv1.BackupStorageSpec)

	// List DatabaseClusterBackup objects for this database
	backupList, err := common.DatabaseClusterBackupsThatReferenceObject(ctx, c, consts.DBClusterBackupDBClusterNameField, database.GetNamespace(), database.GetName())
	if err != nil {
		return emptySpec, err
	}
	// Add the storages used by the DatabaseClusterBackup objects
	if err := p.addBackupStoragesByDatabaseClusterBackups(backupList, storages); err != nil {
		return emptySpec, err
	}

	// List DatabaseClusterRestore objects for this database
	restoreList, err := common.DatabaseClusterRestoresThatReferenceObject(ctx, c, consts.DBClusterRestoreDBClusterNameField, database.GetNamespace(), database.GetName())
	if err != nil {
		return emptySpec, err
	}
	// Add the storages used by restores.
	if err := p.addBackupStoragesByRestores(restoreList, storages); err != nil {
		return emptySpec, err
	}

	// If there are no schedules, just return the storages used in
	// DatabaseClusterBackup objects
	if len(database.Spec.Backup.Schedules) == 0 {
		storages, err = withMarkedAsMain(storages)
		if err != nil {
			return emptySpec, err
		}
		psmdbBackupSpec.Storages = storages
		return psmdbBackupSpec, nil
	}

	tasks, err := p.getBackupTasks(storages)
	if err != nil {
		return emptySpec, err
	}

	if database.Spec.Backup.PITR.Enabled {
		interval := database.Spec.Backup.PITR.UploadIntervalSec
		if interval != nil {
			// the integer amount of minutes. default 10
			intervalMinutes := *interval / int(time.Minute.Seconds())
			psmdbBackupSpec.PITR.OplogSpanMin = numstr.NumberString(strconv.Itoa(intervalMinutes))
		}
	}

	storages, err = withMarkedAsMain(storages)
	if err != nil {
		return emptySpec, err
	}

	psmdbBackupSpec.Storages = storages
	psmdbBackupSpec.Tasks = tasks

	return psmdbBackupSpec, nil
}

func withMarkedAsMain(storages map[string]psmdbv1.BackupStorageSpec) (map[string]psmdbv1.BackupStorageSpec, error) {
	if len(storages) > 1 {
		return nil, common.ErrPSMDBOneStorageRestriction
	}
	for key, storage := range storages {
		// mark the first and the single one as Main
		storage.Main = true
		storages[key] = storage
		return storages, nil
	}
	return storages, nil
}

func (p *applier) getCurrentReplSet(name string) *psmdbv1.ReplsetSpec {
	for _, replset := range p.currentPSMDBSpec.Replsets {
		if replset.Name == name {
			return replset
		}
	}
	return nil
}

func configureStorage(
	ctx context.Context,
	c client.Client,
	desired *psmdbv1.ReplsetSpec,
	current *psmdbv1.ReplsetSpec,
	db *everestv1alpha1.DatabaseCluster,
) error {
	var currentSize resource.Quantity
	if current != nil &&
		current.VolumeSpec != nil &&
		current.VolumeSpec.PersistentVolumeClaim.PersistentVolumeClaimSpec != nil {
		currentSize = current.VolumeSpec.PersistentVolumeClaim.PersistentVolumeClaimSpec.Resources.Requests[corev1.ResourceStorage]
	}

	setStorageSize := func(size resource.Quantity) {
		desired.VolumeSpec = &psmdbv1.VolumeSpec{
			PersistentVolumeClaim: psmdbv1.PVCSpec{
				PersistentVolumeClaimSpec: &corev1.PersistentVolumeClaimSpec{
					StorageClassName: db.Spec.Engine.Storage.Class,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: size,
						},
					},
				},
			},
		}
	}

	return common.ConfigureStorage(ctx, c, db, currentSize, setStorageSize)
}
