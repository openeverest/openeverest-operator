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

package pxc

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/url"

	"github.com/AlekSi/pointer"
	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

const (
	haProxyProbesTimeout = 30
	passwordMaxLen       = 20
	passwordMinLen       = 16
	passSymbols          = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		"0123456789" +
		"!#$%&()+,-.<=>?@[]^_{}~"
)

var errInvalidDataSourceConfiguration = errors.New("invalid dataSource configuration")

type applier struct {
	*Provider
	ctx context.Context //nolint:containedctx
}

func (p *applier) ResetDefaults() error {
	p.PerconaXtraDBCluster.Spec = defaultSpec()
	return nil
}

func (p *applier) Metadata() error {
	if p.PerconaXtraDBCluster.GetDeletionTimestamp().IsZero() {
		for _, f := range []string{
			finalizerDeletePXCPodsInOrder,
			finalizerDeletePXCPVC,
			finalizerDeletePXCSSL,
		} {
			controllerutil.AddFinalizer(p.PerconaXtraDBCluster, f)
		}

		// remove legacy finalizers.
		for _, f := range []string{
			"delete-pxc-pods-in-order",
			"delete-pxc-pvc",
			"delete-ssl",
		} {
			controllerutil.RemoveFinalizer(p.PerconaXtraDBCluster, f)
		}
	}
	return nil
}

func (p *applier) Paused(paused bool) {
	p.Provider.PerconaXtraDBCluster.Spec.Pause = paused
}

//nolint:staticcheck
func (p *applier) AllowUnsafeConfig() {
	p.PerconaXtraDBCluster.Spec.AllowUnsafeConfig = false
	useInsecureSize := p.DB.Spec.Engine.Replicas == 1 || p.DB.Spec.AllowUnsafeConfiguration
	p.PerconaXtraDBCluster.Spec.Unsafe = pxcv1.UnsafeFlags{
		// using deprecated field for backward compatibility
		TLS:               p.DB.Spec.AllowUnsafeConfiguration,
		PXCSize:           useInsecureSize,
		ProxySize:         useInsecureSize,
		BackupIfUnhealthy: p.DB.Spec.AllowUnsafeConfiguration,
	}
}

func configureStorage(
	ctx context.Context,
	c client.Client,
	desired *pxcv1.PerconaXtraDBClusterSpec,
	current *pxcv1.PerconaXtraDBClusterSpec,
	db *everestv1alpha1.DatabaseCluster,
) error {
	getCurrentStorageSize := func() resource.Quantity {
		if db.Status.Status == everestv1alpha1.AppStateNew ||
			current == nil ||
			current.PXC == nil ||
			current.PXC.PodSpec == nil ||
			current.PXC.PodSpec.VolumeSpec == nil ||
			current.PXC.PodSpec.VolumeSpec.PersistentVolumeClaim == nil {
			return resource.Quantity{}
		}
		return current.PXC.PodSpec.VolumeSpec.PersistentVolumeClaim.Resources.Requests[corev1.ResourceStorage]
	}
	currentSize := getCurrentStorageSize()

	setStorageSize := func(size resource.Quantity) {
		desired.PXC.PodSpec.VolumeSpec = &pxcv1.VolumeSpec{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimSpec{
				StorageClassName: db.Spec.Engine.Storage.Class,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: size,
					},
				},
			},
		}
	}

	return common.ConfigureStorage(ctx, c, db, currentSize, setStorageSize)
}

// generatePass generates a random password.
func generatePass() ([]byte, error) {
	randLenDelta, err := rand.Int(rand.Reader, big.NewInt(int64(passwordMaxLen-passwordMinLen)))
	if err != nil {
		return nil, err
	}

	b := make([]byte, passwordMinLen+randLenDelta.Int64())
	for i := range b {
		randInt, err := rand.Int(rand.Reader, big.NewInt(int64(len(passSymbols))))
		if err != nil {
			return nil, err
		}
		b[i] = passSymbols[randInt.Int64()]
	}

	return b, nil
}

func (p *applier) ensureSecretHasProxyAdminPassword(secretsName string) error {
	secret := &corev1.Secret{}
	err := p.C.Get(p.ctx, types.NamespacedName{
		Name:      p.DB.Spec.Engine.UserSecretsName,
		Namespace: p.DB.GetNamespace(),
	}, secret)
	if client.IgnoreNotFound(err) != nil {
		return err
	}

	if _, ok := secret.Data["proxyadmin"]; ok {
		return nil
	}

	// Generate a new password.
	pass, err := generatePass()
	if err != nil {
		return err
	}

	err = common.CreateOrUpdateSecretData(p.ctx, p.C, p.DB, secretsName, map[string][]byte{
		"proxyadmin": pass,
	}, false)
	if err != nil {
		return err
	}

	return nil
}

func (p *applier) Engine() error {
	engine := p.DBEngine
	if p.DB.Spec.Engine.Version == "" {
		p.DB.Spec.Engine.Version = engine.BestEngineVersion()
	}

	pxc := p.PerconaXtraDBCluster

	// Even though we call ResetDefault() as the first step in the mutation process,
	// we still reset the spec here to protect against the defaults being unintentionally changed
	// from a previous mutation.
	pxc.Spec.PXC = defaultSpec().PXC

	// Set the CRVersion specified on the DB, otherwise preserve the current one.
	currentCRVersion := p.currentPerconaXtraDBClusterSpec.CRVersion
	specifiedCRVersion := pointer.Get(p.DB.Spec.Engine.CRVersion)
	pxc.Spec.CRVersion = currentCRVersion
	if specifiedCRVersion != "" {
		pxc.Spec.CRVersion = specifiedCRVersion
	}

	pxc.Spec.SecretsName = p.DB.Spec.Engine.UserSecretsName
	// XXX: PXCO v1.17.0 has a bug in the auto generation of the secrets where
	// there's a 1.5% chance that the proxyadmin password starts with the '*'
	// character, which will be rejected by the secret validation, leaving the
	// cluster stuck in the 'initializing' state.
	// See: https://perconadev.atlassian.net/browse/K8SPXC-1568
	// To work around this, we need to set the password ourselves to bypass the
	// buggy auto generation.
	if p.DBEngine.Status.OperatorVersion == "1.17.0" {
		err := p.ensureSecretHasProxyAdminPassword(pxc.Spec.SecretsName)
		if err != nil {
			return fmt.Errorf("failed to set proxyadmin password: %w", err)
		}
	}

	pxc.Spec.PXC.PodSpec.Size = p.DB.Spec.Engine.Replicas
	pxc.Spec.PXC.PodSpec.Configuration = p.DB.Spec.Engine.Config

	pxcEngineVersion, ok := engine.Status.AvailableVersions.Engine[p.DB.Spec.Engine.Version]
	if !ok {
		return fmt.Errorf("engine version %s not available", p.DB.Spec.Engine.Version)
	}
	pxc.Spec.PXC.Image = pxcEngineVersion.ImagePath

	var engineImagePullPolicy corev1.PullPolicy
	// Set image pull policy explicitly only in case this is a new cluster.
	// This will prevent changing the image pull policy on upgrades and no DB restart will be triggered.
	if common.IsNewDatabaseCluster(p.DB.Status.Status) {
		engineImagePullPolicy = corev1.PullIfNotPresent
	} else if p.currentPerconaXtraDBClusterSpec.PXC != nil {
		engineImagePullPolicy = p.currentPerconaXtraDBClusterSpec.PXC.ImagePullPolicy
	}
	pxc.Spec.PXC.ImagePullPolicy = engineImagePullPolicy

	pxc.Spec.VolumeExpansionEnabled = true
	if err := configureStorage(p.ctx, p.C, &pxc.Spec, &p.currentPerconaXtraDBClusterSpec, p.DB); err != nil {
		return err
	}

	if !p.DB.Spec.Engine.Resources.CPU.IsZero() {
		pxc.Spec.PXC.PodSpec.Resources.Limits[corev1.ResourceCPU] = p.DB.Spec.Engine.Resources.CPU
		pxc.Spec.PXC.PodSpec.Resources.Requests[corev1.ResourceCPU] = p.DB.Spec.Engine.Resources.CPU
	}
	if !p.DB.Spec.Engine.Resources.Memory.IsZero() {
		pxc.Spec.PXC.PodSpec.Resources.Limits[corev1.ResourceMemory] = p.DB.Spec.Engine.Resources.Memory
		pxc.Spec.PXC.PodSpec.Resources.Requests[corev1.ResourceMemory] = p.DB.Spec.Engine.Resources.Memory
	}
	hasDBSpecChanged := func() bool {
		return p.DB.Status.ObservedGeneration > 0 && p.DB.Status.ObservedGeneration != p.DB.Generation
	}
	// We preserve the settings for existing DBs, otherwise restarts are seen when upgrading Everest.
	// Additionally, we also need to check for the spec changes, otherwise the user can never voluntarily change the resource setting.
	// TODO: Remove this once we figure out how to apply such spec changes without automatic restarts.
	// See: https://perconadev.atlassian.net/browse/EVEREST-1413
	if p.DB.Status.Status == everestv1alpha1.AppStateReady && !hasDBSpecChanged() {
		pxc.Spec.PXC.PodSpec.Resources = p.currentPerconaXtraDBClusterSpec.PXC.PodSpec.Resources
	}

	switch p.DB.Spec.Engine.Size() {
	case everestv1alpha1.EngineSizeSmall:
		pxc.Spec.PXC.PodSpec.LivenessProbes.TimeoutSeconds = 450
		pxc.Spec.PXC.PodSpec.ReadinessProbes.TimeoutSeconds = 450
	case everestv1alpha1.EngineSizeMedium:
		pxc.Spec.PXC.PodSpec.LivenessProbes.TimeoutSeconds = 451
		pxc.Spec.PXC.PodSpec.ReadinessProbes.TimeoutSeconds = 451
	case everestv1alpha1.EngineSizeLarge:
		pxc.Spec.PXC.PodSpec.LivenessProbes.TimeoutSeconds = 600
		pxc.Spec.PXC.PodSpec.ReadinessProbes.TimeoutSeconds = 600
	}

	pxc.Spec.UpgradeOptions = defaultSpec().UpgradeOptions

	return nil
}

func (p *applier) EngineFeatures() error {
	// Nothing to do here for PXC
	return nil
}

func (p *applier) Backup() error {
	// Even though we call ResetDefault() as the first step in the mutation process,
	// we still reset the spec here to protect against the defaults being unintentionally changed
	// from a previous mutation.
	p.PerconaXtraDBCluster.Spec.Backup = defaultSpec().Backup

	bkp, err := p.genPXCBackupSpec()
	if err != nil {
		return err
	}
	p.PerconaXtraDBCluster.Spec.Backup = bkp
	return nil
}

func (p *applier) Proxy() error {
	proxySpec := p.DB.Spec.Proxy
	// Apply proxy config.
	switch proxySpec.Type {
	case everestv1alpha1.ProxyTypeHAProxy:
		// Even though we call ResetDefault() as the first step in the mutation process,
		// we still reset the spec here to protect against the defaults being unintentionally changed
		// from a previous mutation.
		p.PerconaXtraDBCluster.Spec.HAProxy = defaultSpec().HAProxy
		if err := p.applyHAProxyCfg(); err != nil {
			return err
		}
	case everestv1alpha1.ProxyTypeProxySQL:
		// Even though we call ResetDefault() as the first step in the mutation process,
		// we still reset the spec here to protect against the defaults being unintentionally changed
		// from a previous mutation.
		p.PerconaXtraDBCluster.Spec.ProxySQL = defaultSpec().ProxySQL
		if err := p.applyProxySQLCfg(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid proxy type %s", proxySpec.Type)
	}
	return nil
}

func (p *applier) DataSource() error {
	if p.DB.Spec.DataSource == nil {
		// Nothing to do.
		return nil
	}
	// Do not restore from datasource until the cluster is ready.
	if p.DB.Status.Status != everestv1alpha1.AppStateReady {
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
	p.PerconaXtraDBCluster.Spec.PMM = defaultSpec().PMM
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

	pxc := p.PerconaXtraDBCluster
	// --------------------------------- //
	if pxc.Spec.HAProxy != nil {
		pxc.Spec.HAProxy.PodSpec.Affinity = nil
	}

	if pxc.Spec.ProxySQL != nil {
		pxc.Spec.ProxySQL.PodSpec.Affinity = nil
	}
	// --------------------------------- //

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

	if psp.Spec.EngineType != everestv1alpha1.DatabaseEnginePXC {
		// Covers case 3.
		return fmt.Errorf("requested pod scheduling policy='%s' is not applicable to engineType='%s'",
			pspName, everestv1alpha1.DatabaseEnginePXC)
	}

	if !psp.HasRules() {
		// Nothing to do.
		// Covers case 4.
		// The affinity rules will be applied later once admin sets them in policy.
		return nil
	}

	// Covers case 5.
	pspAffinityConfig := psp.Spec.AffinityConfig

	// Copy Affinity rules to Engine pods spec from policy
	if pspAffinityConfig.PXC.Engine != nil {
		pxc.Spec.PXC.PodSpec.Affinity = &pxcv1.PodAffinity{
			Advanced: pspAffinityConfig.PXC.Engine.DeepCopy(),
		}
	}

	// Copy Affinity rules to Proxy pod spec from policy
	if pspAffinityConfig.PXC.Proxy != nil {
		proxyAffinityConfig := &pxcv1.PodAffinity{
			Advanced: pspAffinityConfig.PXC.Proxy.DeepCopy(),
		}

		switch p.DB.Spec.Proxy.Type {
		case everestv1alpha1.ProxyTypeHAProxy:
			pxc.Spec.HAProxy.PodSpec.Affinity = proxyAffinityConfig
		case everestv1alpha1.ProxyTypeProxySQL:
			pxc.Spec.ProxySQL.PodSpec.Affinity = proxyAffinityConfig
		default:
			return fmt.Errorf("invalid proxy type %s", p.DB.Spec.Proxy.Type)
		}
	}

	return nil
}

func defaultSpec() pxcv1.PerconaXtraDBClusterSpec {
	maxUnavailable := intstr.FromInt(1)
	return pxcv1.PerconaXtraDBClusterSpec{
		UpdateStrategy: pxcv1.SmartUpdateStatefulSetStrategyType,
		UpgradeOptions: pxcv1.UpgradeOptions{
			Apply:    "never",
			Schedule: "0 4 * * *",
		},
		PXC: &pxcv1.PXCSpec{
			PodSpec: &pxcv1.PodSpec{
				ServiceType: corev1.ServiceTypeClusterIP,
				PodDisruptionBudget: &pxcv1.PodDisruptionBudgetSpec{
					MaxUnavailable: &maxUnavailable,
				},
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1G"),
						corev1.ResourceCPU:    resource.MustParse("600m"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1G"),
						corev1.ResourceCPU:    resource.MustParse("600m"),
					},
				},
			},
		},
		PMM: &pxcv1.PMMSpec{},
		HAProxy: &pxcv1.HAProxySpec{
			PodSpec: pxcv1.PodSpec{
				Enabled: false,
				Resources: corev1.ResourceRequirements{
					// XXX: Remove this once templates will be available
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1G"),
						corev1.ResourceCPU:    resource.MustParse("600m"),
					},
				},
				ReadinessProbes: corev1.Probe{TimeoutSeconds: haProxyProbesTimeout},
				LivenessProbes:  corev1.Probe{TimeoutSeconds: haProxyProbesTimeout},
			},
		},
		ProxySQL: &pxcv1.ProxySQLSpec{
			PodSpec: pxcv1.PodSpec{
				Enabled: false,
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1G"),
						corev1.ResourceCPU:    resource.MustParse("600m"),
					},
				},
			},
		},
	}
}

func (p *applier) applyHAProxyCfg() error {
	haProxy := defaultSpec().HAProxy
	haProxy.PodSpec.Enabled = true

	switch p.DB.Spec.Engine.Size() {
	case everestv1alpha1.EngineSizeSmall:
		haProxy.PodSpec.Resources = haProxyResourceRequirementsSmall
	case everestv1alpha1.EngineSizeMedium:
		haProxy.PodSpec.Resources = haProxyResourceRequirementsMedium
	case everestv1alpha1.EngineSizeLarge:
		haProxy.PodSpec.Resources = haProxyResourceRequirementsLarge
	}

	if p.DB.Spec.Proxy.Replicas == nil {
		haProxy.PodSpec.Size = p.DB.Spec.Engine.Replicas
	} else {
		haProxy.PodSpec.Size = *p.DB.Spec.Proxy.Replicas
	}

	switch p.DB.Spec.Proxy.Expose.Type {
	case everestv1alpha1.ExposeTypeInternal:
		expose := pxcv1.ServiceExpose{
			Enabled:     true,
			Type:        corev1.ServiceTypeClusterIP,
			Annotations: map[string]string{},
		}
		haProxy.ExposePrimary = expose
		haProxy.ExposeReplicas = &pxcv1.ReplicasServiceExpose{ServiceExpose: expose}
	case everestv1alpha1.ExposeTypeExternal:
		annotations, err := common.GetAnnotations(p.ctx, p.C, p.DB)
		if err != nil {
			return err
		}
		expose := pxcv1.ServiceExpose{
			Enabled:                  true,
			Type:                     corev1.ServiceTypeLoadBalancer,
			LoadBalancerSourceRanges: p.DB.Spec.Proxy.Expose.IPSourceRangesStringArray(),
			Annotations:              annotations,
		}
		haProxy.ExposePrimary = expose
		haProxy.ExposeReplicas = &pxcv1.ReplicasServiceExpose{ServiceExpose: expose}
	default:
		return fmt.Errorf("invalid expose type %s", p.DB.Spec.Proxy.Expose.Type)
	}

	haProxy.PodSpec.Configuration = p.DB.Spec.Proxy.Config
	if haProxy.PodSpec.Configuration == "" {
		haProxy.PodSpec.Configuration = haProxyConfigDefault
	}

	// Ensure there is a env vars secret for HAProxy
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pxcHAProxyEnvSecretName,
			Namespace: p.DB.GetNamespace(),
		},
	}
	if err := controllerutil.SetControllerReference(p.DB, secret, p.C.Scheme()); err != nil {
		return fmt.Errorf("failed to set controller reference for secret %s", pxcHAProxyEnvSecretName)
	}
	if _, err := controllerutil.CreateOrUpdate(p.ctx, p.C, secret, func() error {
		secret.Data = haProxyEnvVars
		return nil
	}); err != nil {
		return fmt.Errorf("failed to create or update secret %w", err)
	}
	haProxy.PodSpec.EnvVarsSecretName = pxcHAProxyEnvSecretName

	haProxyAvailVersions, ok := p.DBEngine.Status.AvailableVersions.Proxy[everestv1alpha1.ProxyTypeHAProxy]
	if !ok {
		return errors.New("haproxy version not available")
	}
	bestHAProxyVersion := haProxyAvailVersions.BestVersion()
	haProxyVersion, ok := haProxyAvailVersions[bestHAProxyVersion]
	if !ok {
		return fmt.Errorf("haproxy version %s not available", bestHAProxyVersion)
	}

	// We can update the HAProxy image name only in case the CRVersions match.
	// Otherwise we keep the image unchanged.
	image := haProxyVersion.ImagePath
	if p.currentPerconaXtraDBClusterSpec.HAProxy != nil && p.DBEngine.Status.OperatorVersion != p.DB.Status.CRVersion {
		image = p.currentPerconaXtraDBClusterSpec.HAProxy.PodSpec.Image
	}
	haProxy.PodSpec.Image = image

	var haProxyImagePullPolicy corev1.PullPolicy
	// Set image pull policy explicitly only in case this is a new cluster.
	// This will prevent changing the image pull policy on upgrades and no DB restart will be triggered.
	if common.IsNewDatabaseCluster(p.DB.Status.Status) {
		haProxyImagePullPolicy = corev1.PullIfNotPresent
	} else if p.currentPerconaXtraDBClusterSpec.HAProxy != nil {
		haProxyImagePullPolicy = p.currentPerconaXtraDBClusterSpec.HAProxy.PodSpec.ImagePullPolicy
	}
	haProxy.PodSpec.ImagePullPolicy = haProxyImagePullPolicy

	shouldUpdateRequests := common.IsNewDatabaseCluster(p.DB.Status.Status)
	if !p.DB.Spec.Proxy.Resources.CPU.IsZero() {
		// When the limits are changed, triggers a pod restart, hence ensuring the requests are applied automatically (next block),
		// as it depends on the cluster being in the 'init' state (shouldUpdateRequests).
		haProxy.PodSpec.Resources.Limits[corev1.ResourceCPU] = p.DB.Spec.Proxy.Resources.CPU
		// Prior to 1.3.0, we did not set the requests, and this led to some issues.
		// We now set the requests to the same value as the limits, however, we need to ensure that
		// they're not automatically applied when Everest is upgraded, otherwise it leads to a proxy restart.
		if shouldUpdateRequests ||
			p.currentPerconaXtraDBClusterSpec.HAProxy.Resources.Requests.Cpu().
				Equal(p.DB.Spec.Proxy.Resources.CPU) {
			haProxy.PodSpec.Resources.Requests[corev1.ResourceCPU] = p.DB.Spec.Proxy.Resources.CPU
		}
	}
	if !p.DB.Spec.Proxy.Resources.Memory.IsZero() {
		// When the limits are changed, triggers a pod restart, hence ensuring the requests are applied automatically (next block),
		// as it depends on the cluster being in the 'init' state (shouldUpdateRequests).
		haProxy.PodSpec.Resources.Limits[corev1.ResourceMemory] = p.DB.Spec.Proxy.Resources.Memory
		// Prior to 1.3.0, we did not set the requests, and this led to some issues.
		// We now set the requests to the same value as the limits, however, we need to ensure that
		// they're not automatically applied when Everest is upgraded, otherwise it leads to a proxy restart.
		if shouldUpdateRequests ||
			p.currentPerconaXtraDBClusterSpec.HAProxy.Resources.Requests.Memory().
				Equal(p.DB.Spec.Proxy.Resources.Memory) {
			haProxy.PodSpec.Resources.Requests[corev1.ResourceMemory] = p.DB.Spec.Proxy.Resources.Memory
		}
	}

	p.PerconaXtraDBCluster.Spec.HAProxy = haProxy
	return nil
}

func (p *applier) applyProxySQLCfg() error {
	proxySQL := defaultSpec().ProxySQL
	proxySQL.Enabled = true

	if p.DB.Spec.Proxy.Replicas == nil {
		// By default we set the same number of replicas as the engine
		proxySQL.Size = p.DB.Spec.Engine.Replicas
	} else {
		proxySQL.Size = *p.DB.Spec.Proxy.Replicas
	}

	switch p.DB.Spec.Proxy.Expose.Type {
	case everestv1alpha1.ExposeTypeInternal:
		expose := pxcv1.ServiceExpose{
			Enabled:     true,
			Type:        corev1.ServiceTypeClusterIP,
			Annotations: map[string]string{},
		}
		proxySQL.Expose = expose
	case everestv1alpha1.ExposeTypeExternal:
		annotations, err := common.GetAnnotations(p.ctx, p.C, p.DB)
		if err != nil {
			return err
		}
		expose := pxcv1.ServiceExpose{
			Enabled:                  true,
			Type:                     corev1.ServiceTypeLoadBalancer,
			LoadBalancerSourceRanges: p.DB.Spec.Proxy.Expose.IPSourceRangesStringArray(),
			Annotations:              annotations,
		}
		proxySQL.Expose = expose
	default:
		return fmt.Errorf("invalid expose type %s", p.DB.Spec.Proxy.Expose.Type)
	}

	proxySQL.Configuration = p.DB.Spec.Proxy.Config

	proxySQLAvailVersions, ok := p.DBEngine.Status.AvailableVersions.Proxy[everestv1alpha1.ProxyTypeProxySQL]
	if !ok {
		return errors.New("proxysql version not available")
	}

	bestProxySQLVersion := proxySQLAvailVersions.BestVersion()
	proxySQLVersion, ok := proxySQLAvailVersions[bestProxySQLVersion]
	if !ok {
		return fmt.Errorf("proxysql version %s not available", bestProxySQLVersion)
	}

	// We can update the image name only in case the CRVersions match.
	// Otherwise we keep the image unchanged.
	image := proxySQLVersion.ImagePath
	if p.currentPerconaXtraDBClusterSpec.ProxySQL != nil && p.DBEngine.Status.OperatorVersion != p.DB.Status.CRVersion {
		image = p.currentPerconaXtraDBClusterSpec.ProxySQL.Image
	}
	proxySQL.Image = image

	shouldUpdateRequests := common.IsNewDatabaseCluster(p.DB.Status.Status)
	if !p.DB.Spec.Proxy.Resources.CPU.IsZero() {
		// When the limits are changed, triggers a pod restart, hence ensuring the requests are applied automatically (next block),
		// as it depends on the cluster being in the 'init' state (shouldUpdateRequests).
		proxySQL.Resources.Limits[corev1.ResourceCPU] = p.DB.Spec.Proxy.Resources.CPU
		// Prior to 1.3.0, we did not set the requests, and this led to some issues.
		// We now set the requests to the same value as the limits, however, we need to ensure that
		// they're not automatically applied when Everest is upgraded, otherwise it leads to a proxy restart.
		if shouldUpdateRequests ||
			p.currentPerconaXtraDBClusterSpec.HAProxy.Resources.Requests.Cpu().
				Equal(p.DB.Spec.Proxy.Resources.CPU) {
			proxySQL.Resources.Requests[corev1.ResourceCPU] = p.DB.Spec.Proxy.Resources.CPU
		}
	}
	if !p.DB.Spec.Proxy.Resources.Memory.IsZero() {
		// When the limits are changed, triggers a pod restart, hence ensuring the requests are applied automatically (next block),
		// as it depends on the cluster being in the 'init' state (shouldUpdateRequests).
		proxySQL.Resources.Limits[corev1.ResourceMemory] = p.DB.Spec.Proxy.Resources.Memory
		// Prior to 1.3.0, we did not set the requests, and this led to some issues.
		// We now set the requests to the same value as the limits, however, we need to ensure that
		// they're not automatically applied when Everest is upgraded, otherwise it leads to a proxy restart.
		if shouldUpdateRequests ||
			p.currentPerconaXtraDBClusterSpec.HAProxy.Resources.Requests.Cpu().
				Equal(p.DB.Spec.Proxy.Resources.CPU) {
			proxySQL.Resources.Requests[corev1.ResourceMemory] = p.DB.Spec.Proxy.Resources.Memory
		}
	}
	p.PerconaXtraDBCluster.Spec.ProxySQL = proxySQL
	return nil
}

func (p *applier) applyPMMCfg(monitoring *everestv1alpha1.MonitoringConfig) error {
	pxc := p.PerconaXtraDBCluster

	pmmImage := monitoring.Status.PMMServerVersion.DefaultPMMClientImage()
	if monitoring.Spec.PMM.Image != "" {
		pmmImage = monitoring.Spec.PMM.Image
	}

	pxc.Spec.PMM = &pxcv1.PMMSpec{
		Enabled:         true,
		Image:           pmmImage,
		ImagePullPolicy: p.getPMMImagePullPolicy(),
		Resources: getPMMResources(common.IsNewDatabaseCluster(p.DB.Status.Status),
			&p.DB.Spec, &p.currentPerconaXtraDBClusterSpec),
	}

	//nolint:godox
	// TODO (K8SPXC-1367): Set PMM container LivenessProbes timeouts once possible.
	pmmURL, err := url.Parse(monitoring.Spec.PMM.URL)
	if err != nil {
		return errors.Join(err, errors.New("invalid monitoring URL"))
	}
	pxc.Spec.PMM.ServerHost = pmmURL.Hostname()

	apiKey, err := common.GetSecretFromMonitoringConfig(p.ctx, p.C, monitoring)
	if err != nil {
		return err
	}

	return common.CreateOrUpdateSecretData(p.ctx, p.C, p.DB, pxc.Spec.SecretsName, map[string][]byte{
		monitoring.Status.PMMServerVersion.PMMSecretKeyName(p.DB.Spec.Engine.Type): []byte(apiKey),
	}, false)
}

// getPMMImagePullPolicy returns the PMM image pull policy to be used for the DB cluster.
// The logic is as follows:
// 1. If this is a new DB cluster, use PullIfNotPresent.
// 2. If this is an existing DB cluster and PMM was enabled before, use the current image pull policy to prevent changes in spec.
// 3. If this is an existing DB cluster and PMM was not enabled before, use PullIfNotPresent.
func (p *applier) getPMMImagePullPolicy() corev1.PullPolicy {
	if common.IsNewDatabaseCluster(p.DB.Status.Status) {
		// This is new DB cluster.
		// Set image pull policy and PMM resources explicitly.
		return corev1.PullIfNotPresent
	}

	if p.currentPerconaXtraDBClusterSpec.PMM != nil && p.currentPerconaXtraDBClusterSpec.PMM.Enabled {
		// DB cluster is not new and PMM was enabled before.
		// Copy the current image pull policy to prevent changes in spec.
		return p.currentPerconaXtraDBClusterSpec.PMM.ImagePullPolicy
	}

	// DB cluster is not new and PMM was not enabled before. Now it is being enabled.
	return corev1.PullIfNotPresent
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
	curPxcSpec *pxcv1.PerconaXtraDBClusterSpec,
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
	currentDBSize := (&everestv1alpha1.Engine{Resources: everestv1alpha1.Resources{
		Memory: *curPxcSpec.PXC.Resources.Requests.Memory(),
	}}).Size()

	if dbSpec.Engine.Size() != currentDBSize {
		// DB cluster size has changed -> need to update PMM resources.
		// DB spec may contain custom PMM resources -> merge them with defaults.
		return common.MergeResources(requestedResources,
			common.CalculatePMMResources(dbSpec.Engine.Size()))
	}

	if curPxcSpec.PMM != nil && curPxcSpec.PMM.Enabled {
		// DB cluster is not new and PMM was enabled before.
		// DB spec may contain new custom PMM resources -> merge them with previously used PMM resources.
		return common.MergeResources(requestedResources,
			curPxcSpec.PMM.Resources)
	}

	// DB cluster is not new and PMM was not enabled before. Now it is being enabled.
	// DB spec may contain custom PMM resources -> merge them with defaults.
	return common.MergeResources(requestedResources,
		common.CalculatePMMResources(dbSpec.Engine.Size()))
}

func (p *applier) genPXCStorageSpec(name, namespace string) (*pxcv1.BackupStorageSpec, *everestv1alpha1.BackupStorage, error) {
	backupStorage := &everestv1alpha1.BackupStorage{}
	err := p.C.Get(p.ctx, types.NamespacedName{Name: name, Namespace: namespace}, backupStorage)
	if err != nil {
		return nil, nil, errors.Join(err, fmt.Errorf("failed to get backup storage %s", name))
	}

	return &pxcv1.BackupStorageSpec{
		Type: pxcv1.BackupStorageType(backupStorage.Spec.Type),
		// XXX: Remove this once templates will be available
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1G"),
				corev1.ResourceCPU:    resource.MustParse("600m"),
			},
		},
		VerifyTLS: backupStorage.Spec.VerifyTLS,
	}, backupStorage, nil
}

func (p *applier) genPXCBackupSpec() (*pxcv1.PXCScheduledBackup, error) {
	engine := p.DBEngine
	database := p.DB
	// Get the best backup version for the specified database engine
	bestBackupVersion := engine.BestBackupVersion(database.Spec.Engine.Version)
	backupVersion, ok := engine.Status.AvailableVersions.Backup[bestBackupVersion]
	if !ok {
		return nil, fmt.Errorf("backup version %s not available", bestBackupVersion)
	}

	// We can update the image name only in case the CRVersions match.
	// Otherwise we keep the image unchanged.
	image := backupVersion.ImagePath
	if p.currentPerconaXtraDBClusterSpec.Backup != nil && p.DBEngine.Status.OperatorVersion != p.DB.Status.CRVersion {
		image = p.currentPerconaXtraDBClusterSpec.Backup.Image
	}

	// Initialize PXCScheduledBackup object
	pxcBackupSpec := &pxcv1.PXCScheduledBackup{
		Image: image,
		PITR: pxcv1.PITRSpec{
			Enabled: database.Spec.Backup.PITR.Enabled,
		},
	}

	// Initialize map to store backup storages
	storages := make(map[string]*pxcv1.BackupStorageSpec)

	// List DatabaseClusterBackup objects for this database
	backupList, err := common.DatabaseClusterBackupsThatReferenceObject(p.ctx, p.C, consts.DBClusterBackupDBClusterNameField, database.GetNamespace(), database.GetName())
	if err != nil {
		return nil, err
	}

	// Add the storages used by the DatabaseClusterBackup objects
	if err = p.addBackupStorages(backupList.Items, database.Spec.DataSource, storages); err != nil {
		return nil, err
	}

	// Add PITR configuration if enabled
	if database.Spec.Backup.PITR.Enabled {
		if err := p.addPITRConfiguration(storages, pxcBackupSpec); err != nil {
			return nil, err
		}
	}

	// If there are no schedules, just return the storages used in DatabaseClusterBackup objects
	if len(database.Spec.Backup.Schedules) == 0 {
		pxcBackupSpec.Storages = storages
		return pxcBackupSpec, nil
	}

	// Add scheduled backup configurations
	if err := p.addScheduledBackupsConfiguration(storages, pxcBackupSpec); err != nil {
		return nil, err
	}

	return pxcBackupSpec, nil
}

func (p *applier) addBackupStorages(
	backups []everestv1alpha1.DatabaseClusterBackup,
	dataSource *everestv1alpha1.DataSource,
	storages map[string]*pxcv1.BackupStorageSpec,
) error {
	for _, backup := range backups {
		if _, ok := storages[backup.Spec.BackupStorageName]; ok {
			continue
		}

		spec, err := p.getStoragesSpec(backup.Spec.BackupStorageName)
		if err != nil {
			return err
		}
		storages[backup.Spec.BackupStorageName] = spec
	}
	// add the storage from datasource. The restore works without listing the related storage in the pxc config,
	// however if the storage is insecure, we need to specify it explicitly to set the insecureTLS flag
	if dataSource != nil && (dataSource.DBClusterBackupName != "" || dataSource.BackupSource != nil) {
		storageName, err := p.getStorageNameFromDataSource(*dataSource)
		if err != nil {
			return err
		}
		if _, ok := storages[storageName]; ok {
			return nil
		}

		spec, err := p.getStoragesSpec(storageName)
		if err != nil {
			return err
		}

		storages[storageName] = spec
	}
	return nil
}

func (p *applier) getStorageNameFromDataSource(
	dataSource everestv1alpha1.DataSource,
) (string, error) {
	backup := &everestv1alpha1.DatabaseClusterBackup{}
	var storageName string
	if dataSource.DBClusterBackupName != "" {
		err := p.C.Get(context.Background(), types.NamespacedName{
			Namespace: p.DB.GetNamespace(),
			Name:      dataSource.DBClusterBackupName,
		}, backup)
		if err != nil {
			return "", err
		}
		storageName = backup.Spec.BackupStorageName
	} else if dataSource.BackupSource != nil {
		storageName = dataSource.BackupSource.BackupStorageName
	}
	if storageName == "" {
		return "", errInvalidDataSourceConfiguration
	}
	return storageName, nil
}

func (p *applier) getStoragesSpec(backupStorageName string) (*pxcv1.BackupStorageSpec, error) {
	spec, backupStorage, err := p.genPXCStorageSpec(
		backupStorageName,
		p.DB.GetNamespace(),
	)
	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("failed to generate PXC storage spec for %s", backupStorageName))
	}

	switch backupStorage.Spec.Type {
	case everestv1alpha1.BackupStorageTypeS3:
		spec.S3 = &pxcv1.BackupStorageS3Spec{
			Bucket: fmt.Sprintf(
				"%s/%s",
				backupStorage.Spec.Bucket,
				common.BackupStoragePrefix(p.DB),
			),
			CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
			Region:            backupStorage.Spec.Region,
			EndpointURL:       backupStorage.Spec.EndpointURL,
		}
	case everestv1alpha1.BackupStorageTypeAzure:
		spec.Azure = &pxcv1.BackupStorageAzureSpec{
			ContainerPath: fmt.Sprintf(
				"%s/%s",
				backupStorage.Spec.Bucket,
				common.BackupStoragePrefix(p.DB),
			),
			CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
		}
	default:
		return nil, fmt.Errorf("unsupported backup storage type %s for %s", backupStorage.Spec.Type, backupStorage.Name)
	}
	return spec, nil
}

func (p *applier) addPITRConfiguration(storages map[string]*pxcv1.BackupStorageSpec, pxcBackupSpec *pxcv1.PXCScheduledBackup) error {
	database := p.DB
	storageName := *database.Spec.Backup.PITR.BackupStorageName

	spec, backupStorage, err := p.genPXCStorageSpec(storageName, database.GetNamespace())
	if err != nil {
		return errors.Join(err, errors.New("failed to get pitr storage"))
	}
	pxcBackupSpec.PITR.StorageName = common.PITRStorageName(storageName)

	var timeBetweenUploads float64
	if database.Spec.Backup.PITR.UploadIntervalSec != nil {
		timeBetweenUploads = float64(*database.Spec.Backup.PITR.UploadIntervalSec)
	}
	pxcBackupSpec.PITR.TimeBetweenUploads = timeBetweenUploads

	switch backupStorage.Spec.Type {
	case everestv1alpha1.BackupStorageTypeS3:
		spec.S3 = &pxcv1.BackupStorageS3Spec{
			Bucket:            common.PITRBucketName(database, backupStorage.Spec.Bucket),
			CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
			Region:            backupStorage.Spec.Region,
			EndpointURL:       backupStorage.Spec.EndpointURL,
		}
	default:
		return fmt.Errorf("BackupStorage of type %s is not supported. PITR only works for s3 compatible storages", backupStorage.Spec.Type)
	}

	// create a separate storage for pxc pitr as the docs recommend
	// https://docs.percona.com/percona-operator-for-mysql/pxc/backups-pitr.html
	storages[common.PITRStorageName(backupStorage.Name)] = spec
	return nil
}

func (p *applier) addScheduledBackupsConfiguration(
	storages map[string]*pxcv1.BackupStorageSpec,
	pxcBackupSpec *pxcv1.PXCScheduledBackup,
) error {
	database := p.DB
	var pxcSchedules []pxcv1.PXCScheduledBackupSchedule //nolint:prealloc
	for _, schedule := range database.Spec.Backup.Schedules {
		if !schedule.Enabled {
			continue
		}

		// Add the storages used by the schedule backups
		if _, ok := storages[schedule.BackupStorageName]; !ok {
			backupStorage := &everestv1alpha1.BackupStorage{}
			err := p.C.Get(p.ctx, types.NamespacedName{
				Name:      schedule.BackupStorageName,
				Namespace: database.GetNamespace(),
			}, backupStorage)
			if err != nil {
				return errors.Join(err, fmt.Errorf("failed to get backup storage %s", schedule.BackupStorageName))
			}

			storages[schedule.BackupStorageName] = &pxcv1.BackupStorageSpec{
				Type:      pxcv1.BackupStorageType(backupStorage.Spec.Type),
				VerifyTLS: backupStorage.Spec.VerifyTLS,
			}
			switch backupStorage.Spec.Type {
			case everestv1alpha1.BackupStorageTypeS3:
				storages[schedule.BackupStorageName].S3 = &pxcv1.BackupStorageS3Spec{
					Bucket: fmt.Sprintf(
						"%s/%s",
						backupStorage.Spec.Bucket,
						common.BackupStoragePrefix(database),
					),
					CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
					Region:            backupStorage.Spec.Region,
					EndpointURL:       backupStorage.Spec.EndpointURL,
				}
			case everestv1alpha1.BackupStorageTypeAzure:
				storages[schedule.BackupStorageName].Azure = &pxcv1.BackupStorageAzureSpec{
					ContainerPath: fmt.Sprintf(
						"%s/%s",
						backupStorage.Spec.Bucket,
						common.BackupStoragePrefix(database),
					),
					CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
				}
			default:
				return fmt.Errorf("unsupported backup storage type %s for %s", backupStorage.Spec.Type, backupStorage.Name)
			}
		}

		pxcSchedules = append(pxcSchedules, pxcv1.PXCScheduledBackupSchedule{
			Name:     schedule.Name,
			Schedule: schedule.Schedule,
			Keep:     int(schedule.RetentionCopies), // deprecated field, preserved for backward compatibility with CRVersion <1.18.0
			Retention: &pxcv1.PXCScheduledBackupRetention{
				Type:              pxcv1.PXCScheduledBackupRetentionType("count"),
				Count:             int(schedule.RetentionCopies),
				DeleteFromStorage: false, // We control the deletion using finalizers, not from here.
			},
			StorageName: schedule.BackupStorageName,
		})
	}

	pxcBackupSpec.Storages = storages
	pxcBackupSpec.Schedule = pxcSchedules
	return nil
}
