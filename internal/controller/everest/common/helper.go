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

// Package common contains common utilities for the everest-operator.
package common

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/AlekSi/pointer"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/pkg/apis/postgres-operator.crunchydata.com/v1beta1"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/version"
	"github.com/percona/everest-operator/internal/predicates"
)

// ErrEmptyLbc error returned when no lbc name is definded in db.
var ErrEmptyLbc = errors.New("empty backup load balancer config")

// DefaultNamespaceFilter is the default namespace filter.
var DefaultNamespaceFilter predicate.Predicate = &predicates.Nop{}

// PITRBucketName returns the name of the bucket for the point-in-time recovery backups.
func PITRBucketName(db *everestv1alpha1.DatabaseCluster, bucket string) string {
	return fmt.Sprintf("%s/%s/%s", bucket, BackupStoragePrefix(db), "pitr")
}

// PITRStorageName returns the name of the storage for the point-in-time recovery backups.
func PITRStorageName(storageName string) string {
	return storageName + "-pitr"
}

// BackupStoragePrefix returns the prefix for the backup storage.
func BackupStoragePrefix(db *everestv1alpha1.DatabaseCluster) string {
	return fmt.Sprintf("%s/%s", db.Name, db.UID)
}

// GetOperatorImage returns the image of the operator running in the cluster
// for the specified deployment name and namespace.
func GetOperatorImage(
	ctx context.Context,
	c client.Client,
	name types.NamespacedName,
) (string, error) {
	unstructuredResource := &unstructured.Unstructured{}
	unstructuredResource.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "apps",
		Kind:    "Deployment",
		Version: "v1",
	})
	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, name, unstructuredResource); err != nil {
		return "", err
	}
	err := runtime.DefaultUnstructuredConverter.
		FromUnstructured(unstructuredResource.Object, deployment)
	if err != nil {
		return "", err
	}
	return deployment.Spec.Template.Spec.Containers[0].Image, nil
}

// GetOperatorVersion returns the version of the operator running in the cluster
// for the specified deployment name and namespace.
//
// TODO: Read the operator version from the DatabaseEngine status rather than fetching the Deployment,
// since DatabaseEngines are cached in the controller's client.
//
//nolint:godox
func GetOperatorVersion(
	ctx context.Context,
	c client.Client,
	name types.NamespacedName,
) (*version.Version, error) {
	image, err := GetOperatorImage(ctx, c, name)
	if err != nil {
		return nil, err
	}
	v := strings.Split(image, ":")[1]
	return version.NewVersion(v)
}

// GetClusterType returns the type of the cluster on which this operator is running.
func GetClusterType(ctx context.Context, c client.Client) (consts.ClusterType, error) {
	clusterType := consts.ClusterTypeMinikube
	storageList := &storagev1.StorageClassList{}

	err := c.List(ctx, storageList)
	if err != nil {
		return clusterType, err
	}

	for _, storage := range storageList.Items {
		if strings.Contains(storage.Provisioner, "aws") {
			clusterType = consts.ClusterTypeEKS
			break
		}
	}

	return clusterType, nil
}

func mergeMap(dst map[string]interface{}, src map[string]interface{}) error {
	return mergeMapInternal(dst, src, "")
}

func mergeMapInternal(dst map[string]interface{}, src map[string]interface{}, parent string) error {
	for k, v := range src {
		if dst[k] != nil && reflect.TypeOf(dst[k]) != reflect.TypeOf(v) {
			return fmt.Errorf("type mismatch for %s.%s, %T != %T", parent, k, dst[k], v)
		}
		switch v.(type) { //nolint:gocritic
		case map[string]interface{}:
			switch dst[k].(type) { //nolint:gocritic
			case nil:
				dst[k] = v
			case map[string]interface{}: //nolint:forcetypeassert
				err := mergeMapInternal(dst[k].(map[string]interface{}),
					v.(map[string]interface{}), fmt.Sprintf("%s.%s", parent, k))
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("type mismatch for %s.%s, %T != %T", parent, k, dst[k], v)
			}
		default:
			dst[k] = v
		}
	}
	return nil
}

// GetDatabaseEngineForType gets the DatabaseEngine object for the specified engine type.
func GetDatabaseEngineForType(
	ctx context.Context,
	c client.Client,
	engineType everestv1alpha1.EngineType,
	namespace string,
) (*everestv1alpha1.DatabaseEngine, error) {
	engineName := ""
	switch engineType {
	case everestv1alpha1.DatabaseEnginePXC:
		engineName = consts.PXCDeploymentName
	case everestv1alpha1.DatabaseEnginePSMDB:
		engineName = consts.PSMDBDeploymentName
	case everestv1alpha1.DatabaseEnginePostgresql:
		engineName = consts.PGDeploymentName
	default:
		return nil, fmt.Errorf("unknown engine type: %s", engineType)
	}
	return GetDatabaseEngine(ctx, c, engineName, namespace)
}

// GetDatabaseEngine gets the DatabaseEngine object with the specified name and namespace.
func GetDatabaseEngine(ctx context.Context, c client.Client, name, namespace string) (*everestv1alpha1.DatabaseEngine, error) {
	engine := &everestv1alpha1.DatabaseEngine{}
	err := c.Get(ctx,
		types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		}, engine)
	if err != nil {
		return nil, fmt.Errorf("failed to get database engine %s", name)
	}
	return engine, nil
}

// GetSecretFromMonitoringConfig gets the secret data from the MonitoringConfig.
func GetSecretFromMonitoringConfig(
	ctx context.Context,
	c client.Client,
	monitoring *everestv1alpha1.MonitoringConfig,
) (string, error) {
	var secret *corev1.Secret
	secretData := ""

	if monitoring.Spec.CredentialsSecretName != "" {
		secret = &corev1.Secret{}
		err := c.Get(ctx, types.NamespacedName{
			Name:      monitoring.Spec.CredentialsSecretName,
			Namespace: monitoring.GetNamespace(),
		}, secret)
		if err != nil {
			return "", err
		}

		if key, ok := secret.Data[everestv1alpha1.MonitoringConfigCredentialsSecretAPIKeyKey]; ok {
			secretData = string(key)
		}
	}

	return secretData, nil
}

// UpdateSecretData updates the data of a secret.
// It only changes the values of the keys specified in the data map.
// All other keys are left untouched, so it's not possible to delete a key.
func UpdateSecretData(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
	secretName string,
	data map[string][]byte,
	setControllerRef bool,
) error {
	secret := &corev1.Secret{}
	err := c.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: database.Namespace,
	}, secret)
	if err != nil {
		return err
	}

	var needsUpdate bool
	for k, v := range data {
		oldValue, ok := secret.Data[k]
		if !ok || !bytes.Equal(oldValue, v) {
			secret.Data[k] = v
			needsUpdate = true
		}
	}
	if !needsUpdate {
		return nil
	}

	if setControllerRef {
		err = controllerutil.SetControllerReference(database, secret, c.Scheme())
		if err != nil {
			return err
		}
	}
	return c.Update(ctx, secret)
}

// CreateOrUpdateSecretData creates or updates the data of a secret.
// When updating, it only changes the values of the keys specified in the data
// map.
// All other keys are left untouched, so it's not possible to delete a key.
func CreateOrUpdateSecretData(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
	secretName string,
	data map[string][]byte,
	setControllerRef bool,
) error {
	err := UpdateSecretData(ctx, c, database, secretName, data, setControllerRef)
	if err == nil {
		return nil
	}

	if !k8serrors.IsNotFound(err) {
		return err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: database.Namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}

	if setControllerRef {
		err = controllerutil.SetControllerReference(database, secret, c.Scheme())
		if err != nil {
			return err
		}
	}

	// If the secret does not exist, create it
	return c.Create(ctx, secret)
}

// ReconcileDBRestoreFromDataSource reconciles the DatabaseClusterRestore object
// based on the DataSource field of the DatabaseCluster.
func ReconcileDBRestoreFromDataSource(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
) error {
	dbr := &everestv1alpha1.DatabaseClusterRestore{}
	err := c.Get(ctx, types.NamespacedName{
		Namespace: database.Namespace,
		Name:      database.Name,
	}, dbr)
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	if dbr.IsComplete() {
		database.Spec.DataSource = nil
		return nil
	}

	if (database.Spec.DataSource.DBClusterBackupName == "" && database.Spec.DataSource.BackupSource == nil) ||
		(database.Spec.DataSource.DBClusterBackupName != "" && database.Spec.DataSource.BackupSource != nil) {
		return errors.New("either DBClusterBackupName or BackupSource must be specified in the DataSource field")
	}

	dbRestore := &everestv1alpha1.DatabaseClusterRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      database.Name,
			Namespace: database.Namespace,
		},
	}
	if err := controllerutil.SetControllerReference(database, dbRestore, c.Scheme()); err != nil {
		return err
	}
	_, err = controllerutil.CreateOrUpdate(ctx, c, dbRestore, func() error {
		dbRestore.Spec.DBClusterName = database.Name
		dbRestore.Spec.DataSource = getRestoreDataSource(ctx, c, database)
		return nil
	})

	return err
}

func getRestoreDataSource(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
) everestv1alpha1.DatabaseClusterRestoreDataSource {
	if database.Spec.Engine.Type != everestv1alpha1.DatabaseEnginePSMDB || database.Spec.DataSource.DBClusterBackupName == "" {
		return database.Spec.DataSource.IntoDBRestoreDataSource()
	}

	// Use the full backup source instead of just the backupName to be able to
	// figure out the source dbc's prefix in the storage
	backupName := database.Spec.DataSource.DBClusterBackupName

	backup := &psmdbv1.PerconaServerMongoDBBackup{}
	err := c.Get(ctx, types.NamespacedName{Name: backupName, Namespace: database.Namespace}, backup)
	if err != nil {
		return everestv1alpha1.DatabaseClusterRestoreDataSource{}
	}

	return everestv1alpha1.DatabaseClusterRestoreDataSource{
		BackupSource: &everestv1alpha1.BackupSource{
			Path:              backup.Status.Destination,
			BackupStorageName: backup.Spec.StorageName,
		},
		PITR: database.Spec.DataSource.PITR,
	}
}

// ValidatePitrRestoreSpec validates the PITR restore spec.
func ValidatePitrRestoreSpec(dataSource everestv1alpha1.DataSource) error {
	if dataSource.PITR == nil {
		return nil
	}

	// use 'date' as default
	if dataSource.PITR.Type == "" {
		dataSource.PITR.Type = everestv1alpha1.PITRTypeDate
	}

	switch dataSource.PITR.Type {
	case everestv1alpha1.PITRTypeDate:
		if dataSource.PITR.Date == nil {
			return ErrPitrEmptyDate
		}
	case everestv1alpha1.PITRTypeLatest:
		//nolint:godox
		// TODO: figure out why "latest" doesn't work currently for Everest
		return ErrPitrTypeLatest
	default:
		return ErrPitrTypeIsNotSupported
	}
	return nil
}

// CreateOrUpdate creates or updates a resource.
// With patchSecretData the new secret Data is applied on top of the original secret's Data.
func CreateOrUpdate(
	ctx context.Context,
	c client.Client,
	obj client.Object,
	patchSecretData bool,
) error {
	hash, err := getObjectHash(obj)
	if err != nil {
		return errors.Join(err, errors.New("calculate object hash"))
	}

	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	}
	oldObject, ok := reflect.New(val.Type()).Interface().(client.Object)
	if !ok {
		return errors.New("failed type conversion")
	}

	if obj.GetName() == "" && obj.GetGenerateName() != "" {
		return c.Create(ctx, obj)
	}

	err = c.Get(ctx, types.NamespacedName{
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
	}, oldObject)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.Create(ctx, obj)
		}
		return errors.Join(err, errors.New("get object"))
	}

	oldHash, err := getObjectHash(oldObject)
	if err != nil {
		return errors.Join(err, errors.New("calculate old object hash"))
	}

	if oldHash != hash || !isObjectMetaEqual(obj, oldObject) {
		return updateObject(ctx, c, obj, oldObject, patchSecretData)
	}

	return nil
}

func getObjectHash(obj runtime.Object) (string, error) {
	var dataToMarshall interface{}
	switch object := obj.(type) {
	case *appsv1.StatefulSet:
		dataToMarshall = object.Spec
	case *appsv1.Deployment:
		dataToMarshall = object.Spec
	case *corev1.Service:
		dataToMarshall = object.Spec
	default:
		dataToMarshall = obj
	}
	data, err := json.Marshal(dataToMarshall)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func isObjectMetaEqual(oldObj, newObj metav1.Object) bool {
	return reflect.DeepEqual(oldObj.GetAnnotations(), newObj.GetAnnotations()) &&
		reflect.DeepEqual(oldObj.GetLabels(), newObj.GetLabels())
}

func updateObject(
	ctx context.Context,
	c client.Client,
	obj, oldObject client.Object,
	patchSecretData bool,
) error {
	obj.SetResourceVersion(oldObject.GetResourceVersion())
	switch object := obj.(type) {
	case *corev1.Service:
		oldObjectService, ok := oldObject.(*corev1.Service)
		if !ok {
			return errors.New("failed type conversion to service")
		}
		object.Spec.ClusterIP = oldObjectService.Spec.ClusterIP
		if object.Spec.Type == corev1.ServiceTypeLoadBalancer {
			object.Spec.HealthCheckNodePort = oldObjectService.Spec.HealthCheckNodePort
		}
	case *corev1.Secret:
		if patchSecretData {
			s, ok := oldObject.(*corev1.Secret)
			if !ok {
				return errors.New("failed type conversion to secret")
			}
			for k, v := range object.Data {
				s.Data[k] = v
			}
			object.Data = s.Data
		}
	default:
	}

	return c.Update(ctx, obj)
}

// GetBackupStorage returns a BackupStorage object
// with the specified name and namespace.
func GetBackupStorage(
	ctx context.Context,
	c client.Client,
	name, namespace string,
) (*everestv1alpha1.BackupStorage, error) {
	backupStorage := &everestv1alpha1.BackupStorage{}
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := c.Get(ctx, key, backupStorage); err != nil {
		return nil,
			fmt.Errorf("failed to get backup storage '%s': %w", name, err)
	}
	return backupStorage, nil
}

// DatabaseClustersThatReferenceObject returns a list of DatabaseClusters that
// reference the given name by the provided keyPath and namespace.
func DatabaseClustersThatReferenceObject(
	ctx context.Context,
	c client.Client,
	keyPath, namespace, keyValue string,
) (*everestv1alpha1.DatabaseClusterList, error) {
	attachedList := &everestv1alpha1.DatabaseClusterList{}
	err := findObjectsThatReferenceObject(ctx, c, attachedList, keyPath, namespace, keyValue)
	return attachedList, err
}

// DatabaseClusterBackupsThatReferenceObject returns a list of DatabaseClusterBackups that
// reference the given name by the provided keyPath and namespace.
func DatabaseClusterBackupsThatReferenceObject(
	ctx context.Context,
	c client.Client,
	keyPath, namespace, keyValue string,
) (*everestv1alpha1.DatabaseClusterBackupList, error) {
	attachedList := &everestv1alpha1.DatabaseClusterBackupList{}
	err := findObjectsThatReferenceObject(ctx, c, attachedList, keyPath, namespace, keyValue)
	return attachedList, err
}

// DatabaseClusterRestoresThatReferenceObject returns a list of DatabaseClusterRestores that
// reference the given name by the provided keyPath and namespace.
func DatabaseClusterRestoresThatReferenceObject(
	ctx context.Context,
	c client.Client,
	keyPath, namespace, keyValue string,
) (*everestv1alpha1.DatabaseClusterRestoreList, error) {
	attachedList := &everestv1alpha1.DatabaseClusterRestoreList{}
	err := findObjectsThatReferenceObject(ctx, c, attachedList, keyPath, namespace, keyValue)
	return attachedList, err
}

// BackupStoragesThatReferenceObject returns a list of BackupStorages that
// reference the given name by the provided keyPath and namespace.
func BackupStoragesThatReferenceObject(
	ctx context.Context,
	c client.Client,
	keyPath, namespace, keyValue string,
) (*everestv1alpha1.BackupStorageList, error) {
	attachedList := &everestv1alpha1.BackupStorageList{}
	err := findObjectsThatReferenceObject(ctx, c, attachedList, keyPath, namespace, keyValue)
	return attachedList, err
}

// MonitoringConfigsThatReferenceObject returns a list of MonitoringConfig that
// reference the given name by the provided keyPath and namespace.
func MonitoringConfigsThatReferenceObject(
	ctx context.Context,
	c client.Client,
	keyPath, namespace, keyValue string,
) (*everestv1alpha1.MonitoringConfigList, error) {
	attachedList := &everestv1alpha1.MonitoringConfigList{}
	err := findObjectsThatReferenceObject(ctx, c, attachedList, keyPath, namespace, keyValue)
	return attachedList, err
}

func findObjectsThatReferenceObject(ctx context.Context, c client.Client, objList client.ObjectList, keyPath, namespace, keyValue string) error {
	listOpts := &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(keyPath, keyValue),
		Namespace:     namespace,
	}
	return c.List(ctx, objList, listOpts)
}

// GetDBMonitoringConfig returns the MonitoringConfig object
// for the given DatabaseCluster object.
func GetDBMonitoringConfig(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
) (*everestv1alpha1.MonitoringConfig, error) {
	monitoring := &everestv1alpha1.MonitoringConfig{}
	mcName := pointer.Get(database.Spec.Monitoring).MonitoringConfigName
	if mcName != "" {
		if err := c.Get(ctx, types.NamespacedName{
			Name:      mcName,
			Namespace: database.GetNamespace(),
		}, monitoring); err != nil {
			return nil, err
		}
	}
	return monitoring, nil
}

// GetLoadBalancerConfig returns the LoadBalancerConfig object
// for the given DatabaseCluster object.
func GetLoadBalancerConfig(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
) (*everestv1alpha1.LoadBalancerConfig, error) {
	lbcName := database.Spec.Proxy.Expose.LoadBalancerConfigName

	if lbcName == "" {
		return nil, ErrEmptyLbc
	}

	lbc := &everestv1alpha1.LoadBalancerConfig{}

	err := c.Get(ctx, types.NamespacedName{Name: lbcName}, lbc)
	if err != nil {
		return nil, err
	}

	return lbc, nil
}

// GetAnnotations returns annotations from the LoadBalancerConfig used in the given DB.
func GetAnnotations(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
) (map[string]string, error) {
	lbc, err := GetLoadBalancerConfig(ctx, c, database)
	if err != nil {
		if errors.Is(err, ErrEmptyLbc) {
			return map[string]string{}, nil
		}

		return nil, err
	}

	return lbc.Spec.Annotations, nil
}

// GetPodSchedulingPolicy returns the PodSchedulingPolicy object by name.
func GetPodSchedulingPolicy(ctx context.Context, c client.Client, pspName string) (*everestv1alpha1.PodSchedulingPolicy, error) {
	psp := &everestv1alpha1.PodSchedulingPolicy{}
	if pspName != "" {
		if err := c.Get(ctx, types.NamespacedName{
			Name: pspName,
		}, psp); err != nil {
			return nil, err
		}
	}
	return psp, nil
}

// IsDatabaseClusterRestoreRunning returns true if a restore is running for the
// specified database, otherwise false.
func IsDatabaseClusterRestoreRunning(
	ctx context.Context,
	c client.Client,
	dbName, dbNs string,
) (bool, error) {
	// List restores for this database.
	restoreList, err := DatabaseClusterRestoresThatReferenceObject(ctx, c, consts.DBClusterRestoreDBClusterNameField, dbNs, dbName)
	if err != nil {
		return false, err
	}
	// Check if any are not yet complete?
	return slices.ContainsFunc(restoreList.Items, func(dbr everestv1alpha1.DatabaseClusterRestore) bool {
		return dbr.IsInProgress()
	}), nil
}

// IsNewDatabaseCluster returns true if the database is in a new or init state.
func IsNewDatabaseCluster(dbState everestv1alpha1.AppState) bool {
	dbState = dbState.WithCreatingState()
	return dbState == everestv1alpha1.AppStateCreating || dbState == everestv1alpha1.AppStateInit
}

// GetRepoNameByBackupStorage returns the name of the repo that corresponds to the given backup storage.
func GetRepoNameByBackupStorage(
	backupStorage *everestv1alpha1.BackupStorage,
	repos []crunchyv1beta1.PGBackRestRepo,
) string {
	for _, repo := range repos {
		if repo.S3 != nil &&
			repo.S3.Bucket == backupStorage.Spec.Bucket &&
			repo.S3.Region == backupStorage.Spec.Region &&
			repo.S3.Endpoint == backupStorage.Spec.EndpointURL {
			return repo.Name
		}

		if repo.Azure != nil && repo.Azure.Container == backupStorage.Spec.Bucket {
			return repo.Name
		}
	}
	return ""
}

// HandleDBBackupsCleanup handles the cleanup of the dbbackup objects.
// Returns true if cleanup is complete.
func HandleDBBackupsCleanup(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
) (bool, error) {
	if controllerutil.ContainsFinalizer(database, consts.DBBackupCleanupFinalizer) {
		if done, err := deleteBackupsForDatabase(ctx, c, database.GetName(), database.GetNamespace()); err != nil {
			return false, err
		} else if !done {
			return false, nil
		}
		controllerutil.RemoveFinalizer(database, consts.DBBackupCleanupFinalizer)
		return true, c.Update(ctx, database)
	}
	return true, nil
}

// Delete all dbbackups for the given database.
// Returns true if no dbbackups are found.
func deleteBackupsForDatabase(
	ctx context.Context,
	c client.Client,
	dbName, dbNs string,
) (bool, error) {
	backupList, err := DatabaseClusterBackupsThatReferenceObject(ctx, c, consts.DBClusterBackupDBClusterNameField, dbNs, dbName)
	if err != nil {
		return false, err
	}
	if len(backupList.Items) == 0 {
		return true, nil
	}
	for _, backup := range backupList.Items {
		if !backup.GetDeletionTimestamp().IsZero() {
			// Already deleting, continue to next.
			continue
		}
		if err := c.Delete(ctx, &backup); err != nil {
			return false, err
		}
	}
	return false, nil
}

// HandleUpstreamClusterCleanup handles the cleanup of the psdmb objects.
// Returns true if cleanup is complete.
func HandleUpstreamClusterCleanup(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
	upstream client.Object,
) (bool, error) {
	if controllerutil.ContainsFinalizer(database, consts.UpstreamClusterCleanupFinalizer) { //nolint:nestif
		// first check that all dbb are deleted since the upstream backups may need the upstream cluster to be present.
		backupList, err := DatabaseClusterBackupsThatReferenceObject(ctx, c, consts.DBClusterBackupDBClusterNameField, database.GetNamespace(), database.GetName())
		if err != nil {
			return false, err
		}
		if len(backupList.Items) != 0 {
			return false, nil
		}

		err = c.Get(ctx, types.NamespacedName{
			Name:      database.Name,
			Namespace: database.Namespace,
		}, upstream)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				controllerutil.RemoveFinalizer(database, consts.UpstreamClusterCleanupFinalizer)
				return true, c.Update(ctx, database)
			}
			return false, err
		}

		err = c.Delete(ctx, upstream)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
	}
	return true, nil
}

// IsOwnedBy checks if the child object is owned by the parent object.
// Returns true if child has an owner reference to the parents.
func IsOwnedBy(child, parent metav1.Object) bool {
	for _, owner := range child.GetOwnerReferences() {
		if owner.UID == parent.GetUID() {
			return true
		}
	}
	return false
}

// GetRecommendedCRVersion returns the recommended CR version for the operator.
func GetRecommendedCRVersion(
	ctx context.Context,
	c client.Client,
	operatorName string,
	db *everestv1alpha1.DatabaseCluster,
) (*string, error) {
	v, err := GetOperatorVersion(ctx, c, types.NamespacedName{
		Name:      operatorName,
		Namespace: db.GetNamespace(),
	})
	if err != nil {
		return nil, err
	}
	if v.ToCRVersion() != db.Status.CRVersion {
		return pointer.To(v.ToCRVersion()), nil
	}
	return nil, nil //nolint:nilnil
}

// StatusAsPlainTextOrEmptyString returns the status as a plain text string or an empty string.
func StatusAsPlainTextOrEmptyString(status interface{}) string {
	result, _ := yaml.Marshal(status)
	return string(result)
}

// EnqueueObjectsInNamespace returns an event handler that should be attached with Namespace watchers.
// It enqueues all objects specified by the type of list in the triggered namespace.
func EnqueueObjectsInNamespace(c client.Client, list client.ObjectList) handler.EventHandler { //nolint:ireturn
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
		if _, ok := o.(*corev1.Namespace); !ok {
			panic("EnqueueObjectsInNamespace should be called on a Namespace")
		}
		if err := c.List(ctx, list, client.InNamespace(o.GetName())); err != nil {
			return nil
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			return nil
		}
		requests := make([]reconcile.Request, 0, len(items))
		for _, item := range items {
			uObj, err := toUnstructured(item)
			if err != nil {
				return nil
			}
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: uObj.GetNamespace(),
					Name:      uObj.GetName(),
				},
			})
		}
		return requests
	})
}

func toUnstructured(obj runtime.Object) (*unstructured.Unstructured, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	ud := &unstructured.Unstructured{}
	if err := json.Unmarshal(b, ud); err != nil {
		return nil, err
	}
	return ud, nil
}

const (
	storageClassDefaultAnnotation = "storageclass.kubernetes.io/is-default-class"
)

func storageClassSupportsVolumeExpansion(ctx context.Context, c client.Client, className *string) (bool, error) {
	storageClass, err := getStorageClassOrDefault(ctx, c, className)
	if err != nil {
		return false, fmt.Errorf("getStorageClassOrDefault failed: %w", err)
	}
	return *storageClass.AllowVolumeExpansion, nil
}

func getStorageClassOrDefault(ctx context.Context, c client.Client, scName *string) (*storagev1.StorageClass, error) {
	storageClass := &storagev1.StorageClass{}
	if scName == nil {
		storageClasses := &storagev1.StorageClassList{}
		if err := c.List(ctx, storageClasses); err != nil {
			return nil, err
		}
		for _, sc := range storageClasses.Items {
			if sc.Annotations[storageClassDefaultAnnotation] == "true" {
				return &sc, nil
			}
		}
		return nil, errors.New("no default storage class found")
	}
	if err := c.Get(ctx, types.NamespacedName{Name: *scName}, storageClass); err != nil {
		return nil, err
	}
	return storageClass, nil
}

// ConfigureStorage handles storage configuration and volume expansion checks for the given database cluster.
func ConfigureStorage(
	ctx context.Context,
	c client.Client,
	db *everestv1alpha1.DatabaseCluster,
	currentSize resource.Quantity,
	setStorageSizeFunc func(resource.Quantity),
) error {
	meta.RemoveStatusCondition(&db.Status.Conditions, everestv1alpha1.ConditionTypeCannotResizeVolume)

	desiredSize := db.Spec.Engine.Storage.Size
	storageClass := db.Spec.Engine.Storage.Class

	// We cannot shrink the volume size.
	hasStorageShrunk := currentSize.Cmp(desiredSize) > 0 && !currentSize.IsZero()
	if hasStorageShrunk {
		meta.SetStatusCondition(&db.Status.Conditions, metav1.Condition{
			Type:               everestv1alpha1.ConditionTypeCannotResizeVolume,
			Status:             metav1.ConditionTrue,
			Reason:             everestv1alpha1.ReasonCannotShrinkVolume,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: db.GetGeneration(),
		})
		setStorageSizeFunc(currentSize)
		return nil
	}

	// Check if storage size is being expanded. If not, set the desired size and return early.
	hasStorageExpanded := currentSize.Cmp(desiredSize) < 0 && !currentSize.IsZero()
	if !hasStorageExpanded {
		setStorageSizeFunc(desiredSize)
		return nil
	}

	allowedByStorageClass, err := storageClassSupportsVolumeExpansion(ctx, c, storageClass)
	if err != nil {
		return fmt.Errorf("failed to check if storage class supports volume expansion: %w", err)
	}

	if !allowedByStorageClass {
		meta.SetStatusCondition(&db.Status.Conditions, metav1.Condition{
			Type:               everestv1alpha1.ConditionTypeCannotResizeVolume,
			Status:             metav1.ConditionTrue,
			Reason:             everestv1alpha1.ReasonStorageClassDoesNotSupportExpansion,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: db.GetGeneration(),
		})
		setStorageSizeFunc(currentSize)
		return nil
	}

	setStorageSizeFunc(desiredSize)
	return nil
}

// VerifyPVCResizeFailure checks if the PVC resize failed. If it failed, it
// returns true and the error message.
func VerifyPVCResizeFailure(ctx context.Context, c client.Client, name, namespace string) (bool, string, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcList, client.InNamespace(namespace), client.MatchingLabels{"app.kubernetes.io/instance": name}); err != nil {
		return false, "", fmt.Errorf("failed to list PVCs for DB cluster='%s' in namespace='%s': %w", name, namespace, err)
	}
	for _, pvc := range pvcList.Items {
		for _, condition := range pvc.Status.Conditions {
			if (condition.Type == corev1.PersistentVolumeClaimControllerResizeError ||
				condition.Type == corev1.PersistentVolumeClaimNodeResizeError) &&
				condition.Status == corev1.ConditionTrue {
				return true, condition.Message, nil
			}
		}
	}
	return false, "", nil
}

// GetDataImportJobName returns the name of the DataImport job for the given database cluster.
func GetDataImportJobName(db *everestv1alpha1.DatabaseCluster) string {
	prefix := "data-import-"
	return prefix + db.GetName()
}

// EnsureInUseFinalizer ensures that the InUseResourceFinalizer is present or absent.
func EnsureInUseFinalizer(ctx context.Context, c client.Client, used bool, obj client.Object) error {
	var updated bool
	if used {
		updated = controllerutil.AddFinalizer(obj, everestv1alpha1.InUseResourceFinalizer)
	} else {
		updated = controllerutil.RemoveFinalizer(obj, everestv1alpha1.InUseResourceFinalizer)
	}

	if updated {
		return c.Update(ctx, obj)
	}
	return nil
}

// MergeResources merges highPriorityResources and lowPriorityResources.
// If a resource is specified in both, the value from highPriorityResources is used.
// If a resource is only specified in one, that value is used.
func MergeResources(highPriorityResources, lowPriorityResources corev1.ResourceRequirements) corev1.ResourceRequirements {
	mergedResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	// CPU Requests
	if highPriorityResources.Requests != nil &&
		highPriorityResources.Requests.Cpu() != nil &&
		!highPriorityResources.Requests.Cpu().IsZero() {
		mergedResources.Requests[corev1.ResourceCPU] = *highPriorityResources.Requests.Cpu()
	} else if lowPriorityResources.Requests != nil &&
		lowPriorityResources.Requests.Cpu() != nil &&
		!lowPriorityResources.Requests.Cpu().IsZero() {
		mergedResources.Requests[corev1.ResourceCPU] = *lowPriorityResources.Requests.Cpu()
	}

	// Memory Requests
	if highPriorityResources.Requests != nil &&
		highPriorityResources.Requests.Memory() != nil &&
		!highPriorityResources.Requests.Memory().IsZero() {
		mergedResources.Requests[corev1.ResourceMemory] = *highPriorityResources.Requests.Memory()
	} else if lowPriorityResources.Requests != nil &&
		lowPriorityResources.Requests.Memory() != nil &&
		!lowPriorityResources.Requests.Memory().IsZero() {
		mergedResources.Requests[corev1.ResourceMemory] = *lowPriorityResources.Requests.Memory()
	}

	// CPU Limits
	if highPriorityResources.Limits != nil &&
		highPriorityResources.Limits.Cpu() != nil &&
		!highPriorityResources.Limits.Cpu().IsZero() {
		mergedResources.Limits[corev1.ResourceCPU] = *highPriorityResources.Limits.Cpu()
	} else if lowPriorityResources.Limits != nil &&
		lowPriorityResources.Limits.Cpu() != nil &&
		!lowPriorityResources.Limits.Cpu().IsZero() {
		mergedResources.Limits[corev1.ResourceCPU] = *lowPriorityResources.Limits.Cpu()
	}

	// Memory Limits
	if highPriorityResources.Limits != nil &&
		highPriorityResources.Limits.Memory() != nil &&
		!highPriorityResources.Limits.Memory().IsZero() {
		mergedResources.Limits[corev1.ResourceMemory] = *highPriorityResources.Limits.Memory()
	} else if lowPriorityResources.Limits != nil &&
		lowPriorityResources.Limits.Memory() != nil &&
		!lowPriorityResources.Limits.Memory().IsZero() {
		mergedResources.Limits[corev1.ResourceMemory] = *lowPriorityResources.Limits.Memory()
	}

	return mergedResources
}

// GetSplitHorizonDNSConfigNameFromDB returns the SplitHorizonDNSConfigName from the given DatabaseCluster.
func GetSplitHorizonDNSConfigNameFromDB(db *everestv1alpha1.DatabaseCluster) string {
	return pointer.Get(pointer.Get(db.Spec.EngineFeatures).PSMDB).SplitHorizonDNSConfigName
}
