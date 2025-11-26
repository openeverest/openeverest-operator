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

package controllers

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	pgv2 "github.com/percona/percona-postgresql-operator/pkg/apis/pgv2.percona.com/v2"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

const (
	clusterReadyTimeout = 15 * time.Minute

	dbClusterRestoreDBClusterNameField               = ".spec.dbClusterName"
	dbClusterRestoreDataSourceBackupStorageNameField = ".spec.dataSource.backupSource.backupStorageName"
	pgBackupTypeDate                                 = "time"
	pgBackupTypeImmediate                            = "immediate"
	timeoutWaitingAfterClusterIsReady                = 30 * time.Second
)

var (
	errPitrTypeIsNotSupported = errors.New("unknown PITR type")
	errPitrTypeLatest         = errors.New("'latest' type is not supported by Everest yet")
	errPitrEmptyDate          = errors.New("no date provided for PITR of type 'date'")
)

// DatabaseClusterRestoreReconciler reconciles a DatabaseClusterRestore object.
type DatabaseClusterRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Cache  cache.Cache

	controller *controllerWatcherRegistry
}

// +kubebuilder:rbac:groups=everest.percona.com,resources=databaseclusterrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=everest.percona.com,resources=databaseclusterrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=everest.percona.com,resources=databaseclusterrestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=pxc.percona.com,resources=perconaxtradbclusterrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=psmdb.percona.com,resources=perconaservermongodbrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgv2.percona.com,resources=perconapgrestores,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *DatabaseClusterRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling", "request", req)
	defer func() {
		logger.Info("Reconciled", "request", req)
	}()

	cr := &everestv1alpha1.DatabaseClusterRestore{}
	err := r.Get(ctx, req.NamespacedName, cr)
	if err != nil {
		// NotFound cannot be fixed by requeuing so ignore it. During background
		// deletion, we receive delete events from cluster's dependents after
		// cluster is deleted.
		if err = client.IgnoreNotFound(err); err != nil {
			logger.Error(err, "unable to fetch DatabaseClusterRestore")
		}
		return reconcile.Result{}, err
	}

	if (cr.Spec.DataSource.DBClusterBackupName == "" && cr.Spec.DataSource.BackupSource == nil) ||
		(cr.Spec.DataSource.DBClusterBackupName != "" && cr.Spec.DataSource.BackupSource != nil) {
		return reconcile.Result{}, errors.New("specify either dbClusterBackupName or backupSource")
	}

	if len(cr.ObjectMeta.Labels) == 0 {
		cr.ObjectMeta.Labels = map[string]string{
			consts.DatabaseClusterNameLabel: cr.Spec.DBClusterName,
		}

		if err := r.Update(ctx, cr); err != nil {
			return reconcile.Result{}, err
		}
	}

	dbCRNamespacedName := types.NamespacedName{
		Name:      cr.Spec.DBClusterName,
		Namespace: cr.Namespace,
	}
	dbCR := &everestv1alpha1.DatabaseCluster{}
	err = r.Get(ctx, dbCRNamespacedName, dbCR)
	if err != nil {
		if err = client.IgnoreNotFound(err); err != nil {
			logger.Error(err, "unable to fetch DatabaseCluster")
		}
		return reconcile.Result{}, err
	}

	// Ensure that the DatabaseCluster CR is the owner of the
	// DatabaseClusterRestore CR. This will ensure that the
	// DatabaseClusterRestore CR is deleted when the DatabaseCluster CR is
	// deleted.
	if err := r.ensureOwnerReference(ctx, cr, dbCR); err != nil {
		logger.Error(err, "unable to set owner reference")
		return reconcile.Result{}, err
	}
	var needRequeue bool
	switch dbCR.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePXC:
		if err := r.restorePXC(ctx, cr, dbCR); err != nil {
			logger.Error(err, "unable to restore PXC Cluster")
			return reconcile.Result{}, err
		}
	case everestv1alpha1.DatabaseEnginePSMDB:
		if err, needRequeue = r.restorePSMDB(ctx, cr); err != nil {
			// The DatabaseCluster controller is responsible for updating the
			// upstream DB cluster with the necessary storage definition. If
			// the storage is not defined in the upstream DB cluster CR, we
			// requeue the backup to give the DatabaseCluster controller a
			// chance to update the upstream DB cluster CR.
			if errors.Is(err, ErrBackupStorageUndefined) {
				return reconcile.Result{Requeue: true}, nil
			}

			logger.Error(err, "unable to restore PSMDB Cluster")
			return reconcile.Result{}, err
		}
	case everestv1alpha1.DatabaseEnginePostgresql:
		if err := r.restorePG(ctx, cr); err != nil {
			// The DatabaseCluster controller is responsible for updating the
			// upstream DB cluster with the necessary storage definition. If
			// the storage is not defined in the upstream DB cluster CR, we
			// requeue the backup to give the DatabaseCluster controller a
			// chance to update the upstream DB cluster CR.
			if errors.Is(err, ErrBackupStorageUndefined) {
				return reconcile.Result{Requeue: true}, nil
			}

			logger.Error(err, "unable to restore PG Cluster")
			return reconcile.Result{}, err
		}
	}
	if needRequeue {
		return reconcile.Result{RequeueAfter: requeueAfter}, nil
	}

	return ctrl.Result{}, nil
}

func (r *DatabaseClusterRestoreReconciler) ensureOwnerReference(
	ctx context.Context,
	restore *everestv1alpha1.DatabaseClusterRestore,
	db *everestv1alpha1.DatabaseCluster,
) error {
	if len(restore.GetOwnerReferences()) == 0 {
		restore.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion:         db.APIVersion,
				Kind:               db.Kind,
				Name:               db.Name,
				UID:                db.UID,
				BlockOwnerDeletion: pointer.ToBool(true),
			},
		})
		if err := r.Update(ctx, restore); err != nil {
			return err
		}
	}
	return nil
}

func (r *DatabaseClusterRestoreReconciler) ensureClusterIsReady(ctx context.Context, restore *everestv1alpha1.DatabaseClusterRestore) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, clusterReadyTimeout)
	defer cancel()
	for {
		select {
		case <-timeoutCtx.Done():
			return errors.New("wait timeout exceeded")
		default:
			cluster := &everestv1alpha1.DatabaseCluster{}
			err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.DBClusterName, Namespace: restore.Namespace}, cluster)
			if err != nil {
				return err
			}
			if cluster.Status.Status == everestv1alpha1.AppStateReady ||
				cluster.Status.Status == everestv1alpha1.AppStateRestoring {
				return nil
			}
		}
	}
}

func (r *DatabaseClusterRestoreReconciler) restorePSMDB(
	ctx context.Context, restore *everestv1alpha1.DatabaseClusterRestore,
) (err error, needRequeue bool) {
	logger := log.FromContext(ctx)
	if err := r.ensureClusterIsReady(ctx, restore); err != nil {
		return err, false
	}

	psmdbDBCR := &psmdbv1.PerconaServerMongoDB{}
	err = r.Get(ctx, types.NamespacedName{Name: restore.Spec.DBClusterName, Namespace: restore.Namespace}, psmdbDBCR)
	if err != nil {
		logger.Error(err, "unable to fetch PerconaServerMongoDB")
		return err, false
	}

	if timeoutAfterReadyNotComplete(psmdbDBCR.Status.Conditions) {
		return nil, true
	}

	// We need to check if the storage used by the backup is defined in the
	// PerconaServerMongoDB CR. If not, we requeue the restore to give the
	// DatabaseCluster controller a chance to update the PSMDB cluster CR.
	// Otherwise, the restore will fail.
	if restore.Spec.DataSource.DBClusterBackupName != "" {
		backup := &everestv1alpha1.DatabaseClusterBackup{}
		err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.DataSource.DBClusterBackupName, Namespace: restore.Namespace}, backup)
		if err != nil {
			logger.Error(err, "unable to fetch DatabaseClusterBackup")
			return err, false
		}

		// If the backup storage is not defined in the PerconaServerMongoDB CR,
		// we cannot proceed
		if psmdbDBCR.Spec.Backup.Storages == nil {
			logger.Info(
				fmt.Sprintf("Backup storage %s is not defined in the psmdb cluster %s, requeuing",
					backup.Spec.BackupStorageName,
					restore.Spec.DBClusterName),
			)
			return ErrBackupStorageUndefined, false
		}
		if _, ok := psmdbDBCR.Spec.Backup.Storages[backup.Spec.BackupStorageName]; !ok {
			logger.Info(
				fmt.Sprintf("Backup storage %s is not defined in the psmdb cluster %s, requeuing",
					backup.Spec.BackupStorageName,
					restore.Spec.DBClusterName),
			)
			return ErrBackupStorageUndefined, false
		}
	}

	psmdbCR := &psmdbv1.PerconaServerMongoDBRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restore.Name,
			Namespace: restore.Namespace,
		},
	}
	if err := controllerutil.SetControllerReference(restore, psmdbCR, r.Client.Scheme()); err != nil {
		return err, false
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, psmdbCR, func() error {
		psmdbCR.Spec.ClusterName = restore.Spec.DBClusterName
		if restore.Spec.DataSource.DBClusterBackupName != "" {
			psmdbCR.Spec.BackupName = restore.Spec.DataSource.DBClusterBackupName
		}
		if restore.Spec.DataSource.BackupSource != nil {
			backupStorage := &everestv1alpha1.BackupStorage{}
			err := r.Get(ctx, types.NamespacedName{
				Name:      restore.Spec.DataSource.BackupSource.BackupStorageName,
				Namespace: restore.GetNamespace(),
			}, backupStorage)
			if err != nil {
				return errors.Join(err, fmt.Errorf("failed to get backup storage %s", restore.Spec.DataSource.BackupSource.BackupStorageName))
			}

			psmdbCR.Spec.BackupSource = &psmdbv1.PerconaServerMongoDBBackupStatus{
				Destination: restore.Spec.DataSource.BackupSource.Path,
			}
			switch backupStorage.Spec.Type {
			case everestv1alpha1.BackupStorageTypeS3:
				psmdbCR.Spec.BackupSource.S3 = &psmdbv1.BackupStorageS3Spec{
					Bucket:                backupStorage.Spec.Bucket,
					CredentialsSecret:     backupStorage.Spec.CredentialsSecretName,
					Region:                backupStorage.Spec.Region,
					EndpointURL:           backupStorage.Spec.EndpointURL,
					Prefix:                parsePrefixFromDestination(restore.Spec.DataSource.BackupSource.Path),
					InsecureSkipTLSVerify: !pointer.Get(backupStorage.Spec.VerifyTLS),
				}
			case everestv1alpha1.BackupStorageTypeAzure:
				psmdbCR.Spec.BackupSource.Azure = &psmdbv1.BackupStorageAzureSpec{
					Container:         backupStorage.Spec.Bucket,
					Prefix:            parsePrefixFromDestination(restore.Spec.DataSource.BackupSource.Path),
					CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
				}
			default:
				return fmt.Errorf("unsupported backup storage type %s for %s", backupStorage.Spec.Type, backupStorage.Name)
			}
		}

		if restore.Spec.DataSource.PITR != nil {
			if err := validatePitrRestoreSpec(restore.Spec.DataSource); err != nil {
				return err
			}

			psmdbCR.Spec.PITR = &psmdbv1.PITRestoreSpec{
				Type: psmdbv1.PITRestoreType(restore.Spec.DataSource.PITR.Type),
				Date: &psmdbv1.PITRestoreDate{Time: restore.Spec.DataSource.PITR.Date.Time},
			}
		}

		return nil
	})
	if err != nil {
		return err, false
	}

	psmdbCR = &psmdbv1.PerconaServerMongoDBRestore{}
	err = r.Get(ctx, types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}, psmdbCR)
	if err != nil {
		return err, false
	}

	restore.Status.State = everestv1alpha1.GetDBRestoreState(psmdbCR)
	restore.Status.CompletedAt = psmdbCR.Status.CompletedAt
	restore.Status.Message = psmdbCR.Status.Error

	return r.Status().Update(ctx, restore), false
}

func timeoutAfterReadyNotComplete(conditions []psmdbv1.ClusterCondition) bool {
	for _, condition := range conditions {
		if condition.Type == psmdbv1.AppStateReady && condition.Status == psmdbv1.ConditionTrue {
			return time.Since(condition.LastTransitionTime.Time) < timeoutWaitingAfterClusterIsReady
		}
	}
	return true
}

func (r *DatabaseClusterRestoreReconciler) restorePXC(
	ctx context.Context,
	restore *everestv1alpha1.DatabaseClusterRestore,
	db *everestv1alpha1.DatabaseCluster,
) error {
	pxcCR := &pxcv1.PerconaXtraDBClusterRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restore.Name,
			Namespace: restore.Namespace,
		},
	}

	if err := controllerutil.SetControllerReference(restore, pxcCR, r.Client.Scheme()); err != nil {
		return err
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pxcCR, func() error {
		pxcCR.Spec.PXCCluster = restore.Spec.DBClusterName
		if restore.Spec.DataSource.DBClusterBackupName != "" {
			pxcCR.Spec.BackupName = restore.Spec.DataSource.DBClusterBackupName
		}

		dataSource := restore.Spec.DataSource
		if dataSource.BackupSource != nil {
			backupStorage := &everestv1alpha1.BackupStorage{}
			err := r.Get(ctx, types.NamespacedName{
				Name:      dataSource.BackupSource.BackupStorageName,
				Namespace: restore.GetNamespace(),
			}, backupStorage)
			if err != nil {
				return errors.Join(err, fmt.Errorf("failed to get backup storage %s",
					restore.Spec.DataSource.BackupSource.BackupStorageName))
			}

			dest := fmt.Sprintf("s3://%s/%s", backupStorage.Spec.Bucket, dataSource.BackupSource.Path)
			pxcCR.Spec.BackupSource = &pxcv1.PXCBackupStatus{
				Destination: pxcv1.PXCBackupDestination(dest),
				VerifyTLS:   backupStorage.Spec.VerifyTLS,
			}
			switch backupStorage.Spec.Type {
			case everestv1alpha1.BackupStorageTypeS3:
				pxcCR.Spec.BackupSource.S3 = &pxcv1.BackupStorageS3Spec{
					Bucket: fmt.Sprintf(
						"%s/%s",
						backupStorage.Spec.Bucket,
						common.BackupStoragePrefix(db),
					),
					CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
					Region:            backupStorage.Spec.Region,
					EndpointURL:       backupStorage.Spec.EndpointURL,
				}
			case everestv1alpha1.BackupStorageTypeAzure:
				pxcCR.Spec.BackupSource.Azure = &pxcv1.BackupStorageAzureSpec{
					ContainerPath: fmt.Sprintf(
						"%s/%s",
						backupStorage.Spec.Bucket,
						common.BackupStoragePrefix(db),
					),
					CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
				}
			default:
				return fmt.Errorf("unsupported backup storage type %s for %s", backupStorage.Spec.Type, backupStorage.Name)
			}
		}
		if dataSource.PITR != nil {
			spec, err := r.genPXCPitrRestoreSpec(ctx, dataSource, *db)
			if err != nil {
				return err
			}
			pxcCR.Spec.PITR = spec
		}
		return nil
	})
	if err != nil {
		return err
	}

	pxcCR = &pxcv1.PerconaXtraDBClusterRestore{}
	err = r.Get(ctx, types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}, pxcCR)
	if err != nil {
		return err
	}

	restore.Status.State = everestv1alpha1.GetDBRestoreState(pxcCR)
	restore.Status.CompletedAt = pxcCR.Status.CompletedAt
	restore.Status.Message = pxcCR.Status.Comments

	return r.Status().Update(ctx, restore)
}

func (r *DatabaseClusterRestoreReconciler) restorePG(ctx context.Context, restore *everestv1alpha1.DatabaseClusterRestore) error {
	logger := log.FromContext(ctx)

	var backupStorageName string
	var backupBaseName string
	if restore.Spec.DataSource.DBClusterBackupName != "" {
		backup := &everestv1alpha1.DatabaseClusterBackup{}
		err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.DataSource.DBClusterBackupName, Namespace: restore.Namespace}, backup)
		if err != nil {
			logger.Error(err, "unable to fetch DatabaseClusterBackup")
			return err
		}

		backupStorageName = backup.Spec.BackupStorageName
		if backup.Status.Destination != nil {
			backupBaseName = filepath.Base(*backup.Status.Destination)
		}
	}
	if restore.Spec.DataSource.BackupSource != nil {
		backupStorageName = restore.Spec.DataSource.BackupSource.BackupStorageName
		backupBaseName = filepath.Base(restore.Spec.DataSource.BackupSource.Path)
	}

	pgDBCR := &pgv2.PerconaPGCluster{}
	err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.DBClusterName, Namespace: restore.Namespace}, pgDBCR)
	if err != nil {
		logger.Error(err, "unable to fetch PerconaPGCluster")
		return err
	}

	backupStorage := &everestv1alpha1.BackupStorage{}
	err = r.Get(ctx, types.NamespacedName{
		Name: backupStorageName, Namespace: restore.GetNamespace(),
	}, backupStorage)
	if err != nil {
		return errors.Join(err, fmt.Errorf("failed to get backup storage %s", restore.Spec.DataSource.BackupSource.BackupStorageName))
	}

	// We need to check if the storage used by the backup is defined in the
	// PerconaPGCluster CR. If not, we requeue the restore to give the
	// DatabaseCluster controller a chance to update the PG cluster CR.
	// Otherwise, the restore will fail.
	repoName := common.GetRepoNameByBackupStorage(backupStorage, pgDBCR.Spec.Backups.PGBackRest.Repos)
	if repoName == "" {
		logger.Info(
			fmt.Sprintf("Backup storage %s is not defined in the pg cluster %s, requeuing",
				backupStorageName,
				restore.Spec.DBClusterName),
		)
		return ErrBackupStorageUndefined
	}

	pgCR := &pgv2.PerconaPGRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restore.Name,
			Namespace: restore.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, pgCR, func() error {
		pgCR.Spec.PGCluster = restore.Spec.DBClusterName
		pgCR.Spec.RepoName = repoName
		pgCR.Spec.Options, err = getPGRestoreOptions(restore.Spec.DataSource, backupBaseName)
		return controllerutil.SetControllerReference(restore, pgCR, r.Client.Scheme())
	})
	if err != nil {
		return err
	}

	restore.Status.State = everestv1alpha1.GetDBRestoreState(pgCR)
	restore.Status.CompletedAt = pgCR.Status.CompletedAt

	return r.Status().Update(ctx, restore)
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseClusterRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.initIndexers(context.Background(), mgr); err != nil {
		return err
	}

	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		Named("DatabaseClusterRestore").
		For(&everestv1alpha1.DatabaseClusterRestore{}).
		Watches(
			&corev1.Namespace{},
			common.EnqueueObjectsInNamespace(r.Client, &everestv1alpha1.DatabaseClusterRestoreList{}),
		).
		WithEventFilter(common.DefaultNamespaceFilter)

	// Normally we would call `Complete()`, however, with `Build()`, we get a handle to the underlying controller,
	// so that we can dynamically add watchers from the DatabaseEngine reconciler.
	ctrl, err := ctrlBuilder.Build(r)
	if err != nil {
		return err
	}
	log := mgr.GetLogger().WithName("DynamicWatcher").WithValues("controller", "DatabaseClusterRestore")
	r.controller = newControllerWatcherRegistry(log, ctrl)
	return nil
}

func (r *DatabaseClusterRestoreReconciler) initIndexers(ctx context.Context, mgr ctrl.Manager) error {
	err := mgr.GetFieldIndexer().IndexField(
		ctx, &everestv1alpha1.DatabaseClusterRestore{}, dbClusterRestoreDBClusterNameField,
		func(rawObj client.Object) []string {
			var res []string
			dbr, ok := rawObj.(*everestv1alpha1.DatabaseClusterRestore)
			if !ok {
				return res
			}
			return append(res, dbr.Spec.DBClusterName)
		},
	)
	if err != nil {
		return err
	}

	err = mgr.GetFieldIndexer().IndexField(
		ctx, &everestv1alpha1.DatabaseClusterRestore{}, dbClusterRestoreDataSourceBackupStorageNameField,
		func(rawObj client.Object) []string {
			var res []string
			dbr, ok := rawObj.(*everestv1alpha1.DatabaseClusterRestore)
			if !ok {
				return res
			}
			if pointer.Get(dbr.Spec.DataSource.BackupSource).BackupStorageName != "" {
				res = append(res, dbr.Spec.DataSource.BackupSource.BackupStorageName)
			}
			return res
		},
	)
	return err
}

func parsePrefixFromDestination(url string) string {
	parts := strings.Split(url, "/")
	l := len(parts)
	// taking the third and the second last parts of the destination
	return fmt.Sprintf("%s/%s", parts[l-3], parts[l-2])
}

func (r *DatabaseClusterRestoreReconciler) genPXCPitrRestoreSpec(
	ctx context.Context,
	dataSource everestv1alpha1.DatabaseClusterRestoreDataSource,
	db everestv1alpha1.DatabaseCluster,
) (*pxcv1.PITR, error) {
	// use 'date' as default
	if dataSource.PITR.Type == "" {
		dataSource.PITR.Type = everestv1alpha1.PITRTypeDate
	}

	if err := validatePitrRestoreSpec(dataSource); err != nil {
		return nil, err
	}

	// First get the source backup object.
	// Note: This assumes that we will always restore to same namespace, even to a new cluster.
	sourceBackup := &everestv1alpha1.DatabaseClusterBackup{}
	key := types.NamespacedName{Name: dataSource.DBClusterBackupName, Namespace: db.GetNamespace()}
	if err := r.Get(ctx, key, sourceBackup); err != nil {
		return nil, fmt.Errorf("failed to get source backup %s: %w", dataSource.DBClusterBackupName, err)
	}
	// Get the source cluster the backup belongs to.
	sourceDB := &everestv1alpha1.DatabaseCluster{}
	key = types.NamespacedName{Name: sourceBackup.Spec.DBClusterName, Namespace: sourceBackup.GetNamespace()}
	if err := r.Get(ctx, key, sourceDB); err != nil {
		return nil, fmt.Errorf("failed to get source cluster for backup %s: %w", dataSource.DBClusterBackupName, err)
	}
	// Get the storage object where the source backup is stored.
	backupStorage := &everestv1alpha1.BackupStorage{}

	if sourceDB.Spec.Backup.PITR.BackupStorageName == nil || *sourceDB.Spec.Backup.PITR.BackupStorageName == "" {
		return nil, fmt.Errorf("no backup storage defined for PITR in %s cluster", sourceDB.Name)
	}
	storageName := *sourceDB.Spec.Backup.PITR.BackupStorageName

	key = types.NamespacedName{Name: storageName, Namespace: db.GetNamespace()}
	if err := r.Get(ctx, key, backupStorage); err != nil {
		return nil, fmt.Errorf("failed to get backup storage '%s' for backup: %w", storageName, err)
	}

	spec := &pxcv1.PITR{
		BackupSource: &pxcv1.PXCBackupStatus{},
		Type:         string(dataSource.PITR.Type),
		Date:         dataSource.PITR.Date.Format(everestv1alpha1.DateFormatSpace),
	}

	switch backupStorage.Spec.Type {
	case everestv1alpha1.BackupStorageTypeS3:
		spec.BackupSource.S3 = &pxcv1.BackupStorageS3Spec{
			CredentialsSecret: backupStorage.Spec.CredentialsSecretName,
			Region:            backupStorage.Spec.Region,
			EndpointURL:       backupStorage.Spec.EndpointURL,
			Bucket:            common.PITRBucketName(sourceDB, backupStorage.Spec.Bucket),
		}
		//nolint:godox
		// TODO: add support for Azure.
	default:
		return nil, fmt.Errorf("unsupported backup storage type %s for %s", backupStorage.Spec.Type, backupStorage.Name)
	}
	return spec, nil
}

func getPGRestoreOptions(dataSource everestv1alpha1.DatabaseClusterRestoreDataSource, backupBaseName string) ([]string, error) {
	options := []string{
		"--set=" + backupBaseName,
	}

	if dataSource.PITR != nil {
		if err := validatePitrRestoreSpec(dataSource); err != nil {
			return nil, err
		}
		dateString := fmt.Sprintf(`"%s"`, dataSource.PITR.Date.Format(everestv1alpha1.DateFormatSpace))
		options = append(options, "--type="+pgBackupTypeDate)
		options = append(options, "--target="+dateString)
	} else {
		options = append(options, "--type="+pgBackupTypeImmediate)
	}

	return options, nil
}

func validatePitrRestoreSpec(dataSource everestv1alpha1.DatabaseClusterRestoreDataSource) error {
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
			return errPitrEmptyDate
		}
	case everestv1alpha1.PITRTypeLatest:
		//nolint:godox
		// TODO: figure out why "latest" doesn't work currently for Everest
		return errPitrTypeLatest
	default:
		return errPitrTypeIsNotSupported
	}
	return nil
}

// ReconcileWatchers reconciles the watchers for the DatabaseClusterRestore controller.
func (r *DatabaseClusterRestoreReconciler) ReconcileWatchers(ctx context.Context) error {
	dbEngines := &everestv1alpha1.DatabaseEngineList{}
	if err := r.List(ctx, dbEngines); err != nil {
		return err
	}

	log := log.FromContext(ctx)
	addWatcher := func(dbEngineType everestv1alpha1.EngineType, obj client.Object, f func(context.Context, client.Object) error) error {
		if err := r.controller.addWatchers(string(dbEngineType), source.Kind(r.Cache, obj, r.watchHandler(f))); err != nil {
			return err
		}
		return nil
	}

	for _, dbEngine := range dbEngines.Items {
		if dbEngine.Status.State != everestv1alpha1.DBEngineStateInstalled {
			continue
		}
		switch t := dbEngine.Spec.Type; t {
		case everestv1alpha1.DatabaseEnginePXC:
			if err := addWatcher(t, &pxcv1.PerconaXtraDBClusterRestore{}, nil); err != nil {
				return err
			}
		case everestv1alpha1.DatabaseEnginePostgresql:
			if err := addWatcher(t, &pgv2.PerconaPGRestore{}, r.tryCreatePG); err != nil {
				return err
			}
		case everestv1alpha1.DatabaseEnginePSMDB:
			if err := addWatcher(t, &psmdbv1.PerconaServerMongoDBRestore{}, nil); err != nil {
				return err
			}
		default:
			log.Info("Unknown database engine type", "type", dbEngine.Spec.Type)
			continue
		}
	}
	return nil
}

func (r *DatabaseClusterRestoreReconciler) watchHandler(creationFunc func(ctx context.Context, obj client.Object) error) handler.Funcs { //nolint:dupl
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.tryCreateDBRestore(ctx, e.Object, creationFunc)
			q.Add(reconcileRequestFromObject(e.Object))
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.tryCreateDBRestore(ctx, e.ObjectNew, creationFunc)
			q.Add(reconcileRequestFromObject(e.ObjectNew))
		},
		DeleteFunc: func(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.tryDeleteDBRestore(ctx, e.Object)
			q.Add(reconcileRequestFromObject(e.Object))
		},
	}
}

func (r *DatabaseClusterRestoreReconciler) tryCreateDBRestore(
	ctx context.Context,
	obj client.Object,
	createRestoreFunc func(ctx context.Context, obj client.Object) error,
) {
	if createRestoreFunc == nil {
		return
	}

	logger := log.FromContext(ctx)
	if len(obj.GetOwnerReferences()) == 0 {
		err := createRestoreFunc(ctx, obj)
		if err != nil {
			logger.Error(err, "Failed to create DatabaseClusterRestore "+obj.GetName())
		}
	}
}

func (r *DatabaseClusterRestoreReconciler) tryCreatePG(ctx context.Context, obj client.Object) error {
	pgRestore := &pgv2.PerconaPGRestore{}
	namespacedName := types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}

	if err := r.Get(ctx, namespacedName, pgRestore); err != nil {
		// if such upstream restore is not found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	pg := &pgv2.PerconaPGCluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: pgRestore.Spec.PGCluster}, pg); err != nil {
		// if such upstream cluster is not found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	storages := &everestv1alpha1.BackupStorageList{}
	if err := r.List(ctx, storages, &client.ListOptions{Namespace: namespacedName.Namespace}); err != nil {
		// if no backup storages found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	restore := &everestv1alpha1.DatabaseClusterRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      obj.GetName(),
			Namespace: obj.GetNamespace(),
		},
	}

	err := r.Get(ctx, namespacedName, restore)
	// if such everest restore already exists - do nothing
	if err == nil {
		return nil
	}

	if !k8serrors.IsNotFound(err) {
		return err
	}

	restore.Spec.DBClusterName = pgRestore.Spec.PGCluster

	cluster := &everestv1alpha1.DatabaseCluster{}

	err = r.Get(ctx, types.NamespacedName{Name: pgRestore.Spec.PGCluster, Namespace: pgRestore.Namespace}, cluster)
	if err != nil {
		return err
	}
	// The restore that we try to create when pg-restore CR appears is always a bootstrap restore
	// so we take the DataSource details from the dbCluster.DataSource.
	if cluster.Spec.DataSource != nil { //nolint:nestif
		// if the bootstrap restore is triggered by DataImport - delete the upstream pg-restore (it's excessive)
		// and do not create a dbr for it accordingly
		if cluster.Spec.DataSource.DataImport != nil {
			err = r.Delete(ctx, obj)
			if err != nil {
				return err
			}

			return nil
		}

		if cluster.Spec.DataSource.DBClusterBackupName != "" {
			restore.Spec.DataSource.DBClusterBackupName = cluster.Spec.DataSource.DBClusterBackupName
		}

		if cluster.Spec.DataSource.BackupSource != nil {
			restore.Spec.DataSource.BackupSource = &everestv1alpha1.BackupSource{
				Path:              "",
				BackupStorageName: cluster.Spec.DataSource.BackupSource.BackupStorageName,
			}
		}

		if cluster.Spec.DataSource.PITR != nil {
			restore.Spec.DataSource.PITR = &everestv1alpha1.PITR{
				Type: cluster.Spec.DataSource.PITR.Type,
				Date: cluster.Spec.DataSource.PITR.Date,
			}
		}
	}

	restore.Labels = map[string]string{
		consts.DatabaseClusterNameLabel: pgRestore.Spec.PGCluster,
	}
	if err = controllerutil.SetControllerReference(cluster, restore, r.Scheme); err != nil {
		return err
	}

	return r.Create(ctx, restore)
}

func (r *DatabaseClusterRestoreReconciler) tryDeleteDBRestore(ctx context.Context, obj client.Object) {
	logger := log.FromContext(ctx)

	name := obj.GetName()
	namespace := obj.GetNamespace()
	dbr := &everestv1alpha1.DatabaseClusterRestore{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}

	if err := r.Delete(ctx, dbr); err != nil {
		if k8serrors.IsNotFound(err) {
			return
		}

		logger.Error(err, "Failed to delete the DatabaseClusterRestore", "name", name, "namespace", namespace)
	}
}
