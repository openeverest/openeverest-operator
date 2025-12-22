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

package everest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/AlekSi/pointer"
	goversion "github.com/hashicorp/go-version"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

const (
	backupRetryAfter    = 10 * time.Second
	pxcGapsReasonString = "BinlogGapDetected"
)

// DatabaseClusterBackupReconciler reconciles a DatabaseClusterBackup object.
type DatabaseClusterBackupReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Cache     cache.Cache

	controller *controllerWatcherRegistry
}

// +kubebuilder:rbac:groups=everest.percona.com,resources=databaseclusterbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=everest.percona.com,resources=databaseclusterbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=everest.percona.com,resources=databaseclusterbackups/finalizers,verbs=update
// +kubebuilder:rbac:groups=pxc.percona.com,resources=perconaxtradbclusterbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=psmdb.percona.com,resources=perconaservermongodbbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgv2.percona.com,resources=perconapgbackups,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// Modify the Reconcile function to compare the state specified by
// the DatabaseClusterBackup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *DatabaseClusterBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling")
	defer func() {
		logger.Info("Reconciled")
	}()

	var err error
	backup := &everestv1alpha1.DatabaseClusterBackup{}
	if err = r.Get(ctx, req.NamespacedName, backup); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, fmt.Sprintf("failed to fetch DatabaseClusterBackup='%s'", req.NamespacedName))
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	cluster := &everestv1alpha1.DatabaseCluster{}
	if err = r.Get(ctx, types.NamespacedName{Name: backup.Spec.DBClusterName, Namespace: backup.Namespace}, cluster); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, fmt.Sprintf("failed to fetch DatabaseCluster='%s'",
				types.NamespacedName{Name: backup.Spec.DBClusterName, Namespace: backup.Namespace}))
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	defer func() {
		// Update the status and finalizers of the DatabaseClusterBackup object after the reconciliation.
		if err = r.reconcileStatus(ctx, backup, cluster); err != nil {
			logger.Error(err, "failed to update DatabaseClusterBackup status")
		}
	}()

	if err = r.reconcileMeta(ctx, backup, cluster); err != nil {
		return ctrl.Result{}, err
	}

	requeue := false
	switch cluster.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePXC:
		requeue, err = r.reconcilePXC(ctx, backup)
	case everestv1alpha1.DatabaseEnginePSMDB:
		requeue, err = r.reconcilePSMDB(ctx, backup)
	case everestv1alpha1.DatabaseEnginePostgresql:
		requeue, err = r.reconcilePG(ctx, backup)
	}

	if err != nil {
		logger.Error(err, fmt.Sprintf("failed to reconcile DatabaseCluserBackup for %s DatabaseCluster='%s'",
			cluster.Spec.Engine.Type,
			client.ObjectKeyFromObject(cluster)))
		return ctrl.Result{}, err
	}

	if requeue {
		return ctrl.Result{RequeueAfter: backupRetryAfter}, nil
	}
	return ctrl.Result{}, nil
}

// ReconcileWatchers reconciles the watchers for the DatabaseClusterBackup controller.
func (r *DatabaseClusterBackupReconciler) ReconcileWatchers(ctx context.Context) error {
	dbEngines := &everestv1alpha1.DatabaseEngineList{}
	if err := r.List(ctx, dbEngines); err != nil {
		return err
	}

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
			if err := addWatcher(t, &pxcv1.PerconaXtraDBClusterBackup{}, r.tryCreatePXC); err != nil {
				return err
			}
		case everestv1alpha1.DatabaseEnginePostgresql:
			if err := addWatcher(t, &pgv2.PerconaPGBackup{}, r.tryCreatePG); err != nil {
				return err
			}
		case everestv1alpha1.DatabaseEnginePSMDB:
			if err := addWatcher(t, &psmdbv1.PerconaServerMongoDBBackup{}, r.tryCreatePSMDB); err != nil {
				return err
			}
		default:
			log.FromContext(ctx).Info("Unknown database engine type", "type", dbEngine.Spec.Type)
			continue
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseClusterBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.initIndexers(context.Background(), mgr); err != nil {
		return err
	}

	// Predicate to trigger reconciliation only on .spec.dataSource.dbClusterBackupName changes in the DatabaseClusterRestore resource.
	dbClusterRestoreEventsPredicate := predicate.Funcs{
		// Allow create events only if the .spec.dataSource.dbClusterBackupName is set
		CreateFunc: func(e event.CreateEvent) bool {
			dbcr, ok := e.Object.(*everestv1alpha1.DatabaseClusterRestore)
			if !ok {
				return false
			}
			return dbcr.Spec.DataSource.DBClusterBackupName != ""
		},

		// DatabaseClusterRestore resources don't support updates to the .spec.dataSource.dbClusterBackupName field
		UpdateFunc: func(e event.UpdateEvent) bool {
			dbcr, ok := e.ObjectOld.(*everestv1alpha1.DatabaseClusterRestore)
			if !ok {
				return false
			}
			return dbcr.Spec.DataSource.DBClusterBackupName != ""
		},

		// Allow delete events only if the .spec.dataSource.dbClusterBackupName is set
		DeleteFunc: func(e event.DeleteEvent) bool {
			dbcr, ok := e.Object.(*everestv1alpha1.DatabaseClusterRestore)
			if !ok {
				return false
			}
			return dbcr.Spec.DataSource.DBClusterBackupName != ""
		},

		// Nothing to process on generic events
		GenericFunc: func(_ event.GenericEvent) bool {
			return false
		},
	}

	// Predicate to trigger reconciliation only on .spec.dataSource.dbClusterBackupName changes in the DatabaseCluster resource.
	dbClusterEventsPredicate := predicate.Funcs{ //nolint:dupl
		// Allow create events only if the .spec.dataSource.dbClusterBackupName is set
		CreateFunc: func(e event.CreateEvent) bool {
			db, ok := e.Object.(*everestv1alpha1.DatabaseCluster)
			if !ok {
				return false
			}
			return pointer.Get(db.Spec.DataSource).DBClusterBackupName != ""
		},

		// DatabaseClusterRestore resources don't support updates to the .spec.dataSource.dbClusterBackupName field
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldDB, oldOk := e.ObjectOld.(*everestv1alpha1.DatabaseCluster)
			newDB, newOk := e.ObjectNew.(*everestv1alpha1.DatabaseCluster)
			if !oldOk || !newOk {
				return false
			}

			// Trigger reconciliation only if the .spec.dataSource.dbClusterBackupName field has changed
			return pointer.Get(oldDB.Spec.DataSource).DBClusterBackupName !=
				pointer.Get(newDB.Spec.DataSource).DBClusterBackupName
		},

		// Allow delete events only if the .spec.dataSource.dbClusterBackupName is set
		DeleteFunc: func(e event.DeleteEvent) bool {
			db, ok := e.Object.(*everestv1alpha1.DatabaseCluster)
			if !ok {
				return false
			}
			return pointer.Get(db.Spec.DataSource).DBClusterBackupName != ""
		},

		// Nothing to process on generic events
		GenericFunc: func(_ event.GenericEvent) bool {
			return false
		},
	}

	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		Named("DatabaseClusterBackup").
		For(&everestv1alpha1.DatabaseClusterBackup{})
	ctrlBuilder.Watches(
		&corev1.Namespace{},
		common.EnqueueObjectsInNamespace(r.Client, &everestv1alpha1.DatabaseClusterBackupList{}),
	).
		Watches(
			// Watch for DatabaseClusterRestore resources that reference DatabaseClusterBackup
			&everestv1alpha1.DatabaseClusterRestore{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				dbcr, ok := obj.(*everestv1alpha1.DatabaseClusterRestore)
				if !ok {
					return []reconcile.Request{}
				}
				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							// dbClusterRestoreEventsPredicate events filter allows only DBRestores
							// with the .spec.dataSource.dbClusterBackupName set
							Name:      dbcr.Spec.DataSource.DBClusterBackupName,
							Namespace: dbcr.Namespace,
						},
					},
				}
			}),
			builder.WithPredicates(dbClusterRestoreEventsPredicate),
		).
		Watches(
			// Watch for DatabaseCluster resources that reference DatabaseClusterBackup
			&everestv1alpha1.DatabaseCluster{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				db, ok := obj.(*everestv1alpha1.DatabaseCluster)
				if !ok {
					return []reconcile.Request{}
				}

				if pointer.Get(db.Spec.DataSource).DBClusterBackupName == "" {
					return []reconcile.Request{}
				}

				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							// dbClusterEventsPredicate events filter allows only DBRestores
							// with the .spec.dataSource.dbClusterBackupName set
							Name:      db.Spec.DataSource.DBClusterBackupName,
							Namespace: db.Namespace,
						},
					},
				}
			}),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}, dbClusterEventsPredicate),
		)
	ctrlBuilder.WithEventFilter(common.DefaultNamespaceFilter)

	// Normally we would call `Complete()`, however, with `Build()`, we get a handle to the underlying controller,
	// so that we can dynamically add watchers from the DatabaseEngine reconciler.
	controller, err := ctrlBuilder.Build(r)
	if err != nil {
		return err
	}
	logger := mgr.GetLogger().WithName("DynamicWatcher").WithValues("controller", "DatabaseClusterBackup")
	r.controller = newControllerWatcherRegistry(logger, controller)
	return nil
}

func (r *DatabaseClusterBackupReconciler) getBackupStatus(
	ctx context.Context,
	backup *everestv1alpha1.DatabaseClusterBackup,
	db *everestv1alpha1.DatabaseCluster,
) (everestv1alpha1.DatabaseClusterBackupStatus, error) {
	var err error
	logger := log.FromContext(ctx)
	namespacedName := client.ObjectKeyFromObject(backup)

	backupStatus := everestv1alpha1.DatabaseClusterBackupStatus{}
	if !backup.GetDeletionTimestamp().IsZero() {
		backupStatus.State = everestv1alpha1.BackupDeleting
		return backupStatus, nil
	}

	switch db.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePXC:
		pxcCR := &pxcv1.PerconaXtraDBClusterBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      namespacedName.Name,
				Namespace: namespacedName.Namespace,
			},
		}
		if err = r.Get(ctx, namespacedName, pxcCR); err != nil {
			if client.IgnoreNotFound(err) != nil {
				msg := fmt.Sprintf("failed to fetch PerconaXtraDBClusterBackup='%s' to update status",
					namespacedName)
				logger.Error(err, msg)
				return backupStatus, fmt.Errorf("%s: %w", msg, err)
			}
			return backupStatus, nil
		}
		backupStatus.State = everestv1alpha1.GetDBBackupState(pxcCR)
		backupStatus.CompletedAt = pxcCR.Status.CompletedAt
		backupStatus.CreatedAt = &pxcCR.CreationTimestamp
		backupStatus.Destination = pointer.To(string(pxcCR.Status.Destination))
		for _, condition := range pxcCR.Status.Conditions {
			if condition.Reason == pxcGapsReasonString {
				backupStatus.Gaps = true
			}
		}
		backupStatus.LatestRestorableTime = pxcCR.Status.LatestRestorableTime
	case everestv1alpha1.DatabaseEnginePSMDB:
		psmdbCR := &psmdbv1.PerconaServerMongoDBBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      namespacedName.Name,
				Namespace: namespacedName.Namespace,
			},
		}
		if err = r.Get(ctx, namespacedName, psmdbCR); err != nil {
			if client.IgnoreNotFound(err) != nil {
				msg := fmt.Sprintf("failed to fetch PerconaServerMongoDBBackup='%s' to update status",
					namespacedName)
				logger.Error(err, msg)
				return backupStatus, fmt.Errorf("%s: %w", msg, err)
			}
			return backupStatus, nil
		}
		backupStatus.State = everestv1alpha1.GetDBBackupState(psmdbCR)
		backupStatus.CompletedAt = psmdbCR.Status.CompletedAt
		backupStatus.CreatedAt = &psmdbCR.CreationTimestamp
		backupStatus.Destination = &psmdbCR.Status.Destination
		backupStatus.LatestRestorableTime = psmdbCR.Status.LatestRestorableTime
	case everestv1alpha1.DatabaseEnginePostgresql:
		pgCR := &pgv2.PerconaPGBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      namespacedName.Name,
				Namespace: namespacedName.Namespace,
			},
		}
		if err = r.Get(ctx, namespacedName, pgCR); err != nil {
			if client.IgnoreNotFound(err) != nil {
				msg := fmt.Sprintf("failed to fetch PerconaPGBackup='%s' to update status",
					namespacedName)
				logger.Error(err, msg)
				return backupStatus, fmt.Errorf("%s: %w", msg, err)
			}
			return backupStatus, nil
		}

		backupStorage := &everestv1alpha1.BackupStorage{}
		err := r.Get(ctx, types.NamespacedName{
			Name:      backup.Spec.BackupStorageName,
			Namespace: namespacedName.Namespace,
		}, backupStorage)
		if err != nil {
			return backupStatus, fmt.Errorf("failed to get backup storage %s", backup.Spec.BackupStorageName)
		}
		backupStatus.State = everestv1alpha1.GetDBBackupState(pgCR)
		backupStatus.CompletedAt = pgCR.Status.CompletedAt
		backupStatus.CreatedAt = &pgCR.CreationTimestamp
		backupStatus.Destination = getLastPGBackupDestination(backupStorage, db, pgCR)
		backupStatus.LatestRestorableTime = pgCR.Status.LatestRestorableTime.Time
	}

	return backupStatus, nil
}

func (r *DatabaseClusterBackupReconciler) reconcileStatus(
	ctx context.Context,
	backup *everestv1alpha1.DatabaseClusterBackup,
	db *everestv1alpha1.DatabaseCluster,
) error {
	logger := log.FromContext(ctx)
	namespacedName := client.ObjectKeyFromObject(backup)

	backupStatus, err := r.getBackupStatus(ctx, backup, db)
	if err != nil {
		logger.Error(err, fmt.Sprintf("failed to get status for DatabaseClusterBackup='%s'", namespacedName))
		return err
	}

	// backup can be used in the following cases:
	// - DB cluster restoration from this backup is in progress
	// - DB cluster creation from this backup is in progress
	// - backup itself is in progress

	isRestoreInProgress := func() bool {
		// Check if any DatabaseClusterRestore resources are using this backup and are in progress.
		dbRestores, err := common.DatabaseClusterRestoresThatReferenceObject(
			ctx,
			r.Client,
			dbClusterRestoreBackupNameField,
			namespacedName.Namespace,
			namespacedName.Name,
		)
		if err != nil {
			msg := fmt.Sprintf("failed to fetch DatabaseClusterRestores that use DatabaseClusterBackup='%s'", namespacedName)
			logger.Error(err, msg)
			return false
		}

		return slices.ContainsFunc(dbRestores.Items, func(restore everestv1alpha1.DatabaseClusterRestore) bool {
			return restore.IsInProgress()
		})
	}

	isDbCreationFromBackupInProgress := func() bool {
		// Check if any DatabaseCluster resources are using this backup and are in creation progress.
		dbList, err := common.DatabaseClustersThatReferenceObject(
			ctx,
			r.Client,
			dbClusterRestoreBackupNameField,
			namespacedName.Namespace,
			namespacedName.Name,
		)
		if err != nil {
			msg := fmt.Sprintf("failed to fetch DatabaseClusters that use DatabaseClusterBackup='%s'", namespacedName)
			logger.Error(err, msg)
			return false
		}

		return slices.ContainsFunc(dbList.Items, func(db everestv1alpha1.DatabaseCluster) bool {
			switch db.Status.Status.WithCreatingState() {
			case everestv1alpha1.AppStateCreating,
				everestv1alpha1.AppStateInit,
				everestv1alpha1.AppStateRestoring:
				return true
			default:
				return false
			}
		})
	}

	backupStatus.InUse = backup.IsInProgress() ||
		isRestoreInProgress() ||
		isDbCreationFromBackupInProgress()

	if err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if cErr := r.Get(ctx, namespacedName, backup); cErr != nil {
			return cErr
		}

		backup.Status = backupStatus

		if err = common.EnsureInUseFinalizer(ctx, r.Client, backup.Status.InUse, backup); err != nil {
			return err
		}
		return r.Client.Status().Update(ctx, backup)
	}); client.IgnoreNotFound(err) != nil {
		msg := fmt.Sprintf("failed to update status for DatabaseClusterBackup='%s'", namespacedName)
		logger.Error(err, msg)
		return fmt.Errorf("%s: %w", msg, err)
	}
	return nil
}

func (r *DatabaseClusterBackupReconciler) reconcileMeta(
	ctx context.Context,
	backup *everestv1alpha1.DatabaseClusterBackup,
	cluster *everestv1alpha1.DatabaseCluster,
) error {
	var needUpdate bool

	// DBBackups are always deleted in foreground.
	if backup.GetDeletionTimestamp().IsZero() &&
		controllerutil.AddFinalizer(backup, consts.ForegroundDeletionFinalizer) {
		needUpdate = true
	}

	if _, ok := backup.Labels[consts.DatabaseClusterNameLabel]; !ok {
		if len(backup.Labels) == 0 {
			backup.Labels = make(map[string]string)
		}
		backup.Labels[consts.DatabaseClusterNameLabel] = backup.Spec.DBClusterName
		needUpdate = true
	}

	if metav1.GetControllerOf(backup) == nil {
		if err := controllerutil.SetControllerReference(cluster, backup, r.Client.Scheme()); err != nil {
			return err
		}

		needUpdate = true
	}

	if needUpdate {
		return r.Update(ctx, backup)
	}

	return nil
}

func (r *DatabaseClusterBackupReconciler) initIndexers(ctx context.Context, mgr ctrl.Manager) error {
	// Index the dbClusterName field in DatabaseClusterBackup.
	err := mgr.GetFieldIndexer().IndexField(
		ctx, &everestv1alpha1.DatabaseClusterBackup{}, consts.DBClusterBackupDBClusterNameField,
		func(o client.Object) []string {
			var res []string
			dbb, ok := o.(*everestv1alpha1.DatabaseClusterBackup)
			if !ok {
				return res
			}
			res = append(res, dbb.Spec.DBClusterName)
			return res
		},
	)
	if err != nil {
		return err
	}

	// Index the DBClusterBackupBackupStorageNameField field in DatabaseClusterBackup.
	err = mgr.GetFieldIndexer().IndexField(
		ctx, &everestv1alpha1.DatabaseClusterBackup{}, consts.DBClusterBackupBackupStorageNameField,
		func(o client.Object) []string {
			var res []string
			dbb, ok := o.(*everestv1alpha1.DatabaseClusterBackup)
			if !ok {
				return res
			}
			res = append(res, dbb.Spec.BackupStorageName)
			return res
		},
	)

	return err
}

func (r *DatabaseClusterBackupReconciler) watchHandler(creationFunc func(ctx context.Context, obj client.Object) error) handler.Funcs { //nolint:dupl
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.tryCreateDBBackups(ctx, e.Object, creationFunc)
			q.Add(common.ReconcileRequestFromObject(e.Object))
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			// remove the r.tryCreateDBBackups call below once https://perconadev.atlassian.net/browse/K8SPSMDB-1088 is fixed.
			r.tryCreateDBBackups(ctx, e.ObjectNew, creationFunc)

			q.Add(common.ReconcileRequestFromObject(e.ObjectNew))
		},
		DeleteFunc: func(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.tryDeleteDBBackup(ctx, e.Object)
			q.Add(common.ReconcileRequestFromObject(e.Object))
		},
	}
}

func (r *DatabaseClusterBackupReconciler) tryDeleteDBBackup(ctx context.Context, obj client.Object) {
	logger := log.FromContext(ctx)
	dbb := &everestv1alpha1.DatabaseClusterBackup{}
	namespacedName := client.ObjectKeyFromObject(obj)

	if err := r.Get(ctx, namespacedName, dbb); err != nil {
		if k8serrors.IsNotFound(err) {
			return
		}
		logger.Error(err, "Failed to get the DatabaseClusterBackup", "name", obj.GetName())
		return
	}

	if err := r.Delete(ctx, dbb); err != nil {
		if k8serrors.IsNotFound(err) {
			return
		}
		logger.Error(err, "Failed to delete the DatabaseClusterBackup", "name", obj.GetName())
	}
}

func (r *DatabaseClusterBackupReconciler) tryCreateDBBackups(
	ctx context.Context,
	obj client.Object,
	createBackupFunc func(ctx context.Context, obj client.Object) error,
) {
	logger := log.FromContext(ctx)
	if len(obj.GetOwnerReferences()) == 0 {
		err := createBackupFunc(ctx, obj)
		if err != nil {
			logger.Error(err, "Failed to create DatabaseClusterBackup "+obj.GetName())
		}
	}
}

func (r *DatabaseClusterBackupReconciler) tryCreatePG(ctx context.Context, obj client.Object) error {
	namespacedName := client.ObjectKeyFromObject(obj)
	pgBackup := &pgv2.PerconaPGBackup{}
	if err := r.Get(ctx, namespacedName, pgBackup); err != nil {
		// if such upstream backup is not found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	pg := &pgv2.PerconaPGCluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: pgBackup.Spec.PGCluster}, pg); err != nil {
		// if such upstream cluster is not found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// We want to ignore backups that are done to the hardcoded PVC-based repo1.
	// This repo only exists to allow users to spin up a PG cluster without specifying a backup storage.
	// Therefore, we don't want to allow users to restore from these backups so shouldn't create a DBB CR from repo1.
	if pgBackup.Spec.RepoName == "repo1" && len(pg.Spec.Backups.PGBackRest.Repos) > 0 && pg.Spec.Backups.PGBackRest.Repos[0].Volume != nil {
		return nil
	}

	storages := &everestv1alpha1.BackupStorageList{}
	if err := r.List(ctx, storages, &client.ListOptions{}); err != nil {
		// if no backup storages found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	backup := &everestv1alpha1.DatabaseClusterBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
		},
	}
	err := r.Get(ctx, namespacedName, backup)
	// if such everest backup already exists - do nothing
	if err == nil {
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	backup.Spec.DBClusterName = pgBackup.Spec.PGCluster

	cluster := &everestv1alpha1.DatabaseCluster{}
	err = r.Get(ctx, types.NamespacedName{Name: pgBackup.Spec.PGCluster, Namespace: pgBackup.Namespace}, cluster)
	if err != nil {
		return err
	}
	name, nErr := backupStorageName(pgBackup.Spec.RepoName, pg, storages)
	if nErr != nil {
		return nErr
	}

	backup.Spec.BackupStorageName = name
	backup.ObjectMeta.Labels = map[string]string{
		consts.DatabaseClusterNameLabel: pgBackup.Spec.PGCluster,
	}
	if err = controllerutil.SetControllerReference(cluster, backup, r.Scheme); err != nil {
		return err
	}

	return r.Create(ctx, backup)
}

func (r *DatabaseClusterBackupReconciler) tryCreatePXC(ctx context.Context, obj client.Object) error {
	namespacedName := client.ObjectKeyFromObject(obj)
	pxcBackup := &pxcv1.PerconaXtraDBClusterBackup{}
	if err := r.Get(ctx, namespacedName, pxcBackup); err != nil {
		// if such upstream backup is not found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	backup := &everestv1alpha1.DatabaseClusterBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
		},
	}
	err := r.Get(ctx, namespacedName, backup)
	// if such everest backup already exists - do nothing
	if err == nil {
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}

	backup.Spec.DBClusterName = pxcBackup.Spec.PXCCluster
	backup.Spec.BackupStorageName = pxcBackup.Spec.StorageName

	backup.ObjectMeta.Labels = map[string]string{
		consts.DatabaseClusterNameLabel: pxcBackup.Spec.PXCCluster,
	}
	cluster := &everestv1alpha1.DatabaseCluster{}
	err = r.Get(ctx, types.NamespacedName{Name: pxcBackup.Spec.PXCCluster, Namespace: pxcBackup.Namespace}, cluster)
	if err != nil {
		return err
	}
	if err = controllerutil.SetControllerReference(cluster, backup, r.Scheme); err != nil {
		return err
	}

	return r.Create(ctx, backup)
}

func (r *DatabaseClusterBackupReconciler) tryCreatePSMDB(ctx context.Context, obj client.Object) error {
	psmdbBackup := &psmdbv1.PerconaServerMongoDBBackup{}
	namespacedName := client.ObjectKeyFromObject(obj)
	if err := r.Get(ctx, namespacedName, psmdbBackup); err != nil {
		// if such upstream backup is not found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	backup := &everestv1alpha1.DatabaseClusterBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
		},
	}
	err := r.Get(ctx, namespacedName, backup)
	// if such everest backup already exists - do nothing
	if err == nil {
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}

	backup.Spec.DBClusterName = psmdbBackup.Spec.ClusterName
	backup.Spec.BackupStorageName = psmdbBackup.Spec.StorageName

	backup.ObjectMeta.Labels = map[string]string{
		consts.DatabaseClusterNameLabel: psmdbBackup.Spec.ClusterName,
	}
	cluster := &everestv1alpha1.DatabaseCluster{}
	err = r.Get(ctx, types.NamespacedName{Name: psmdbBackup.Spec.ClusterName, Namespace: psmdbBackup.Namespace}, cluster)
	if err != nil {
		return err
	}
	if err = controllerutil.SetControllerReference(cluster, backup, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, backup)
}

// Reconcile PXC.
// Returns: (requeue(bool), error).
func (r *DatabaseClusterBackupReconciler) reconcilePXC(
	ctx context.Context,
	backup *everestv1alpha1.DatabaseClusterBackup,
) (bool, error) {
	logger := log.FromContext(ctx)
	namespacedName := client.ObjectKeyFromObject(backup)
	pxcCR := &pxcv1.PerconaXtraDBClusterBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
		},
	}
	if err := r.Get(ctx, namespacedName, pxcCR); client.IgnoreNotFound(err) != nil {
		return false, err
	}

	// Handle cleanup.
	if !backup.GetDeletionTimestamp().IsZero() {
		if err := r.handleStorageProtectionFinalizer(ctx, backup, pxcCR, consts.DeletePXCBackupFinalizer); err != nil {
			return false, err
		}
		return true, nil
	}

	pxcDBCR := &pxcv1.PerconaXtraDBCluster{}
	err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.DBClusterName, Namespace: namespacedName.Namespace}, pxcDBCR)
	if err != nil {
		return false, err
	}

	// If the backup storage is not defined in the PerconaXtraDBCluster CR, we
	// cannot proceed
	if _, ok := pointer.Get(pxcDBCR.Spec.Backup).Storages[backup.Spec.BackupStorageName]; !ok {
		// The DatabaseCluster controller is responsible for updating the
		// upstream DB cluster with the necessary storage definition. If
		// the storage is not defined in the upstream DB cluster CR, we
		// requeue the backup to give the DatabaseCluster controller a
		// chance to update the upstream DB cluster CR.
		logger.Info(
			fmt.Sprintf("BackupStorage='%s' is not defined in PerconaXtraDBCluster='%s', requeuing",
				backup.Spec.BackupStorageName,
				client.ObjectKeyFromObject(pxcDBCR)),
		)
		return true, nil
	}

	if !backup.HasCompleted() {
		if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, pxcCR, func() error {
			pxcCR.Spec.PXCCluster = backup.Spec.DBClusterName
			pxcCR.Spec.StorageName = backup.Spec.BackupStorageName
			// replace legacy finalizer.
			controllerutil.RemoveFinalizer(pxcCR, "delete-s3-backup")
			controllerutil.AddFinalizer(pxcCR, consts.DeletePXCBackupFinalizer)
			return controllerutil.SetControllerReference(backup, pxcCR, r.Client.Scheme())
		}); err != nil {
			return false, err
		}
	}

	return false, nil
}

// Reconcile PSMDB.
// Returns: (requeue(bool), error).
func (r *DatabaseClusterBackupReconciler) reconcilePSMDB(
	ctx context.Context,
	backup *everestv1alpha1.DatabaseClusterBackup,
) (bool, error) {
	namespacedName := client.ObjectKeyFromObject(backup)
	psmdbCR := &psmdbv1.PerconaServerMongoDBBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
		},
	}
	if err := r.Get(ctx, namespacedName, psmdbCR); client.IgnoreNotFound(err) != nil {
		return false, err
	}

	psmdbCluster := &psmdbv1.PerconaServerMongoDB{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.DBClusterName,
		Namespace: namespacedName.Namespace,
	}, psmdbCluster)
	if client.IgnoreNotFound(err) != nil {
		return false, err
	}

	logger := log.FromContext(ctx)

	// the .Status.BackupConfigHash field appeared in psmdb 1.20.0 so
	// we need to check the CR version when we check if the field is populated
	versionCheck, err := isCRVersionGreaterOrEqual(psmdbCluster.Spec.CRVersion, "1.20.0")
	if err != nil {
		logger.Error(err, fmt.Sprintf("failed to compare CR version for PerconaServerMongoDB='%s'", client.ObjectKeyFromObject(psmdbCluster)))
		return false, err
	}
	// Requeue if the pbm is not configured yet in psmdb 1.20.0+.
	// An indicator that the pbm is configured in 1.20.0+ is the non-empty psmdb.Status.BackupConfigHash
	// NOTE: in the future, to add support for multiple storages we would need not only to check the BackupConfigHash but also understand if
	// it has changed to be able to create on-demand backups to a new storage.
	if psmdbCluster.Status.BackupConfigHash == "" && versionCheck {
		logger.Info(
			fmt.Sprintf("Backup configuration is not ready yet for PerconaServerMongoDB='%s', requeuing",
				client.ObjectKeyFromObject(psmdbCluster)))
		return true, nil
	}

	// If the psmdb-backup object exists, we will wait for it to progress beyond the waiting state.
	// This is a known limitation in PSMSD operator, where updating the object while it is in the waiting
	// state results in a duplicate backup being created.
	// See: https://perconadev.atlassian.net/browse/K8SPSMDB-1088
	if !pointer.To(psmdbCR.GetCreationTimestamp()).IsZero() &&
		(psmdbCR.Status.State == "" || psmdbCR.Status.State == psmdbv1.BackupStateWaiting) {
		return true, nil
	}

	// Handle cleanup.
	if !backup.GetDeletionTimestamp().IsZero() {
		if err = r.handleStorageProtectionFinalizer(ctx, backup, psmdbCR, consts.DeletePSMDBBackupFinalizer); err != nil {
			return false, err
		}
		return true, nil
	}

	// If the backup storage is not defined in the PerconaServerMongoDB CR, we
	// cannot proceed
	if _, ok := psmdbCluster.Spec.Backup.Storages[backup.Spec.BackupStorageName]; !ok {
		// The DatabaseCluster controller is responsible for updating the
		// upstream DB cluster with the necessary storage definition. If
		// the storage is not defined in the upstream DB cluster CR, we
		// requeue the backup to give the DatabaseCluster controller a
		// chance to update the upstream DB cluster CR.
		logger.Info(
			fmt.Sprintf("BackupStorage='%s' is not defined in PerconaServerMongoDB='%s', requeuing",
				backup.Spec.BackupStorageName,
				client.ObjectKeyFromObject(psmdbCluster)),
		)
		return true, nil
	}

	if !backup.HasCompleted() {
		if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, psmdbCR, func() error {
			psmdbCR.Spec.ClusterName = backup.Spec.DBClusterName
			psmdbCR.Spec.StorageName = backup.Spec.BackupStorageName

			// replace legacy finalizer.
			controllerutil.RemoveFinalizer(psmdbCR, "delete-backup")
			controllerutil.AddFinalizer(psmdbCR, consts.DeletePSMDBBackupFinalizer)
			return controllerutil.SetControllerReference(backup, psmdbCR, r.Client.Scheme())
		}); err != nil {
			return false, err
		}
	}

	return false, nil
}

func isCRVersionGreaterOrEqual(currentVersionStr, desiredVersionStr string) (bool, error) {
	crVersion, err := goversion.NewVersion(currentVersionStr)
	if err != nil {
		return false, err
	}
	desiredVersion, err := goversion.NewVersion(desiredVersionStr)
	if err != nil {
		return false, err
	}
	return crVersion.GreaterThanOrEqual(desiredVersion), nil
}

// Build the backup destination path.
func getLastPGBackupDestination(
	backupStorage *everestv1alpha1.BackupStorage,
	db *everestv1alpha1.DatabaseCluster,
	pgBackup *pgv2.PerconaPGBackup,
) *string {
	return ptr.To(fmt.Sprintf("s3://%s/%s/backup/db/%s", backupStorage.Spec.Bucket, common.BackupStoragePrefix(db), pgBackup.Status.BackupName))
}

// Reconcile PG.
// Returns: (requeue(bool), error.
func (r *DatabaseClusterBackupReconciler) reconcilePG(
	ctx context.Context,
	backup *everestv1alpha1.DatabaseClusterBackup,
) (bool, error) {
	logger := log.FromContext(ctx)
	namespacedName := client.ObjectKeyFromObject(backup)
	pgCR := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
		},
	}
	if err := r.Get(ctx, namespacedName, pgCR); client.IgnoreNotFound(err) != nil {
		return false, err
	}

	pgDBCR := &pgv2.PerconaPGCluster{}
	if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.DBClusterName, Namespace: namespacedName.Namespace}, pgDBCR); err != nil {
		return false, err
	}

	if !backup.GetDeletionTimestamp().IsZero() {
		// We can't handle this finalizer in PG yet, so we will simply remove it (if present).
		// See: https://perconadev.atlassian.net/browse/K8SPG-538
		if controllerutil.RemoveFinalizer(backup, everestv1alpha1.DBBackupStorageProtectionFinalizer) {
			return true, r.Update(ctx, backup)
		}
		if err := r.tryFinalizePGBackupJob(ctx, backup); err != nil {
			return false, fmt.Errorf("failed to finalize backup job: %w", err)
		}
		return true, nil
	}

	backupStorage := &everestv1alpha1.BackupStorage{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.BackupStorageName,
		Namespace: namespacedName.Namespace,
	}, backupStorage)
	if err != nil {
		return false, errors.Join(err, fmt.Errorf("failed to get backup storage %s", backup.Spec.BackupStorageName))
	}

	// If the backup storage is not defined in the PerconaPGCluster CR, we
	// cannot proceed
	repoName := common.GetRepoNameByBackupStorage(backupStorage, pgDBCR.Spec.Backups.PGBackRest.Repos)
	if repoName == "" {
		// The DatabaseCluster controller is responsible for updating the
		// upstream DB cluster with the necessary storage definition. If
		// the storage is not defined in the upstream DB cluster CR, we
		// requeue the backup to give the DatabaseCluster controller a
		// chance to update the upstream DB cluster CR.
		logger.Info(
			fmt.Sprintf("BackupStorage='%s' is not defined in PerconaPGCluster='%s', requeuing",
				backup.Spec.BackupStorageName,
				client.ObjectKeyFromObject(pgDBCR)),
		)
		return true, nil
	}

	if !backup.HasCompleted() {
		_, err = controllerutil.CreateOrUpdate(ctx, r.Client, pgCR, func() error {
			pgCR.Spec.PGCluster = backup.Spec.DBClusterName
			pgCR.Spec.RepoName = repoName
			pgCR.Spec.Options = []string{
				"--type=full",
			}
			return controllerutil.SetControllerReference(backup, pgCR, r.Client.Scheme())
		})
		if err != nil {
			return false, err
		}
	}

	return false, nil
}

func backupStorageName(repoName string, pg *pgv2.PerconaPGCluster, storages *everestv1alpha1.BackupStorageList) (string, error) {
	for _, repo := range pg.Spec.Backups.PGBackRest.Repos {
		if repo.Name == repoName {
			for _, storage := range storages.Items {
				if pg.Namespace == storage.Namespace &&
					repo.S3.Region == storage.Spec.Region &&
					repo.S3.Bucket == storage.Spec.Bucket &&
					repo.S3.Endpoint == storage.Spec.EndpointURL {
					return storage.Name, nil
				}
			}
		}
	}
	return "", fmt.Errorf("failed to find backup storage for repo %s", repoName)
}

func (r *DatabaseClusterBackupReconciler) handleStorageProtectionFinalizer(
	ctx context.Context,
	dbcBackup *everestv1alpha1.DatabaseClusterBackup,
	upstreamBackup client.Object,
	storageFinalizer string,
) error {
	if !controllerutil.ContainsFinalizer(dbcBackup, everestv1alpha1.DBBackupStorageProtectionFinalizer) {
		return nil
	}
	// Ensure that S3 finalizer is removed from the upstream backup.
	if controllerutil.RemoveFinalizer(upstreamBackup, storageFinalizer) {
		return r.Update(ctx, upstreamBackup)
	}
	// Finalizer is gone from upstream object, remove from DatabaseClusterBackup.
	if controllerutil.RemoveFinalizer(dbcBackup, everestv1alpha1.DBBackupStorageProtectionFinalizer) {
		return r.Update(ctx, dbcBackup)
	}
	return nil
}

const (
	crunchyClusterLabel       = "postgres-operator.crunchydata.com/cluster"
	crunchyBackupAnnotation   = "postgres-operator.crunchydata.com/pgbackrest-backup"
	perconaPGJobKeepFinalizer = "internal.percona.com/keep-job"
)

// K8SPG-703 introduces a finalizer on the job that prevents it from being deleted when the backup is running.
// This finalizer is intended to prevent backup jobs from being deleted while the backup is running in order
// to prevent a race condition when the Job has a `ttlSecondsAfterFinished` set.
// But since we do not set `ttlSecondsAfterFinished` for backup jobs, we can remove the finalizer to unblock deletion
// of running backups.
// See: https://perconadev.atlassian.net/browse/K8SPG-703
func (r *DatabaseClusterBackupReconciler) tryFinalizePGBackupJob(
	ctx context.Context,
	dbb *everestv1alpha1.DatabaseClusterBackup,
) error {
	jobList := &batchv1.JobList{}

	// List backup jobs for the specified cluster.
	// Use APIReader to avoid starting an informer/cache for jobs.
	if err := r.APIReader.List(ctx, jobList, client.InNamespace(dbb.GetNamespace()), client.MatchingLabels{
		crunchyClusterLabel: dbb.Spec.DBClusterName,
	}); err != nil {
		return err
	}

	// Find the job for the specified backup.
	for _, job := range jobList.Items {
		if job.GetAnnotations()[crunchyBackupAnnotation] == dbb.GetName() &&
			controllerutil.RemoveFinalizer(&job, perconaPGJobKeepFinalizer) {
			return r.Client.Update(ctx, &job)
		}
	}
	return nil
}
