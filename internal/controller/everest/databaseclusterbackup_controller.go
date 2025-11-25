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
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	goversion "github.com/hashicorp/go-version"
	pgv2 "github.com/percona/percona-postgresql-operator/pkg/apis/pgv2.percona.com/v2"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	batchv1 "k8s.io/api/batch/v1"
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
	backupRetryAfter = 1 * time.Minute
	databaseClusterBackupKind = "DatabaseClusterBackup"
	everestAPIVersion         = "everest.percona.com/v1alpha1"

	pxcBackupKind = "PerconaXtraDBClusterBackup"
	pxcAPIVersion = "pxc.percona.com/v1"

	psmdbBackupKind = "PerconaServerMongoDBBackup"
	psmdbAPIVersion = "psmdb.percona.com/v1"

	pgBackupKind = "PerconaPGBackup"
	pgAPIVersion = "pgv2.percona.com/v2"

	pxcGapsReasonString = "BinlogGapDetected"

	deletePXCBackupFinalizer   = "percona.com/delete-backup"
	deletePSMDBBackupFinalizer = "percona.com/delete-backup"
)

// ErrBackupStorageUndefined is returned when a backup storage is not defined
// in the corresponding upstream DB cluster CR.
var ErrBackupStorageUndefined = errors.New("backup storage is not defined in the upstream DB cluster CR")

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

	backup := &everestv1alpha1.DatabaseClusterBackup{}

	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		logger.Error(err, fmt.Sprintf("unable to fetch DatabaseClusterBackup='%s'", req.NamespacedName))
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// DBBackups are always deleted in foreground.
	if backup.GetDeletionTimestamp().IsZero() &&
		controllerutil.AddFinalizer(backup, consts.ForegroundDeletionFinalizer) {
		if err := r.Update(ctx, backup); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil // Requeue so that we get an updated copy.
	}

	cluster := &everestv1alpha1.DatabaseCluster{}
	err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.DBClusterName, Namespace: backup.Namespace}, cluster)
	if err != nil {
		if err = client.IgnoreNotFound(err); err != nil {
			logger.Error(err, fmt.Sprintf("unable to fetch DatabaseCluster='%s'",
				types.NamespacedName{Name: backup.Spec.DBClusterName, Namespace: backup.Namespace}))
		}
		return ctrl.Result{}, err
	}

	if err := r.reconcileMeta(ctx, backup, cluster); err != nil {
		return ctrl.Result{}, err
	}

	storage := &everestv1alpha1.BackupStorage{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.BackupStorageName,
		Namespace: backup.GetNamespace(),
	}, storage)
	if err != nil {
		if err = client.IgnoreNotFound(err); err != nil {
			logger.Error(err, fmt.Sprintf("unable to fetch BackupStorage='%s'", types.NamespacedName{
				Name:      backup.Spec.BackupStorageName,
				Namespace: backup.GetNamespace(),
			}))
		}
		return ctrl.Result{}, err
	}

	// By default, we must always verify TLS.
	// TODO: move to defaulter webhook.
	if verifyTLS := storage.Spec.VerifyTLS; verifyTLS == nil {
		storage.Spec.VerifyTLS = pointer.To(true)
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

	// The DatabaseCluster controller is responsible for updating the
	// upstream DB cluster with the necessary storage definition. If the
	// storage is not defined in the upstream DB cluster CR, we requeue the
	// backup to give the DatabaseCluster controller a chance to update the
	// upstream DB cluster CR.
	if errors.Is(err, ErrBackupStorageUndefined) {
		logger.Info(
			fmt.Sprintf("BackupStorage='%s' is not defined in the %s DatabaseCluster='%s', requeuing",
				backup.Spec.BackupStorageName,
				cluster.Spec.Engine.Type,
				backup.Spec.DBClusterName),
		)
		return ctrl.Result{RequeueAfter: backupRetryAfter}, nil
	}

	if err != nil {
		logger.Error(err, fmt.Sprintf("failed to reconcile backup for %s DatabaseCluster='%s'",
			cluster.Spec.Engine.Type,
			backup.Spec.DBClusterName))
		return ctrl.Result{}, err
	}

	if requeue {
		return ctrl.Result{RequeueAfter: backupRetryAfter}, nil
	} else {
		return ctrl.Result{}, nil
	}
}

// ReconcileWatchers reconciles the watchers for the DatabaseClusterBackup controller.
func (r *DatabaseClusterBackupReconciler) ReconcileWatchers(ctx context.Context) error {
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
			log.Info("Unknown database engine type", "type", dbEngine.Spec.Type)
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

	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		Named("DatabaseClusterBackup").
		For(&everestv1alpha1.DatabaseClusterBackup{})
	ctrlBuilder.Watches(
		&corev1.Namespace{},
		common.EnqueueObjectsInNamespace(r.Client, &everestv1alpha1.DatabaseClusterBackupList{}),
	)
	ctrlBuilder.WithEventFilter(common.DefaultNamespaceFilter)

	// Normally we would call `Complete()`, however, with `Build()`, we get a handle to the underlying controller,
	// so that we can dynamically add watchers from the DatabaseEngine reconciler.
	ctrl, err := ctrlBuilder.Build(r)
	if err != nil {
		return err
	}
	log := mgr.GetLogger().WithName("DynamicWatcher").WithValues("controller", "DatabaseClusterBackup")
	r.controller = newControllerWatcherRegistry(log, ctrl)
	return nil
}

func (r *DatabaseClusterBackupReconciler) reconcileMeta(
	ctx context.Context,
	backup *everestv1alpha1.DatabaseClusterBackup,
	cluster *everestv1alpha1.DatabaseCluster,
) error {
	var needUpdate bool

	if len(backup.Labels) == 0 {
		backup.Labels = map[string]string{
			consts.DatabaseClusterNameLabel: backup.Spec.DBClusterName,
		}
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
			q.Add(reconcileRequestFromObject(e.Object))
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			// remove the r.tryCreateDBBackups call below once https://perconadev.atlassian.net/browse/K8SPSMDB-1088 is fixed.
			r.tryCreateDBBackups(ctx, e.ObjectNew, creationFunc)

			q.Add(reconcileRequestFromObject(e.ObjectNew))
		},
		DeleteFunc: func(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			r.tryDeleteDBBackup(ctx, e.Object)
			q.Add(reconcileRequestFromObject(e.Object))
		},
	}
}

func reconcileRequestFromObject(obj client.Object) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      obj.GetName(),
			Namespace: obj.GetNamespace(),
		},
	}
}

func (r *DatabaseClusterBackupReconciler) tryDeleteDBBackup(ctx context.Context, obj client.Object) {
	logger := log.FromContext(ctx)
	dbb := &everestv1alpha1.DatabaseClusterBackup{}
	namespacedName := types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}

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
	pgBackup := &pgv2.PerconaPGBackup{}
	namespacedName := types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}

	if err := r.Get(ctx, namespacedName, pgBackup); err != nil {
		// if such upstream backup is not found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// We want to ignore backups that are done to the hardcoded PVC-based repo1.
	// This repo only exists to allow users to spin up a PG cluster without specifying a backup storage.
	// Therefore, we don't want to allow users to restore from these backups so shouldn't create a DBB CR from repo1.
	if pgBackup.Spec.RepoName == "repo1" {
		return nil
	}

	pg := &pgv2.PerconaPGCluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: pgBackup.Spec.PGCluster}, pg); err != nil {
		// if such upstream cluster is not found - do nothing
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
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
	pxcBackup := &pxcv1.PerconaXtraDBClusterBackup{}
	namespacedName := types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}

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
	namespacedName := types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}

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
		TypeMeta: metav1.TypeMeta{
			Kind:       databaseClusterBackupKind,
			APIVersion: everestAPIVersion,
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
	pxcCR := &pxcv1.PerconaXtraDBClusterBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backup.Name,
			Namespace: backup.Namespace,
		},
	}
	if err := r.Get(ctx,
		types.NamespacedName{
			Name:      backup.Name,
			Namespace: backup.Namespace,
		},
		pxcCR); client.IgnoreNotFound(err) != nil {
		return false, err
	}

	pxcDBCR := &pxcv1.PerconaXtraDBCluster{}
	err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.DBClusterName, Namespace: backup.Namespace}, pxcDBCR)
	if err != nil {
		return false, err
	}

	// Handle cleanup.
	if !backup.GetDeletionTimestamp().IsZero() {
		if err := r.handleStorageProtectionFinalizer(ctx, backup, pxcCR, deletePXCBackupFinalizer); err != nil {
			return false, err
		}
		backup.Status.State = everestv1alpha1.BackupDeleting
		return true, r.Status().Update(ctx, backup)
	}

	// If the backup storage is not defined in the PerconaXtraDBCluster CR, we
	// cannot proceed
	if pxcDBCR.Spec.Backup.Storages == nil {
		return false, ErrBackupStorageUndefined
	}
	if _, ok := pxcDBCR.Spec.Backup.Storages[backup.Spec.BackupStorageName]; !ok {
		return false, ErrBackupStorageUndefined
	}

	if !backup.HasCompleted() {
		_, err = controllerutil.CreateOrUpdate(ctx, r.Client, pxcCR, func() error {
			pxcCR.TypeMeta = metav1.TypeMeta{
				APIVersion: pxcAPIVersion,
				Kind:       pxcBackupKind,
			}
			pxcCR.Spec.PXCCluster = backup.Spec.DBClusterName
			pxcCR.Spec.StorageName = backup.Spec.BackupStorageName
			pxcCR.SetOwnerReferences([]metav1.OwnerReference{{
				APIVersion:         everestAPIVersion,
				Kind:               databaseClusterBackupKind,
				Name:               backup.Name,
				UID:                backup.UID,
				BlockOwnerDeletion: pointer.ToBool(true),
			}})
			controllerutil.AddFinalizer(pxcCR, deletePXCBackupFinalizer)
			return nil
		})
		if err != nil {
			return false, err
		}
	}

	// replace legacy finalizer.
	if controllerutil.RemoveFinalizer(pxcCR, "delete-s3-backup") &&
		pxcCR.GetDeletionTimestamp().IsZero() {
		controllerutil.AddFinalizer(pxcCR, deletePXCBackupFinalizer)
		if err := r.Update(ctx, pxcCR); err != nil {
			return false, err
		}
	}

	backup.Status.State = everestv1alpha1.GetDBBackupState(pxcCR)
	backup.Status.CompletedAt = pxcCR.Status.CompletedAt
	backup.Status.CreatedAt = &pxcCR.CreationTimestamp
	backup.Status.Destination = pointer.To(string(pxcCR.Status.Destination))
	for _, condition := range pxcCR.Status.Conditions {
		if condition.Reason == pxcGapsReasonString {
			backup.Status.Gaps = true
		}
	}
	backup.Status.LatestRestorableTime = pxcCR.Status.LatestRestorableTime
	return false, r.Status().Update(ctx, backup)
}

// Reconcile PSMDB.
// Returns: (requeue(bool), error).
func (r *DatabaseClusterBackupReconciler) reconcilePSMDB(
	ctx context.Context,
	backup *everestv1alpha1.DatabaseClusterBackup,
) (bool, error) {
	psmdbCR := &psmdbv1.PerconaServerMongoDBBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backup.Name,
			Namespace: backup.Namespace,
		},
	}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      backup.Name,
		Namespace: backup.Namespace,
	},
		psmdbCR); client.IgnoreNotFound(err) != nil {
		return false, err
	}

	psmdbCluster := &psmdbv1.PerconaServerMongoDB{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.DBClusterName,
		Namespace: backup.Namespace,
	}, psmdbCluster)
	if client.IgnoreNotFound(err) != nil {
		return false, err
	}

	// the .Status.BackupConfigHash field appeared in psmdb 1.20.0 so
	// we need to check the CR version when we check if the field is populated
	versionCheck, err := isCRVersionGreaterOrEqual(psmdbCluster.Spec.CRVersion, "1.20.0")
	if err != nil {
		return false, err
	}
	// Requeue if the pbm is not configured yet in psmdb 1.20.0+.
	// An indicator that the pbm is configured in 1.20.0+ is the non-empty psmdb.Status.BackupConfigHash
	// NOTE: in the future, to add support for multiple storages we would need not only to check the BackupConfigHash but also understand if
	// it has changed to be able to create on-demand backups to a new storage.
	if psmdbCluster.Status.BackupConfigHash == "" && versionCheck {
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

	psmdbDBCR := &psmdbv1.PerconaServerMongoDB{}
	err = r.Get(ctx, types.NamespacedName{Name: backup.Spec.DBClusterName, Namespace: backup.Namespace}, psmdbDBCR)
	if err != nil {
		return false, err
	}

	// Handle cleanup.
	if !backup.GetDeletionTimestamp().IsZero() {
		if err := r.handleStorageProtectionFinalizer(ctx, backup, psmdbCR, deletePSMDBBackupFinalizer); err != nil {
			return false, err
		}
		backup.Status.State = everestv1alpha1.BackupDeleting
		return true, r.Status().Update(ctx, backup)
	}

	// If the backup storage is not defined in the PerconaServerMongoDB CR, we
	// cannot proceed
	if psmdbDBCR.Spec.Backup.Storages == nil {
		return false, ErrBackupStorageUndefined
	}
	if _, ok := psmdbDBCR.Spec.Backup.Storages[backup.Spec.BackupStorageName]; !ok {
		return false, ErrBackupStorageUndefined
	}

	if !backup.HasCompleted() {
		_, err = controllerutil.CreateOrUpdate(ctx, r.Client, psmdbCR, func() error {
			psmdbCR.TypeMeta = metav1.TypeMeta{
				APIVersion: psmdbAPIVersion,
				Kind:       psmdbBackupKind,
			}
			psmdbCR.Spec.ClusterName = backup.Spec.DBClusterName
			psmdbCR.Spec.StorageName = backup.Spec.BackupStorageName

			psmdbCR.SetOwnerReferences([]metav1.OwnerReference{{
				APIVersion:         everestAPIVersion,
				Kind:               databaseClusterBackupKind,
				Name:               backup.Name,
				UID:                backup.UID,
				BlockOwnerDeletion: pointer.ToBool(true),
			}})
			controllerutil.AddFinalizer(psmdbCR, deletePSMDBBackupFinalizer)
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	// replace legacy finalizer.
	if controllerutil.RemoveFinalizer(psmdbCR, "delete-backup") &&
		psmdbCR.GetDeletionTimestamp().IsZero() {
		controllerutil.AddFinalizer(psmdbCR, deletePSMDBBackupFinalizer)
		if err := r.Update(ctx, psmdbCR); err != nil {
			return false, err
		}
	}

	backup.Status.State = everestv1alpha1.GetDBBackupState(psmdbCR)
	backup.Status.CompletedAt = psmdbCR.Status.CompletedAt
	backup.Status.CreatedAt = &psmdbCR.CreationTimestamp
	backup.Status.Destination = &psmdbCR.Status.Destination
	backup.Status.LatestRestorableTime = psmdbCR.Status.LatestRestorableTime
	return false, r.Status().Update(ctx, backup)
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

// Get the last performed PG backup directly from S3.
func (r *DatabaseClusterBackupReconciler) getLastPGBackupDestination(
	ctx context.Context,
	backupStorage *everestv1alpha1.BackupStorage,
	db *everestv1alpha1.DatabaseCluster,
	pgBackup *pgv2.PerconaPGBackup,
) *string {
	logger := log.FromContext(ctx)
	if pgBackup.Status.BackupName != "" {
		logger.Info("using backup name from status")
		destination := fmt.Sprintf("s3://%s/%s/backup/db/%s", backupStorage.Spec.Bucket, common.BackupStoragePrefix(db), pgBackup.Status.BackupName)
		return &destination
	}
	backupStorageSecret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: backupStorage.Spec.CredentialsSecretName, Namespace: backupStorage.Namespace}, backupStorageSecret)
	if err != nil {
		logger.Error(err, "unable to get backup storage secret")
		return nil
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(backupStorage.Spec.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			string(backupStorageSecret.Data["AWS_ACCESS_KEY_ID"]),
			string(backupStorageSecret.Data["AWS_SECRET_ACCESS_KEY"]),
			"",
		)),
	)
	if err != nil {
		logger.Error(err, "unable to load AWS configuration")
		return nil
	}
	httpClient := http.DefaultClient
	httpClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: !pointer.Get(backupStorage.Spec.VerifyTLS), //nolint:gosec
		},
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(backupStorage.Spec.EndpointURL)
		o.HTTPClient = httpClient
		o.UsePathStyle = pointer.Get(backupStorage.Spec.ForcePathStyle)
	})
	result, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(backupStorage.Spec.Bucket),
		Prefix: aws.String(common.BackupStoragePrefix(db) + "/backup/db/backup.history/"),
	})
	if err != nil {
		logger.Error(err, "unable to list objects in bucket", "bucket", backupStorage.Spec.Bucket)
		return nil
	}

	if len(result.Contents) == 0 {
		logger.Error(err, "no backup found in bucket", "bucket", backupStorage.Spec.Bucket)
		return nil
	}

	var lastBackup string
	var lastModified time.Time
	for _, content := range result.Contents {
		if lastModified.Before(*content.LastModified) {
			lastModified = *content.LastModified
			lastBackup = strings.Split(filepath.Base(*content.Key), ".")[0]
		}
	}

	destination := fmt.Sprintf("s3://%s/%s/backup/db/%s", backupStorage.Spec.Bucket, common.BackupStoragePrefix(db), lastBackup)
	return &destination
}

// Reconcile PG.
// Returns: (requeue(bool), error.
func (r *DatabaseClusterBackupReconciler) reconcilePG(
	ctx context.Context,
	backup *everestv1alpha1.DatabaseClusterBackup,
) (bool, error) {
	logger := log.FromContext(ctx)
	pgCR := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backup.Name,
			Namespace: backup.Namespace,
		},
	}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      backup.GetName(),
		Namespace: backup.GetNamespace(),
	},
		pgCR); client.IgnoreNotFound(err) != nil {
		return false, err
	}

	pgDBCR := &pgv2.PerconaPGCluster{}
	err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.DBClusterName, Namespace: backup.Namespace}, pgDBCR)
	if err != nil {
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
		backup.Status.State = everestv1alpha1.BackupDeleting
		return true, r.Status().Update(ctx, backup)
	}

	backupStorage := &everestv1alpha1.BackupStorage{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.BackupStorageName,
		Namespace: backup.GetNamespace(),
	}, backupStorage)
	if err != nil {
		return false, errors.Join(err, fmt.Errorf("failed to get backup storage %s", backup.Spec.BackupStorageName))
	}

	// If the backup storage is not defined in the PerconaPGCluster CR, we
	// cannot proceed
	repoName := common.GetRepoNameByBackupStorage(backupStorage, pgDBCR.Spec.Backups.PGBackRest.Repos)
	if repoName == "" {
		return false, ErrBackupStorageUndefined
	}

	if !backup.HasCompleted() {
		_, err = controllerutil.CreateOrUpdate(ctx, r.Client, pgCR, func() error {
			pgCR.TypeMeta = metav1.TypeMeta{
				APIVersion: pgAPIVersion,
				Kind:       pgBackupKind,
			}
			pgCR.Spec.PGCluster = backup.Spec.DBClusterName
			pgCR.SetOwnerReferences([]metav1.OwnerReference{{
				APIVersion:         everestAPIVersion,
				Kind:               databaseClusterBackupKind,
				Name:               backup.Name,
				UID:                backup.UID,
				BlockOwnerDeletion: pointer.ToBool(true),
			}})

			pgCR.Spec.RepoName = repoName
			pgCR.Spec.Options = []string{
				"--type=full",
			}
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	backup.Status.State = everestv1alpha1.GetDBBackupState(pgCR)
	backup.Status.CompletedAt = pgCR.Status.CompletedAt
	backup.Status.CreatedAt = &pgCR.CreationTimestamp
	// XXX: Until https://jira.percona.com/browse/K8SPG-411 is done
	// we work around not having the destination in the
	// PerconaPGBackup CR by getting this info directly from S3
	if backup.Status.State == everestv1alpha1.BackupState(pgv2.BackupSucceeded) && backup.Status.Destination == nil {
		db := &everestv1alpha1.DatabaseCluster{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      backup.Spec.DBClusterName,
			Namespace: backup.Namespace,
		}, db); err != nil {
			logger.Error(err, "could not get database cluster ")
		}
		if err == nil {
			backup.Status.Destination = r.getLastPGBackupDestination(ctx, backupStorage, db, pgCR)
		}
	}
	backup.Status.LatestRestorableTime = pgCR.Status.LatestRestorableTime.Time
	return false, r.Status().Update(ctx, backup)
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
