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

	"github.com/AlekSi/pointer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

// BackupStorageReconciler reconciles a BackupStorage object.
type BackupStorageReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=everest.percona.com,resources=backupstorages,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=everest.percona.com,resources=backupstorages/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=everest.percona.com,resources=backupstorages/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;watch;list

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// Modify the Reconcile function to compare the state specified by
// the DatabaseClusterBackup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *BackupStorageReconciler) Reconcile(ctx context.Context, req ctrl.Request) (rr ctrl.Result, rerr error) { //nolint:nonamedreturns
	logger := log.FromContext(ctx)
	logger.Info("Reconciling")
	defer func() {
		logger.Info("Reconciled")
	}()

	bs := &everestv1alpha1.BackupStorage{}
	err := r.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, bs)
	if err != nil {
		// NotFound cannot be fixed by requeuing so ignore it. During background
		// deletion, we receive delete events from cluster's dependents after
		// cluster is deleted.
		if err = client.IgnoreNotFound(err); err != nil {
			logger.Error(err, "unable to fetch BackupStorage")
		}
		return reconcile.Result{}, err
	}

	bsSecret := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      bs.Spec.CredentialsSecretName,
		Namespace: req.Namespace,
	},
		bsSecret)
	if err != nil {
		if err = client.IgnoreNotFound(err); err != nil {
			logger.Error(err, "unable to fetch Secret", "secretName", bs.Spec.CredentialsSecretName)
		}
		return ctrl.Result{}, err
	}

	// If the default secret is not owned/controlled by anyone, we will adopt it.
	if controller := metav1.GetControllerOf(bsSecret); controller == nil {
		logger.Info("setting controller reference for the secret")
		if err := controllerutil.SetControllerReference(bs, bsSecret, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Update(ctx, bsSecret); err != nil {
			return ctrl.Result{}, err
		}
	}

	bsName := bs.GetName()
	bsUsed, err := r.isBackupStorageUsed(ctx, bs)
	if err != nil {
		msg := fmt.Sprintf("failed to check if backup storage='%s' is used", bs.GetName())
		logger.Error(err, msg)
		return ctrl.Result{}, fmt.Errorf("%s: %w", msg, err)
	}
	// Update the status of the BackupStorage object after the reconciliation.
	defer func() {
		// Nothing to process on delete events
		if !bs.GetDeletionTimestamp().IsZero() {
			return
		}

		bs.Status.InUse = bsUsed
		bs.Status.LastObservedGeneration = bs.GetGeneration()
		if err = r.Client.Status().Update(ctx, bs); err != nil {
			rr = ctrl.Result{}
			msg := fmt.Sprintf("failed to update status for backup storage='%s'", bsName)
			logger.Error(err, msg)
			rerr = fmt.Errorf("%s: %w", msg, err)
		}
	}()

	if err = common.EnsureInUseFinalizer(ctx, r.Client, bsUsed, bs); err != nil {
		logger.Error(err, fmt.Sprintf("failed to update finalizers for backup storage='%s'", bsName))
		return ctrl.Result{}, errors.Join(err, fmt.Errorf("failed to update finalizers for backup storage='%s': %w", bsName, err))
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BackupStorageReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.initIndexers(context.Background(), mgr); err != nil {
		return err
	}

	// Predicate to filter events of DatabaseClusterRestore resource.
	dbClusterRestoreEventsPredicate := predicate.Funcs{
		// Allow CREATE events.
		CreateFunc: func(_ event.CreateEvent) bool {
			return true
		},

		// Only allow UPDATE events when the .status.state field has changed.
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldDbr, oldOk := e.ObjectOld.(*everestv1alpha1.DatabaseClusterRestore)
			newDbr, newOk := e.ObjectNew.(*everestv1alpha1.DatabaseClusterRestore)
			if !oldOk || !newOk {
				return false
			}
			return oldDbr.Status.State != newDbr.Status.State
		},

		// Nothing to process on DELETE events.
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return false
		},

		// Nothing to process on GENERIC events.
		GenericFunc: func(_ event.GenericEvent) bool {
			return false
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("BackupStorage").
		For(&everestv1alpha1.BackupStorage{}).
		Owns(&corev1.Secret{}).
		Watches(
			&corev1.Namespace{},
			common.EnqueueObjectsInNamespace(r.Client, &everestv1alpha1.BackupStorageList{}),
		).
		// need to watch DatabaseClusterBackup that reference BackupStorage to update the storage status.
		Watches(
			&everestv1alpha1.DatabaseClusterBackup{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				dbb, ok := obj.(*everestv1alpha1.DatabaseClusterBackup)
				if !ok || dbb.Spec.BackupStorageName == "" {
					return []reconcile.Request{}
				}

				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      dbb.Spec.BackupStorageName,
							Namespace: dbb.GetNamespace(),
						},
					},
				}
			}),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		// need to watch DatabaseCluster that reference BackupStorage to update the storage status.
		Watches(
			&everestv1alpha1.DatabaseCluster{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				db, ok := obj.(*everestv1alpha1.DatabaseCluster)
				if !ok {
					return []reconcile.Request{}
				}

				// use map to avoid duplicates of BackupStorage name+namespace pairs
				bsToReconcileMap := make(map[types.NamespacedName]struct{})

				// get BackupStorage from scheduled backups
				for _, bsName := range db.Spec.Backup.Schedules {
					if bsName.BackupStorageName == "" {
						// No BackupStorageName specified, no need to enqueue
						continue
					}
					bsToReconcileMap[types.NamespacedName{
						Name:      bsName.BackupStorageName,
						Namespace: db.GetNamespace(),
					}] = struct{}{}
				}

				// get BackupStorage from PITR backups
				if db.Spec.Backup.PITR.Enabled &&
					pointer.Get(db.Spec.Backup.PITR.BackupStorageName) != "" {
					bsToReconcileMap[types.NamespacedName{
						Name:      pointer.Get(db.Spec.Backup.PITR.BackupStorageName),
						Namespace: db.GetNamespace(),
					}] = struct{}{}
				}

				// get BackupStorage from spec.dataSource.backupSource
				if pointer.Get(pointer.Get(db.Spec.DataSource).BackupSource).BackupStorageName != "" {
					bsToReconcileMap[types.NamespacedName{
						Name:      pointer.Get(pointer.Get(db.Spec.DataSource).BackupSource).BackupStorageName,
						Namespace: db.GetNamespace(),
					}] = struct{}{}
				}

				requests := make([]reconcile.Request, 0, len(bsToReconcileMap))
				for bs := range bsToReconcileMap {
					requests = append(requests, reconcile.Request{NamespacedName: bs})
				}

				return requests
			}),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		// need to watch DatabaseClusterRestore that reference BackupStorage to update the storage status.
		Watches(
			&everestv1alpha1.DatabaseClusterRestore{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				dbr, ok := obj.(*everestv1alpha1.DatabaseClusterRestore)
				if !ok || pointer.Get(dbr.Spec.DataSource.BackupSource).BackupStorageName == "" {
					return []reconcile.Request{}
				}

				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      dbr.Spec.DataSource.BackupSource.BackupStorageName,
							Namespace: dbr.GetNamespace(),
						},
					},
				}
			}),
			builder.WithPredicates(dbClusterRestoreEventsPredicate),
		).
		WithEventFilter(common.DefaultNamespaceFilter).
		Complete(r)
}

func (r *BackupStorageReconciler) initIndexers(ctx context.Context, mgr ctrl.Manager) error {
	// Index the BackupStorage's CredentialsSecretName field so that it can be
	// used by the databaseClustersThatReferenceSecret function to
	// find all DatabaseClusters that reference a specific secret through the
	// BackupStorage's CredentialsSecretName field
	err := mgr.GetFieldIndexer().IndexField(
		ctx, &everestv1alpha1.BackupStorage{}, credentialsSecretNameField,
		func(o client.Object) []string {
			var res []string
			backupStorage, ok := o.(*everestv1alpha1.BackupStorage)
			if !ok {
				return res
			}
			return append(res, backupStorage.Spec.CredentialsSecretName)
		},
	)

	return err
}

func (r *BackupStorageReconciler) isBackupStorageUsed(ctx context.Context, bs *everestv1alpha1.BackupStorage) (bool, error) {
	// Check if the backup storage is used by any database cluster backups through the DBClusterBackupBackupStorageNameField field
	if dbbList, err := common.DatabaseClusterBackupsThatReferenceObject(ctx, r.Client, consts.DBClusterBackupBackupStorageNameField,
		bs.GetNamespace(), bs.GetName()); err != nil {
		return false, fmt.Errorf("failed to fetch DB cluster backups that use backup storage='%s' through '%s' field: %w",
			bs.GetName(), consts.DBClusterBackupBackupStorageNameField, err)
	} else if len(dbbList.Items) > 0 {
		return true, nil
	}

	// Check if the backup storage is used by any database clusters through the BackupStorageName field
	if dbList, err := common.DatabaseClustersThatReferenceObject(ctx, r.Client, backupStorageNameField,
		bs.GetNamespace(), bs.GetName()); err != nil {
		return false, fmt.Errorf("failed to fetch DB clusters that use backup storage='%s' through '%s' field: %w",
			bs.GetName(), backupStorageNameField, err)
	} else if len(dbList.Items) > 0 {
		return true, nil
	}

	// Check if the backup storage is used by any database clusters through the PITRBackupStorageName field
	if dbList, err := common.DatabaseClustersThatReferenceObject(ctx, r.Client, pitrBackupStorageNameField,
		bs.GetNamespace(), bs.GetName()); err != nil {
		return false, fmt.Errorf("failed to fetch DB clusters that use backup storage='%s' through '%s' field: %w",
			bs.GetName(), pitrBackupStorageNameField, err)
	} else if len(dbList.Items) > 0 {
		return true, nil
	}

	// Check if the backup storage is used by any database clusters through the DBClusterDataSourceBackupStorageNameField field
	if dbList, err := common.DatabaseClustersThatReferenceObject(ctx, r.Client, consts.DataSourceBackupStorageNameField,
		bs.GetNamespace(), bs.GetName()); err != nil {
		return false, fmt.Errorf("failed to fetch DB clusters that use backup storage='%s' through '%s' field: %w",
			bs.GetName(), consts.DataSourceBackupStorageNameField, err)
	} else if len(dbList.Items) > 0 {
		return true, nil
	}

	// Check if the backup storage is used by any database cluster restore through the dbClusterRestoreDataSourceBackupStorageNameField field
	if dbrList, err := common.DatabaseClusterRestoresThatReferenceObject(ctx, r.Client, consts.DataSourceBackupStorageNameField,
		bs.GetNamespace(), bs.GetName()); err != nil {
		return false, fmt.Errorf("failed to fetch DB cluster restores that use backup storage='%s' through '%s' field: %w",
			bs.GetName(), consts.DataSourceBackupStorageNameField, err)
	} else if len(dbrList.Items) > 0 {
		return slices.ContainsFunc(dbrList.Items, func(dbr everestv1alpha1.DatabaseClusterRestore) bool {
			// If any of the restores is in progress, we consider the backup storage as used.
			return dbr.IsInProgress()
		}), nil
	}

	return false, nil
}
