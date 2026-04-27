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

// Package pg contains the Percona PostgreSQL provider code.
package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/AlekSi/pointer"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/postgres-operator.crunchydata.com/v1beta1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

const (
	finalizerDeletePGPVC = "percona.com/delete-pvc"
	finalizerDeletePGSSL = "percona.com/delete-ssl"
)

// Provider is a provider for Percona PostgreSQL.
type Provider struct {
	*pgv2.PerconaPGCluster
	providers.ProviderOptions
	clusterType consts.ClusterType
	// currentPGSpec holds the current PXC spec.
	currentPGSpec pgv2.PerconaPGClusterSpec
}

// New returns a new provider for Percona PostgreSQL.
func New(
	ctx context.Context,
	opts providers.ProviderOptions,
) (*Provider, error) {
	client := opts.C
	pg := &pgv2.PerconaPGCluster{}
	err := client.Get(ctx, types.NamespacedName{Name: opts.DB.GetName(), Namespace: opts.DB.GetNamespace()}, pg)
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	}

	dbEngine, err := common.GetDatabaseEngine(ctx, client, consts.PGDeploymentName, opts.DB.GetNamespace())
	if err != nil {
		return nil, err
	}
	opts.DBEngine = dbEngine

	currentPGSpec := pg.Spec
	pg.Spec = defaultSpec()

	p := &Provider{
		PerconaPGCluster: pg,
		ProviderOptions:  opts,
		currentPGSpec:    currentPGSpec,
	}
	ct, err := common.GetClusterType(ctx, p.C)
	if err != nil {
		return nil, err
	}
	p.clusterType = ct
	return p, nil
}

// Apply returns the PG applier.
//
//nolint:ireturn
func (p *Provider) Apply(ctx context.Context) everestv1alpha1.Applier {
	return &applier{
		Provider: p,
		ctx:      ctx,
	}
}

// isDatabaseUpgrading returns true if the database is upgrading.
func (p *Provider) isDatabaseUpgrading(ctx context.Context) (bool, error) {
	// Get PG pods to check if an upgrade is pending or in progress.
	listOpts := &client.ListOptions{
		Namespace:     p.DB.GetNamespace(),
		LabelSelector: labels.SelectorFromSet(labels.Set{"app.kubernetes.io/component": "pg", "app.kubernetes.io/instance": p.DB.GetName()}),
	}

	podList := &corev1.PodList{}
	if err := p.C.List(ctx, podList, listOpts); err != nil {
		return false, err
	}

	for _, pod := range podList.Items {
		for _, container := range pod.Spec.Containers {
			if container.Name != "database" {
				continue
			}

			// If there's a pod with a different image tag from the one
			// specified in the CR, an upgrade is pending or in progress.
			if container.Image != p.PerconaPGCluster.Spec.Image {
				return true, nil
			}
		}
	}

	return false, nil
}

// +kubebuilder:rbac:groups=postgres-operator.crunchydata.com,resources=postgresclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch

// Status builds the DatabaseCluster Status based on the current state of the PerconaPGCluster.
func (p *Provider) Status(ctx context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
	c := p.C
	pg := p.PerconaPGCluster

	status := p.DB.Status
	prevStatus := status
	status.Status = everestv1alpha1.AppState(pg.Status.State).WithCreatingState()
	status.Hostname = pg.Status.Host
	status.Ready = pg.Status.Postgres.Ready + pg.Status.PGBouncer.Ready
	status.Size = pg.Status.Postgres.Size + pg.Status.PGBouncer.Size
	status.Port = 5432
	status.CRVersion = pg.Spec.CRVersion
	status.Details = common.StatusAsPlainTextOrEmptyString(pg.Status)

	// If a restore is running for this database, set the database status to restoring
	if restoring, err := common.IsDatabaseClusterRestoreRunning(ctx, c, p.DB.GetName(), p.DB.GetNamespace()); err != nil {
		return status, false, err
	} else if restoring {
		status.Status = everestv1alpha1.AppStateRestoring
	}

	if ok, err := isPVCResizing(ctx, p.C, p.DB.GetName(), p.DB.GetNamespace()); err != nil {
		return status, false, err
	} else if ok {
		status.Status = everestv1alpha1.AppStateResizingVolumes
	}

	// If the PVC resize is currently in progress, or just finished, we need to
	// check if it failed in order to set or clear the error condition.
	if status.Status == everestv1alpha1.AppStateResizingVolumes ||
		prevStatus.Status == everestv1alpha1.AppStateResizingVolumes {
		meta.RemoveStatusCondition(&status.Conditions, everestv1alpha1.ConditionTypeVolumeResizeFailed)
		if failed, condMessage, err := common.VerifyPVCResizeFailure(ctx, p.C, p.DB.GetName(), p.DB.GetNamespace()); err != nil {
			return status, false, err
		} else if failed {
			// XXX: If a PVC resize failed, the DB operator will revert the
			// spec to the previous one and unset the annotation we use to
			// detect that a PVC resize is in progress. This means that we
			// would move away from the ResizingVolumes state until the next
			// reconcile loop where the PVC resize will be retried. To avoid
			// having the state change back and forth, we keep the state as
			// ResizingVolumes until the PVC resize is successful.
			status.Status = everestv1alpha1.AppStateResizingVolumes
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               everestv1alpha1.ConditionTypeVolumeResizeFailed,
				Status:             metav1.ConditionTrue,
				Reason:             everestv1alpha1.ReasonVolumeResizeFailed,
				Message:            condMessage,
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: p.DB.GetGeneration(),
			})
		}
	}

	if upgrading, err := p.isDatabaseUpgrading(ctx); err != nil {
		return status, false, err
	} else if upgrading {
		status.Status = everestv1alpha1.AppStateUpgrading
	}

	recCRVer, err := common.GetRecommendedCRVersion(ctx, p.C, consts.PGDeploymentName, p.DB)
	if err != nil && !k8serrors.IsNotFound(err) {
		return status, false, err
	}
	status.RecommendedCRVersion = recCRVer

	return status, true, nil
}

func isPVCResizing(ctx context.Context, c client.Client, name, namespace string) (bool, error) {
	pg := &crunchyv1beta1.PostgresCluster{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, pg); client.IgnoreNotFound(err) != nil {
		if client.IgnoreNotFound(err) != nil {
			return false, fmt.Errorf("failed to get PostgreSQL cluster: %w", err)
		}
		// If the PG cluster is not found, we assume it's not resizing.
		return false, nil
	}

	isResizing := meta.IsStatusConditionTrue(pg.Status.Conditions, crunchyv1beta1.PersistentVolumeResizing)
	if !isResizing {
		return false, nil
	}
	// There is a known bug in the Crunchy PostgreSQL Operator where the PVC resize condition
	// is not removed promptly. We need to verify the status of the actual PVCs to ensure we are not
	// reporting a false positive.
	// See: https://perconadev.atlassian.net/browse/K8SPG-747
	// TODO: Remove this once K8SPG-747 is fixed.
	return verifyPVCResizingStatus(ctx, c, name, namespace)
}

func verifyPVCResizingStatus(ctx context.Context, c client.Client, name, namespace string) (bool, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcList, client.InNamespace(namespace), client.MatchingLabels{"app.kubernetes.io/instance": name}); err != nil {
		return false, fmt.Errorf("failed to list PVCs: %w", err)
	}
	for _, pvc := range pvcList.Items {
		for _, condition := range pvc.Status.Conditions {
			if (condition.Type == corev1.PersistentVolumeClaimResizing ||
				condition.Type == corev1.PersistentVolumeClaimFileSystemResizePending) &&
				condition.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
	}
	return false, nil
}

func ensureBackupsDisabled(ctx context.Context, c client.Client, database *everestv1alpha1.DatabaseCluster) error {
	pg := &pgv2.PerconaPGCluster{}
	if err := c.Get(ctx, types.NamespacedName{Name: database.GetName(), Namespace: database.GetNamespace()}, pg); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("failed to get PostgreSQL cluster: %w", err)
		}
		return nil
	}
	pg.Spec.Backups.Enabled = pointer.To[bool](false)
	if err := c.Update(ctx, pg); err != nil {
		return fmt.Errorf("failed to update PostgreSQL cluster to disable backup schedules: %w", err)
	}
	return nil
}

// handleDBBackupsCleanup handles the cleanup of the dbbackup objects.
// Returns true if cleanup is complete.
func handleDBBackupsCleanup(
	ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
) (bool, error) {
	if controllerutil.ContainsFinalizer(database, consts.DBBackupCleanupFinalizer) {
		// Disable backups on the cluster before deleting the PerconaPGBackup CR.
		//
		// Why this is necessary:
		// As long as backups are enabled, the operator runs a WAL watcher goroutine
		// (WatchCommitTimestamps) that continuously sends GenericEvents to the backup
		// controller's reconcile queue. Each enqueue causes the BackupSucceeded reconcile
		// branch to run, which writes LatestRestorableTime to the PerconaPGBackup status
		// and removes FinalizerKeepJob from the underlying Job. These writes in turn
		// trigger new cluster reconciles via the owned-object watch. The result is a
		// constant stream of cluster reconcile loops while any Succeeded backup exists.
		//
		// Each of those reconcile loops calls reconcileBackupJob(), which recreates a
		// PerconaPGBackup CR for any backup Job it finds without one. If we delete the
		// CR while this stream is active, there is a window between the CR disappearing
		// and the Kubernetes GC setting DeletionTimestamp on the owned Job. Any reconcile
		// loop landing in that window will create a new CR, undoing the deletion.
		//
		// Setting Spec.Backups.Enabled=false stops the WAL watcher goroutine and makes
		// the crunchy reconciler skip all pgBackRest management (clearing its status and
		// returning early). This drains the constant flow of reconcile triggers before
		// we delete the CR, so by the time we issue the delete the Job's DeletionTimestamp
		// is already set and reconcileBackupJob() will not recreate the CR.
		err := ensureBackupsDisabled(ctx, c, database)
		if err != nil {
			return false, err
		}

		if done, err := common.DeleteBackupsForDatabase(ctx, c, database.GetName(), database.GetNamespace()); err != nil {
			return false, err
		} else if !done {
			return false, nil
		}

		// Wait for the backup controller to fully finish reconciling deleted backups
		// before deleting the cluster.
		//
		// When a PerconaPGBackup is deleted, its controller runs a finalizer
		// (internal.percona.com/delete-backup) via finishBackup(), which performs
		// several cleanup steps: removing the pgbackrest backup annotation from the
		// crunchy PostgresCluster, stripping pgbackrest labels from the backup Job,
		// clearing the ManualBackup status on the crunchy PostgresCluster, and finally
		// removing the backup-in-progress annotation. Each of these steps involves
		// writing directly to the crunchy PostgresCluster object, which wakes up the
		// crunchy reconciler. The backup controller retries this loop every 5 seconds
		// until AnnotationBackupInProgress is cleared, so writes can continue for
		// several seconds after the PerconaPGBackup object has disappeared from the API.
		//
		// If the cluster is deleted while those writes are still happening, the
		// following race occurs:
		//   1. The Percona cluster reconciler runs the delete-pvc/delete-ssl finalizers,
		//      deleting user and TLS secrets.
		//   2. The backup controller writes to the crunchy PostgresCluster, waking the
		//      crunchy reconciler.
		//   3. If the crunchy reconciler runs before Delete(postgresCluster) sets a
		//      DeletionTimestamp, it sees a live cluster with missing secrets and
		//      recreates them.
		//   4. The secrets are left behind permanently after the cluster is gone.
		//
		// https://github.com/percona/percona-postgresql-operator/issues/1564
		//
		// The 5s sleep gives the backup controller's finalizer loop enough time to
		// complete all its writes and clear AnnotationBackupInProgress before we
		// trigger cluster deletion, closing the race window.
		time.Sleep(5 * time.Second) //nolint:mnd

		controllerutil.RemoveFinalizer(database, consts.DBBackupCleanupFinalizer)
		return true, c.Update(ctx, database)
	}
	return true, nil
}

// Cleanup runs the cleanup routines and returns true if the cleanup is done.
func (p *Provider) Cleanup(ctx context.Context, database *everestv1alpha1.DatabaseCluster) (bool, error) {
	done, err := handleDBBackupsCleanup(ctx, p.C, database)
	if err != nil || !done {
		return done, err
	}
	return common.HandleUpstreamClusterCleanup(ctx, p.C, database, &pgv2.PerconaPGCluster{})
}

// DBObject returns the PerconaPGCluster object.
//
//nolint:ireturn
func (p *Provider) DBObject() client.Object {
	p.PerconaPGCluster.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   consts.PGAPIGroup,
		Version: "v2",
		Kind:    consts.PerconaPGClusterKind,
	})
	return p.PerconaPGCluster
}

// RunPreReconcileHook runs the pre-reconcile hook for the PG provider.
func (p *Provider) RunPreReconcileHook(_ context.Context) (providers.HookResult, error) {
	return providers.HookResult{}, nil
}
