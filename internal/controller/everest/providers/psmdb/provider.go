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

// Package psmdb contains the provider for Percona Server for MongoDB.
package psmdb

import (
	"context"

	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
	"github.com/percona/everest-operator/internal/controller/everest/version"
)

const (
	finalizerDeletePSMDBPodsInOrder = "percona.com/delete-psmdb-pods-in-order"
	finalizerDeletePSMDBPVC         = "percona.com/delete-psmdb-pvc"
)

// Provider is a provider for Percona Server for MongoDB.
type Provider struct {
	*psmdbv1.PerconaServerMongoDB
	providers.ProviderOptions

	// currentPSMDB holds the current PXC spec.
	currentPSMDBSpec psmdbv1.PerconaServerMongoDBSpec

	clusterType     consts.ClusterType
	operatorVersion *version.Version
}

// New returns a new provider for Percona Server for MongoDB.
func New(
	ctx context.Context,
	opts providers.ProviderOptions,
) (*Provider, error) {
	client := opts.C

	psmdb := &psmdbv1.PerconaServerMongoDB{}
	err := client.Get(ctx,
		types.NamespacedName{
			Name:      opts.DB.GetName(),
			Namespace: opts.DB.GetNamespace(),
		},
		psmdb)
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	}

	dbEngine, err := common.GetDatabaseEngine(ctx, client, consts.PSMDBDeploymentName, opts.DB.GetNamespace())
	if err != nil {
		return nil, err
	}
	opts.DBEngine = dbEngine

	// Get operator version.
	v, err := common.GetOperatorVersion(ctx, opts.C, types.NamespacedName{
		Name:      consts.PSMDBDeploymentName,
		Namespace: opts.DB.GetNamespace(),
	})
	if err != nil {
		return nil, err
	}

	currentSpec := psmdb.Spec
	psmdb.Spec = defaultSpec()
	p := &Provider{
		PerconaServerMongoDB: psmdb,
		ProviderOptions:      opts,
		operatorVersion:      v,
		currentPSMDBSpec:     currentSpec,
	}
	ct, err := common.GetClusterType(ctx, p.C)
	if err != nil {
		return nil, err
	}
	p.clusterType = ct
	return p, nil
}

// Apply returns the applier for Percona Server for MongoDB.
//
//nolint:ireturn
func (p *Provider) Apply(ctx context.Context) everestv1alpha1.Applier {
	return &applier{
		Provider: p,
		ctx:      ctx,
	}
}

// Status builds the DatabaseCluster Status based on the current state of the PerconaServerMongoDB.
func (p *Provider) Status(ctx context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
	status := p.DB.Status
	prevStatus := status
	psmdb := p.PerconaServerMongoDB

	activeStorage := getActiveStorage(psmdb)
	status.Status = everestv1alpha1.AppState(psmdb.Status.State).WithCreatingState()
	status.Hostname = psmdb.Status.Host
	status.Ready = psmdb.Status.Ready
	status.Size = psmdb.Status.Size
	message := psmdb.Status.Message
	conditions := psmdb.Status.Conditions
	if message == "" && len(conditions) != 0 {
		message = conditions[len(conditions)-1].Message
	}
	status.Message = message
	status.Port = 27017
	status.ActiveStorage = activeStorage
	status.CRVersion = psmdb.Spec.CRVersion
	status.Details = common.StatusAsPlainTextOrEmptyString(psmdb.Status)

	// If a restore is running for this database, set the database status to restoring.
	if restoring, err := common.IsDatabaseClusterRestoreRunning(ctx, p.C, p.DB.GetName(), p.DB.GetNamespace()); err != nil {
		return status, false, err
	} else if restoring {
		status.Status = everestv1alpha1.AppStateRestoring
	}

	if inProgress, err := isPVCResizeInProgress(ctx, p.C, p.PerconaServerMongoDB); err != nil {
		return status, false, err
	} else if inProgress {
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

	// If the current version of the database is different from the version in
	// the CR, an upgrade is pending or in progress.
	if p.DB.Spec.Engine.Version != "" && psmdb.Status.MongoVersion != "" && p.DB.Spec.Engine.Version != psmdb.Status.MongoVersion {
		status.Status = everestv1alpha1.AppStateUpgrading
	}

	recCRVer, err := common.GetRecommendedCRVersion(ctx, p.C, consts.PSMDBDeploymentName, p.DB)
	if err != nil && !k8serrors.IsNotFound(err) {
		return status, false, err
	}
	status.RecommendedCRVersion = recCRVer

	// Set PSMDB engine features statuses (if any).
	var efStatuses *everestv1alpha1.PSMDBEngineFeaturesStatus
	var statusReady bool
	if efStatuses, statusReady = NewEngineFeaturesApplier(p).GetEngineFeaturesStatuses(ctx); efStatuses != nil {
		status.EngineFeatures = &everestv1alpha1.EngineFeaturesStatus{
			PSMDB: efStatuses,
		}
	}

	return status, statusReady, nil
}

func isPVCResizeInProgress(ctx context.Context, c client.Client, psmdb *psmdbv1.PerconaServerMongoDB) (bool, error) {
	if psmdb.Status.State == psmdbv1.AppStateInit {
		// We must list all StatefulSets belonging to this PSMDB object,
		// and check for the PVC resize annotation.
		stsList := &appsv1.StatefulSetList{}
		err := c.List(
			ctx,
			stsList,
			client.InNamespace(psmdb.GetNamespace()),
			client.MatchingLabels{
				"app.kubernetes.io/instance": psmdb.GetName(),
			},
		)
		if err != nil {
			return false, err
		}
		for _, sts := range stsList.Items {
			annots := sts.GetAnnotations()
			_, ok := annots[psmdbv1.AnnotationPVCResizeInProgress]
			if ok {
				return true, nil
			}
		}
	}
	return false, nil
}

// Cleanup runs the cleanup routines and returns true if the cleanup is done.
func (p *Provider) Cleanup(ctx context.Context, database *everestv1alpha1.DatabaseCluster) (bool, error) {
	// as a first step of the psmdb cleanup we need to ensure that psmdb pitr is disabled, otherwise the
	// "failed to delete backup: unable to delete the last backup while PITR is on"
	// appears when trying to delete the last backup.
	err := ensurePSMDBPitrDisabled(ctx, p.C, database)
	if err != nil {
		return false, err
	}
	// Even though we no longer set the DBBackupCleanupFinalizer, we still need
	// to handle the cleanup to ensure backward compatibility.
	done, err := common.HandleDBBackupsCleanup(ctx, p.C, database)
	if err != nil || !done {
		return done, err
	}
	return common.HandleUpstreamClusterCleanup(ctx, p.C, database, &psmdbv1.PerconaServerMongoDB{})
}

func ensurePSMDBPitrDisabled(ctx context.Context,
	c client.Client,
	database *everestv1alpha1.DatabaseCluster,
) error {
	psmdb := &psmdbv1.PerconaServerMongoDB{}
	err := c.Get(ctx, types.NamespacedName{
		Name:      database.Name,
		Namespace: database.Namespace,
	}, psmdb)
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	if !psmdb.Spec.Backup.PITR.Enabled {
		return nil
	}
	psmdb.Spec.Backup.PITR.Enabled = false
	return client.IgnoreNotFound(c.Update(ctx, psmdb))
}

// DBObject returns the PerconaServerMongoDB object.
//
//nolint:ireturn
func (p *Provider) DBObject() client.Object {
	p.PerconaServerMongoDB.SetGroupVersionKind(schema.GroupVersionKind{
		Version: p.operatorVersion.ToK8sVersion(),
		Group:   consts.PSMDBAPIGroup,
		Kind:    consts.PerconaServerMongoDBKind,
	})
	return p.PerconaServerMongoDB
}

func defaultSpec() psmdbv1.PerconaServerMongoDBSpec {
	maxUnavailable := intstr.FromInt(1)
	return psmdbv1.PerconaServerMongoDBSpec{
		UpdateStrategy: psmdbv1.SmartUpdateStatefulSetStrategyType,
		UpgradeOptions: psmdbv1.UpgradeOptions{
			Apply:    "disabled",
			Schedule: "0 4 * * *",
			SetFCV:   true,
		},
		PMM: psmdbv1.PMMSpec{},
		Replsets: []*psmdbv1.ReplsetSpec{
			{
				Name: "rs0",
				MultiAZ: psmdbv1.MultiAZ{
					PodDisruptionBudget: &psmdbv1.PodDisruptionBudgetSpec{
						MaxUnavailable: &maxUnavailable,
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{},
					},
				},
			},
		},
		Sharding: psmdbv1.Sharding{
			Enabled: false,
		},
	}
}

// RunPreReconcileHook runs the pre-reconcile hook for the PSMDB provider.
func (p *Provider) RunPreReconcileHook(_ context.Context) (providers.HookResult, error) {
	return providers.HookResult{}, nil
}

func getActiveStorage(psmdb *psmdbv1.PerconaServerMongoDB) string {
	for name := range psmdb.Spec.Backup.Storages {
		return name
	}
	return ""
}
