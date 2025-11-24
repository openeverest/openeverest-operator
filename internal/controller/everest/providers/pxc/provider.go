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

// Package pxc contains the provider for Percona XtraDB Cluster.
package pxc

import (
	"context"
	"errors"
	"strings"
	"time"

	goversion "github.com/hashicorp/go-version"
	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
	"github.com/percona/everest-operator/internal/controller/everest/version"
)

const (
	pxcHAProxyEnvSecretName = "haproxy-env-secret" //nolint:gosec // This is not a credential, only a secret name.

	finalizerDeletePXCPodsInOrder = "percona.com/delete-pxc-pods-in-order"
	finalizerDeletePXCPVC         = "percona.com/delete-pxc-pvc"
	finalizerDeletePXCSSL         = "percona.com/delete-ssl"
)

// Provider is a provider for Percona XtraDB Cluster.
type Provider struct {
	providers.ProviderOptions
	*pxcv1.PerconaXtraDBCluster

	// currentPerconaXtraDBClusterSpec holds the current PXC spec.
	currentPerconaXtraDBClusterSpec pxcv1.PerconaXtraDBClusterSpec

	clusterType     consts.ClusterType
	operatorVersion *version.Version
}

// New returns a new provider for Percona XtraDB Cluster.
func New(
	ctx context.Context,
	opts providers.ProviderOptions,
) (*Provider, error) {
	pxc := &pxcv1.PerconaXtraDBCluster{}
	client := opts.C
	err := client.Get(
		ctx,
		types.NamespacedName{Name: opts.DB.GetName(), Namespace: opts.DB.GetNamespace()},
		pxc)
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	}

	dbEngine, err := common.GetDatabaseEngine(ctx, client, consts.PXCDeploymentName, opts.DB.GetNamespace())
	if err != nil {
		return nil, err
	}
	opts.DBEngine = dbEngine

	// Get operator version.
	v, err := common.GetOperatorVersion(ctx, opts.C, types.NamespacedName{
		Name:      consts.PXCDeploymentName,
		Namespace: opts.DB.GetNamespace(),
	})
	if err != nil {
		return nil, err
	}

	currentSpec := pxc.Spec
	pxc.Spec = defaultSpec()

	p := &Provider{
		PerconaXtraDBCluster:            pxc,
		ProviderOptions:                 opts,
		operatorVersion:                 v,
		currentPerconaXtraDBClusterSpec: currentSpec,
	}

	// Get cluster type.
	ct, err := common.GetClusterType(ctx, p.C)
	if err != nil {
		return nil, err
	}
	p.clusterType = ct

	if err := p.ensureDefaults(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

// Apply returns the applier for Percona XtraDB Cluster.
//
//nolint:ireturn
func (p *Provider) Apply(ctx context.Context) everestv1alpha1.Applier {
	return &applier{
		Provider: p,
		ctx:      ctx,
	}
}

func (p *Provider) dbEngineVersionOrDefault() string {
	engineVersion := p.DB.Spec.Engine.Version
	if engineVersion == "" {
		engineVersion = p.DBEngine.BestEngineVersion()
	}
	return engineVersion
}

func (p *Provider) ensureDefaults(ctx context.Context) error {
	db := p.DB
	updated := false
	if db.Spec.Proxy.Type == "" {
		db.Spec.Proxy.Type = everestv1alpha1.ProxyTypeHAProxy
		updated = true
	}

	engineSemVer, err := goversion.NewVersion(p.dbEngineVersionOrDefault())
	if err != nil {
		return errors.Join(err, errors.New("cannot parse engine version"))
	}
	engineSemVer = engineSemVer.Core()

	if db.Spec.Engine.Config == "" &&
		engineSemVer.GreaterThanOrEqual(minVersionForOptimizedConfig) {
		switch db.Spec.Engine.Size() {
		case everestv1alpha1.EngineSizeSmall:
			db.Spec.Engine.Config = pxcConfigSizeSmall
		case everestv1alpha1.EngineSizeMedium:
			db.Spec.Engine.Config = pxcConfigSizeMedium
		case everestv1alpha1.EngineSizeLarge:
			db.Spec.Engine.Config = pxcConfigSizeLarge
		}
		updated = true
	}

	if updated {
		return p.C.Update(ctx, db)
	}
	return nil
}

// Status builds the DatabaseCluster Status based on the current state of the Percona XtraDB Cluster.
func (p *Provider) Status(ctx context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
	status := p.DB.Status
	prevStatus := status
	pxc := p.PerconaXtraDBCluster

	status.Status = everestv1alpha1.AppState(p.PerconaXtraDBCluster.Status.Status).WithCreatingState()
	status.Hostname = pxc.Status.Host
	status.Ready = pxc.Status.Ready
	status.Size = pxc.Status.Size
	status.Message = strings.Join(pxc.Status.Messages, ";")
	status.Port = 3306
	status.CRVersion = pxc.Spec.CRVersion
	status.Details = common.StatusAsPlainTextOrEmptyString(pxc.Status)

	// If a restore is running for this database, set the database status to restoring.
	if restoring, err := common.IsDatabaseClusterRestoreRunning(ctx, p.C, p.DB.GetName(), p.DB.GetNamespace()); err != nil {
		return status, false, err
	} else if restoring {
		status.Status = everestv1alpha1.AppStateRestoring
	}

	annotations := pxc.GetAnnotations()
	_, pvcResizing := annotations[pxcv1.AnnotationPVCResizeInProgress]
	if pvcResizing {
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
	if p.DB.Spec.Engine.Version != "" && pxc.Status.PXC.Version != "" && p.DB.Spec.Engine.Version != pxc.Status.PXC.Version {
		status.Status = everestv1alpha1.AppStateUpgrading
	}

	recCRVer, err := common.GetRecommendedCRVersion(ctx, p.C, consts.PXCDeploymentName, p.DB)
	if err != nil && !k8serrors.IsNotFound(err) {
		return status, false, err
	}
	status.RecommendedCRVersion = recCRVer

	return status, true, nil
}

// when a PXC restore is in progress, we will retry reconciliation
// after the specified duration.
const defaultRestoreRequeueDuration = 15 * time.Second

func (p *Provider) isRestoreInProgress(ctx context.Context) (bool, error) {
	restores := &everestv1alpha1.DatabaseClusterRestoreList{}
	if err := p.C.List(ctx, restores, client.InNamespace(p.DB.GetNamespace())); err != nil {
		return false, err
	}
	for _, dbr := range restores.Items {
		if dbr.IsInProgress() && dbr.Spec.DBClusterName == p.DB.GetName() {
			return true, nil
		}
	}
	return false, nil
}

// RunPreReconcileHook runs the pre-reconcile hook for the PXC provider.
func (p *Provider) RunPreReconcileHook(ctx context.Context) (providers.HookResult, error) {
	// The pxc-operator does some funny things to the PXC spec during a restore.
	// We must avoid interfering with that process, so we simply skip reconciliation.
	// Replicating the same behavior here would be a nightmare, so its simpler to just do this.
	if ok, err := p.isRestoreInProgress(ctx); err != nil {
		return providers.HookResult{}, err
	} else if ok {
		return providers.HookResult{
			RequeueAfter: defaultRestoreRequeueDuration,
			Message:      "Restore is in progress",
		}, nil
	}
	return providers.HookResult{}, nil
}

// Cleanup runs the cleanup routines and returns true if the cleanup is done.
func (p *Provider) Cleanup(ctx context.Context, database *everestv1alpha1.DatabaseCluster) (bool, error) {
	// Even though we no longer set the DBBackupCleanupFinalizer, we still need
	// to handle the cleanup to ensure backward compatibility.
	done, err := common.HandleDBBackupsCleanup(ctx, p.C, database)
	if err != nil || !done {
		return done, err
	}
	return common.HandleUpstreamClusterCleanup(ctx, p.C, database, &pxcv1.PerconaXtraDBCluster{})
}

// DBObject returns the PerconaXtraDBCluster object.
//
//nolint:ireturn
func (p *Provider) DBObject() client.Object {
	p.PerconaXtraDBCluster.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   consts.PXCAPIGroup,
		Version: p.operatorVersion.ToK8sVersion(),
		Kind:    consts.PerconaXtraDBClusterKind,
	})
	return p.PerconaXtraDBCluster
}
