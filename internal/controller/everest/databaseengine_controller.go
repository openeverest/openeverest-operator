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
	"regexp"
	"slices"
	"strings"
	"time"

	opfwv1alpha1 "github.com/operator-framework/api/pkg/operators/v1alpha1"
	"golang.org/x/mod/semver"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/version"
	"github.com/percona/everest-operator/internal/predicates"
)

const (
	requeueAfter   = 10 * time.Second
	upgradeTimeout = 10 * time.Minute
)

var errInstallPlanNotFound = errors.New("install plan not found")

var operatorEngine = map[string]everestv1alpha1.EngineType{
	consts.PXCDeploymentName:   everestv1alpha1.DatabaseEnginePXC,
	consts.PSMDBDeploymentName: everestv1alpha1.DatabaseEnginePSMDB,
	consts.PGDeploymentName:    everestv1alpha1.DatabaseEnginePostgresql,
}

// DatabaseEngineReconciler reconciles a DatabaseEngine object.
type DatabaseEngineReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	versionService *version.Service

	Controllers []DatabaseController
}

// DatabaseController provides an abstraction for the DatabaseEngine controller
// to orchestrate operations across various database controllers (like DBCluster, DBBackup, etc.).
type DatabaseController interface {
	ReconcileWatchers(ctx context.Context) error
}

//+kubebuilder:rbac:groups=everest.percona.com,resources=databaseengines,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=everest.percona.com,resources=databaseengines/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=everest.percona.com,resources=databaseengines/finalizers,verbs=update
//+kubebuilder:rbac:groups=operators.coreos.com,resources=installplans,verbs=get;list;watch;update
//+kubebuilder:rbac:groups=operators.coreos.com,resources=clusterserviceversions,verbs=get;list;watch;update
//+kubebuilder:rbac:groups=operators.coreos.com,resources=subscriptions,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods,verbs=delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *DatabaseEngineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	engineType, ok := operatorEngine[req.NamespacedName.Name]
	if !ok {
		// Unknown operator, nothing to do here
		return ctrl.Result{}, nil
	}

	dbEngine := &everestv1alpha1.DatabaseEngine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.NamespacedName.Name,
			Namespace: req.NamespacedName.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, dbEngine, func() error {
		dbEngine.Spec.Type = engineType
		dbEngine.Spec.SecretKeys.User = userSecretKeys[engineType]
		return nil
	}); err != nil {
		return ctrl.Result{}, err
	}

	pendingUpgrades, err := r.listPendingOperatorUpgrades(ctx, dbEngine)
	if err != nil {
		return ctrl.Result{}, err
	}
	dbEngine.Status.PendingOperatorUpgrades = pendingUpgrades

	dbEngine.Status.State = everestv1alpha1.DBEngineStateNotInstalled
	ready, version, err := r.getOperatorStatus(ctx, req.NamespacedName)
	if err != nil {
		// Operator not installed, upgrade the status and return.
		if apierrors.IsNotFound(err) {
			return ctrl.Result{},
				r.Status().Update(ctx, dbEngine)
		}
		return ctrl.Result{}, err
	}

	// Not ready yet, update status and check again later.
	dbEngine.Status.OperatorVersion = version // even though deployment is not ready, we still know the version through the image tag.
	if !ready {
		dbEngine.Status.State = everestv1alpha1.DBEngineStateInstalling
		if err := r.reconcileOperatorUpgradeStatus(ctx, dbEngine); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{},
			r.Status().Update(ctx, dbEngine)
	}

	timeUntilUnlock, err := r.tryUnlockDBEngine(ctx, dbEngine)
	if err != nil {
		return ctrl.Result{}, err
	}

	dbEngine.Status.State = everestv1alpha1.DBEngineStateInstalled
	dbEngine.Status.OperatorUpgrade = nil
	matrix, err := r.versionService.GetVersions(engineType, dbEngine.Status.OperatorVersion)
	if err != nil {
		return ctrl.Result{}, err
	}

	versions := everestv1alpha1.Versions{
		Backup: matrix.Backup,
	}

	if dbEngine.Spec.Type == everestv1alpha1.DatabaseEnginePXC {
		for key := range matrix.PXC {
			// We do not need supporting mysql 5
			if strings.HasPrefix(key, "5") {
				delete(matrix.PXC, key)
			}
		}
		versions.Engine = matrix.PXC
		versions.Proxy = map[everestv1alpha1.ProxyType]everestv1alpha1.ComponentsMap{
			everestv1alpha1.ProxyTypeHAProxy:  matrix.HAProxy,
			everestv1alpha1.ProxyTypeProxySQL: matrix.ProxySQL,
		}
		versions.Tools = map[string]everestv1alpha1.ComponentsMap{
			"logCollector": matrix.LogCollector,
		}
	}

	if dbEngine.Spec.Type == everestv1alpha1.DatabaseEnginePSMDB {
		versions.Engine = matrix.Mongod
	}

	if dbEngine.Spec.Type == everestv1alpha1.DatabaseEnginePostgresql {
		versions.Engine = matrix.Postgresql
		versions.Backup = matrix.PGBackRest
		versions.Proxy = map[everestv1alpha1.ProxyType]everestv1alpha1.ComponentsMap{
			everestv1alpha1.ProxyTypePGBouncer: matrix.PGBouncer,
		}
	}
	dbEngine.Status.AvailableVersions = versions

	if err := r.Status().Update(ctx, dbEngine); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileWatchers(ctx); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to reconcile watchers: %w", err)
	}

	return ctrl.Result{
		RequeueAfter: timeUntilUnlock,
	}, nil
}

func (r *DatabaseEngineReconciler) reconcileWatchers(ctx context.Context) error {
	for _, c := range r.Controllers {
		if err := c.ReconcileWatchers(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *DatabaseEngineReconciler) reconcileOperatorUpgradeStatus(
	ctx context.Context,
	dbEngine *everestv1alpha1.DatabaseEngine,
) error {
	// We depend on reading CSV status for upgrades,
	// so if OLM is not installed, we cannot check for upgrades.
	if !r.isOLMInstalled(ctx) {
		return nil
	}

	// get the CSV for the current installed version.
	csv := &opfwv1alpha1.ClusterServiceVersion{}
	csvKey := types.NamespacedName{
		Name:      dbEngine.GetName() + ".v" + dbEngine.Status.OperatorVersion,
		Namespace: dbEngine.GetNamespace(),
	}
	if err := r.Get(ctx, csvKey, csv); err != nil {
		return err
	}

	// Try to find the new CSV that replaces the current version.
	newCSV := opfwv1alpha1.ClusterServiceVersion{}
	csvs := &opfwv1alpha1.ClusterServiceVersionList{}
	if err := r.List(ctx, csvs, client.InNamespace(dbEngine.GetNamespace())); err != nil {
		return err
	}
	if idx := slices.IndexFunc(csvs.Items, func(c opfwv1alpha1.ClusterServiceVersion) bool {
		return c.Spec.Replaces == csv.GetName()
	}); idx > 0 {
		newCSV = csvs.Items[idx]
	} else {
		// The upgrade might not have started yet, so we don't have the new CSV.
		// We shall check again later.
		return nil
	}

	// Find the InstallPlan that created the new CSV.
	var installPlan *opfwv1alpha1.InstallPlan
	ipList := &opfwv1alpha1.InstallPlanList{}
	if err := r.List(ctx, ipList, client.InNamespace(dbEngine.GetNamespace())); err != nil {
		return err
	}
	if idx := slices.IndexFunc(ipList.Items, func(ip opfwv1alpha1.InstallPlan) bool {
		return slices.Contains(ip.Spec.ClusterServiceVersionNames, newCSV.GetName())
	}); idx > 0 {
		installPlan = &ipList.Items[idx]
	} else {
		return errInstallPlanNotFound
	}

	if csv.Status.Phase == opfwv1alpha1.CSVPhaseReplacing {
		dbEngine.Status.State = everestv1alpha1.DBEngineStateUpgrading
		if dbEngine.Status.OperatorUpgrade == nil {
			_, targetVersion := parseOperatorCSVName(newCSV.GetName())
			now := metav1.Now()
			dbEngine.Status.OperatorUpgrade = &everestv1alpha1.OperatorUpgradeStatus{
				OperatorUpgrade: everestv1alpha1.OperatorUpgrade{
					TargetVersion: targetVersion,
					InstallPlanRef: corev1.LocalObjectReference{
						Name: installPlan.GetName(),
					},
				},
				Phase:     everestv1alpha1.UpgradePhaseStarted,
				StartedAt: &now,
				Message:   "Upgrading operator to version " + targetVersion,
			}
		}
	}

	if newCSV.Status.Phase == opfwv1alpha1.CSVPhaseFailed {
		dbEngine.Status.OperatorUpgrade.Phase = everestv1alpha1.UpgradePhaseFailed
	}
	return nil
}

// tryUnlockDBEngine checks if the upgrade lock can be removed.
// If a lock is set, returns the duration after which we expect the lock to be removed.
func (r *DatabaseEngineReconciler) tryUnlockDBEngine(
	ctx context.Context,
	dbEngine *everestv1alpha1.DatabaseEngine,
) (time.Duration, error) {
	logger := log.FromContext(ctx)
	annotations := dbEngine.GetAnnotations()
	if annotations == nil {
		return 0, nil
	}
	lockedAtStr, ok := annotations[everestv1alpha1.DatabaseOperatorUpgradeLockAnnotation]
	if !ok {
		return 0, nil
	}
	lockedAt, err := time.Parse(time.RFC3339, lockedAtStr)
	if err != nil {
		return 0, errors.Join(err, errors.New("cannot parse the value of 'everest.percona.com/upgrade-lock' annotation"))
	}
	removeAt := lockedAt.Add(upgradeTimeout)
	timeUntilUnlock := time.Until(removeAt)

	unlock := func() error {
		delete(annotations, everestv1alpha1.DatabaseOperatorUpgradeLockAnnotation)
		dbEngine.SetAnnotations(annotations)
		return r.Update(ctx, dbEngine)
	}

	// Check if it has exceeded the upgrade timeout.
	if timeUntilUnlock <= 0 {
		logger.Info("databaseengine upgrade timeout, force unlock triggered")
		return 0, unlock()
	}

	// If there's a pending upgrade, we cannot yet remove the lock.
	if len(dbEngine.Status.PendingOperatorUpgrades) > 0 {
		return timeUntilUnlock, nil
	}

	// If the CSV has not yet succeeded, we cannot yet remove the lock.
	csv := &opfwv1alpha1.ClusterServiceVersion{}
	csvKey := types.NamespacedName{
		Name:      dbEngine.GetName() + ".v" + dbEngine.Status.OperatorVersion,
		Namespace: dbEngine.GetNamespace(),
	}
	if err := r.Get(ctx, csvKey, csv); err != nil {
		return 0, err
	}
	if csv.Status.Phase != opfwv1alpha1.CSVPhaseSucceeded {
		return timeUntilUnlock, nil
	}
	return 0, unlock()
}

func getInstallPlanRefsForUpgrade(
	dbEngine *everestv1alpha1.DatabaseEngine,
	subscription *opfwv1alpha1.Subscription,
	installPlans *opfwv1alpha1.InstallPlanList,
) map[string]string {
	currentVersion := dbEngine.Status.OperatorVersion
	upgradeStatus := dbEngine.Status.OperatorUpgrade
	result := make(map[string]string)

	// Sort installPlans such that the newest is first
	// For a given version, we will consider only the newest InstallPlan.
	slices.SortFunc(installPlans.Items, func(a, b opfwv1alpha1.InstallPlan) int {
		return b.GetCreationTimestamp().Time.Compare(a.GetCreationTimestamp().Time)
	})

	for _, ip := range installPlans.Items {
		for _, csvName := range ip.Spec.ClusterServiceVersionNames {
			operatorName, version := parseOperatorCSVName(csvName)
			// Not our operator, skip.
			if operatorName != dbEngine.GetName() {
				continue
			}
			// Skip the current version.
			if version == currentVersion {
				continue
			}
			// Skip the version we're upgrading to (if any).
			if upgradeStatus != nil && upgradeStatus.TargetVersion == version {
				continue
			}
			// Skip if not owned by current Subscription.
			if !common.IsOwnedBy(&ip, subscription) {
				continue
			}
			// Check if the version is greater than the current version.
			if semver.Compare("v"+version, "v"+currentVersion) > 0 {
				if _, ok := result[version]; ok {
					continue // we already have this version, with the newest IP
				}
				result[version] = ip.GetName()
			}
		}
	}
	return result
}

func (r *DatabaseEngineReconciler) listPendingOperatorUpgrades(
	ctx context.Context,
	dbEngine *everestv1alpha1.DatabaseEngine,
) ([]everestv1alpha1.OperatorUpgrade, error) {
	// If OLM is not installed, we cannot check for pending upgrades.
	if !r.isOLMInstalled(ctx) {
		return nil, nil
	}
	// We need some version to be reported first.
	currentVersion := dbEngine.Status.OperatorVersion
	if currentVersion == "" {
		return nil, nil
	}

	// Get the Subscription for this operator.
	subscription := &opfwv1alpha1.Subscription{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      dbEngine.GetName(),
		Namespace: dbEngine.GetNamespace(),
	}, subscription); err != nil {
		// the Subscription is not found or removed, so we cannot check for upgrades.
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	// List install plans in this namespace.
	installPlans := &opfwv1alpha1.InstallPlanList{}
	if err := r.List(ctx, installPlans, client.InNamespace(dbEngine.GetNamespace())); err != nil {
		return nil, err
	}

	installPlanRefs := getInstallPlanRefsForUpgrade(dbEngine, subscription, installPlans)
	result := []everestv1alpha1.OperatorUpgrade{}
	for v, ipName := range installPlanRefs {
		result = append(result, everestv1alpha1.OperatorUpgrade{
			TargetVersion:  v,
			InstallPlanRef: corev1.LocalObjectReference{Name: ipName},
		})
	}
	return result, nil
}

// get the status and version of the operator deployment.
// TODO: can we instead check the status of the corresponding CSV?
//
//nolint:godox
func (r *DatabaseEngineReconciler) getOperatorStatus(ctx context.Context, name types.NamespacedName) (bool, string, error) {
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, name, deployment); err != nil {
		return false, "", err
	}
	version := strings.Split(deployment.Spec.Template.Spec.Containers[0].Image, ":")[1]
	ready := deployment.Status.ReadyReplicas == deployment.Status.Replicas &&
		deployment.Status.Replicas == deployment.Status.UpdatedReplicas &&
		deployment.Status.UnavailableReplicas == 0 &&
		deployment.GetGeneration() == deployment.Status.ObservedGeneration
	return ready, version, nil
}

func (r *DatabaseEngineReconciler) ensureDBEnginesInNamespaces(ctx context.Context, namespaces []string) ([]reconcile.Request, error) {
	requests := []reconcile.Request{}
	for _, ns := range namespaces {
		for operatorName, engineType := range operatorEngine {
			dbEngine := &everestv1alpha1.DatabaseEngine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      operatorName,
					Namespace: ns,
				},
				Spec: everestv1alpha1.DatabaseEngineSpec{
					Type: engineType,
				},
			}
			if err := r.Client.Create(ctx, dbEngine); client.IgnoreAlreadyExists(err) != nil {
				return nil, err
			}
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{
				Name:      dbEngine.GetName(),
				Namespace: dbEngine.GetNamespace(),
			}})
		}
	}
	return requests, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseEngineReconciler) SetupWithManager(mgr ctrl.Manager, namespaces []string) error {
	if _, err := r.ensureDBEnginesInNamespaces(context.Background(), namespaces); err != nil {
		return err
	}
	r.versionService = version.NewVersionService()
	c := ctrl.NewControllerManagedBy(mgr).
		Named("DatabaseEngine").
		For(&everestv1alpha1.DatabaseEngine{}).
		Watches(&appsv1.Deployment{}, &handler.EnqueueRequestForObject{}).
		Watches(
			&corev1.Namespace{},
			common.EnqueueObjectsInNamespace(r.Client, &everestv1alpha1.DatabaseEngineList{}),
		)

	// No DBNamespaces were provided during setup, so we need to watch Namespaces,
	// and create and enqueue DBEngine requests on the fly.
	if len(namespaces) == 0 {
		filter, ok := common.DefaultNamespaceFilter.(*predicates.NamespaceFilter)
		if !ok {
			return errors.New("expected common.DefaultNamespaceFilter to be of type *NamespaceFilter")
		}
		c.Watches(&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
				ns, ok := o.(*corev1.Namespace)
				if !ok {
					return nil
				}
				if !filter.Match(ns) {
					return nil
				}
				requests, err := r.ensureDBEnginesInNamespaces(ctx, []string{ns.GetName()})
				if err != nil {
					log.Log.Error(err, "failed to ensure DBEngines in namespace", "namespace", ns.GetName())
				}
				return requests
			}),
		)
	}

	if r.isOLMInstalled(context.Background()) {
		err := opfwv1alpha1.AddToScheme(r.Scheme)
		if err != nil {
			return err
		}
		c.Watches(
			&opfwv1alpha1.InstallPlan{},
			handler.EnqueueRequestsFromMapFunc(getDatabaseEngineRequestsFromInstallPlan),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		)
		c.Watches(
			&opfwv1alpha1.ClusterServiceVersion{},
			handler.EnqueueRequestsFromMapFunc(getDatabaseEngineRequestsFromCSV),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		)
	}
	c.WithEventFilter(common.DefaultNamespaceFilter)

	return c.Complete(r)
}

func (r *DatabaseEngineReconciler) isOLMInstalled(ctx context.Context) bool {
	unstructuredResource := &unstructured.Unstructured{}
	unstructuredResource.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "apiextensions.k8s.io",
		Kind:    "CustomResourceDefinition",
		Version: "v1",
	})
	if err := r.Get(
		ctx,
		types.NamespacedName{Name: "subscriptions.operators.coreos.com"},
		unstructuredResource); err == nil {
		return true
	}
	return false
}

func getDatabaseEngineRequestsFromCSV(_ context.Context, o client.Object) []reconcile.Request {
	result := []reconcile.Request{}
	csv, ok := o.(*opfwv1alpha1.ClusterServiceVersion)
	if !ok {
		return result
	}

	dbEngineName, _ := parseOperatorCSVName(csv.GetName())
	result = append(result, reconcile.Request{NamespacedName: types.NamespacedName{
		Name:      dbEngineName,
		Namespace: csv.GetNamespace(),
	}})
	return result
}

// getDatabaseEngineRequestsFromInstallPlan returns a list of reconcile.Request for each possible
// databaseengine referenced by an InstallPlan.
func getDatabaseEngineRequestsFromInstallPlan(_ context.Context, o client.Object) []reconcile.Request {
	result := []reconcile.Request{}
	installPlan, ok := o.(*opfwv1alpha1.InstallPlan)
	if !ok {
		return result
	}

	for _, csv := range installPlan.Spec.ClusterServiceVersionNames {
		dbEngineName, _ := parseOperatorCSVName(csv)
		result = append(result, reconcile.Request{NamespacedName: types.NamespacedName{
			Name:      dbEngineName,
			Namespace: installPlan.GetNamespace(),
		}})
	}
	return result
}

// parseOperatorCSVName parses the CSV name to extract the operator name and version.
// Example:
//   - input: "percona-xtradb-cluster-operator.v1.9.0"
//     output: "percona-xtradb-cluster-operator", "1.9.0"
func parseOperatorCSVName(csvName string) (string, string) {
	// Regex for matching the version part of the CSV name
	pattern := `.v\d+\.\d+\.\d+`
	regex := regexp.MustCompile(pattern)
	matchIndex := regex.FindStringIndex(csvName)
	if matchIndex != nil {
		split := matchIndex[0]
		return csvName[:split], csvName[split+2:]
	}
	return "", ""
}
