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
	"net/url"
	"slices"

	"github.com/AlekSi/pointer"
	vmv1beta1 "github.com/VictoriaMetrics/operator/api/operator/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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

const vmAgentResourceName = "everest-monitoring"

// MonitoringConfigReconciler reconciles a MonitoringConfig object.
type MonitoringConfigReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	monitoringNamespace string
}

// +kubebuilder:rbac:groups=everest.percona.com,resources=monitoringconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=everest.percona.com,resources=monitoringconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=everest.percona.com,resources=databaseclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=operator.victoriametrics.com,resources=vmagents,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// Modify the Reconcile function to compare the state specified by
// the DatabaseClusterBackup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *MonitoringConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (rr ctrl.Result, rerr error) { //nolint:nonamedreturns
	logger := log.FromContext(ctx)
	logger.Info("Reconciling")
	defer func() {
		logger.Info("Reconciled")
	}()

	mc := &everestv1alpha1.MonitoringConfig{}
	if err := r.Get(ctx, req.NamespacedName, mc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Check if the MonitoringConfig is in use.
	mcName := mc.GetName()
	dbList, err := common.DatabaseClustersThatReferenceObject(ctx, r.Client, monitoringConfigNameField, req.Namespace, req.Name)
	if err != nil {
		fetchErr := fmt.Errorf("failed to fetch DB clusters that use monitoring config='%s'", mcName)
		logger.Error(err, fetchErr.Error())
		return ctrl.Result{}, errors.Join(err, fetchErr)
	}

	credentialsSecret := &corev1.Secret{}

	// Update the status and finalizers of the MonitoringConfig object after the reconciliation.
	defer func() {
		// Nothing to process on delete events
		if !mc.GetDeletionTimestamp().IsZero() {
			return
		}

		mc.Status.InUse = len(dbList.Items) > 0
		mc.Status.LastObservedGeneration = mc.GetGeneration()
		v, vErr := mc.GetPMMServerVersion(ctx, credentialsSecret)
		if vErr != nil {
			logger.Error(err, "Failed to get PMM server version "+vErr.Error())
		}
		mc.Status.PMMServerVersion = v

		if err = r.Client.Status().Update(ctx, mc); err != nil {
			rr = ctrl.Result{}
			logger.Error(err, fmt.Sprintf("failed to update status for monitoring config='%s'", mcName))
			rerr = errors.Join(err, fmt.Errorf("failed to update status for monitoring config='%s': %w", mcName, err))
		}
	}()

	if err = common.EnsureInUseFinalizer(ctx, r.Client, len(dbList.Items) > 0, mc); err != nil {
		logger.Error(err, fmt.Sprintf("failed to update finalizers for monitoring config='%s'", mcName))
		return ctrl.Result{}, errors.Join(err, fmt.Errorf("failed to update finalizers for monitoring config='%s': %w", mcName, err))
	}

	if !mc.GetDeletionTimestamp().IsZero() {
		// reconcile VMAgent so that it can be cleaned-up.
		logger.Info("Reconciling VMAgent")
		defer func() {
			logger.Info("Reconciled VMAgent")
		}()
		if err := r.reconcileVMAgent(ctx); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.cleanupSecrets(ctx, mc)
	}

	if err := r.Get(ctx, types.NamespacedName{
		Name:      mc.Spec.CredentialsSecretName,
		Namespace: mc.GetNamespace(),
	}, credentialsSecret); err != nil {
		logger.Error(err, "unable to fetch Secret")
		return ctrl.Result{}, err
	}

	if metav1.GetControllerOf(credentialsSecret) == nil {
		logger.Info("setting controller references for the secret")
		if err := controllerutil.SetControllerReference(mc, credentialsSecret, r.Client.Scheme()); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Update(ctx, credentialsSecret); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.Info("Reconciling VMAgent")
	defer func() {
		logger.Info("Reconciled VMAgent")
	}()
	if err := r.reconcileVMAgent(ctx); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *MonitoringConfigReconciler) reconcileVMAgent(ctx context.Context) error {
	monitoringConfigList := &everestv1alpha1.MonitoringConfigList{}
	err := r.List(ctx, monitoringConfigList, &client.ListOptions{})
	if err != nil {
		return errors.Join(err, errors.New("could not list monitoringconfigs"))
	}

	vmAgentSpec, err := r.genVMAgentSpec(ctx, monitoringConfigList)
	if err != nil {
		return err
	}

	vmAgent := &vmv1beta1.VMAgent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmAgentResourceName,
			Namespace: r.monitoringNamespace,
		},
	}

	// No remote writes, delete the VMAgent.
	if len(vmAgentSpec.RemoteWrite) == 0 {
		if err := r.Delete(ctx, vmAgent); client.IgnoreNotFound(err) != nil {
			return errors.Join(err, errors.New("could not delete vmagent"))
		}
		return nil
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, vmAgent, func() error {
		vmAgent.SetLabels(map[string]string{
			"app.kubernetes.io/managed-by": "everest",
			"everest.percona.com/type":     "monitoring",
		})
		vmAgent.Spec = vmAgentSpec
		return nil
	})
	return err
}

func (r *MonitoringConfigReconciler) genVMAgentSpec(ctx context.Context, monitoringConfigList *everestv1alpha1.MonitoringConfigList) (vmv1beta1.VMAgentSpec, error) {
	spec := vmv1beta1.VMAgentSpec{
		SelectAllByDefault: true,
		CommonApplicationDeploymentParams: vmv1beta1.CommonApplicationDeploymentParams{
			ExtraArgs: map[string]string{
				"memory.allowedPercent": "40",
			},
		},
		CommonDefaultableParams: vmv1beta1.CommonDefaultableParams{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("250m"),
					corev1.ResourceMemory: resource.MustParse("350Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("850Mi"),
				},
			},
		},
	}

	remoteWrites := make([]vmv1beta1.VMAgentRemoteWriteSpec, 0, len(monitoringConfigList.Items))
	for _, monitoringConfig := range monitoringConfigList.Items {
		if monitoringConfig.Spec.Type != everestv1alpha1.PMMMonitoringType {
			continue
		}

		// Skip the MonitoringConfig if it is being deleted.
		// Remove the vmagent finalizer.
		if !monitoringConfig.GetDeletionTimestamp().IsZero() {
			if removed := controllerutil.RemoveFinalizer(&monitoringConfig, consts.MonitoringConfigVMAgentFinalizer); removed {
				if err := r.Update(ctx, &monitoringConfig); err != nil {
					return spec, errors.Join(err, errors.New("could not remove finalizer"))
				}
			}
			continue
		}

		// This MonitoringConfig is a part of the vmagent.
		// Add the finalizer so we can clean up the vmagent when the MonitoringConfig is deleted.
		if updated := controllerutil.AddFinalizer(&monitoringConfig, consts.MonitoringConfigVMAgentFinalizer); updated {
			if err := r.Update(ctx, &monitoringConfig); err != nil {
				return spec, errors.Join(err, errors.New("could not add finalizer"))
			}
		}

		secretName, err := r.reconcileSecret(ctx, &monitoringConfig)
		if err != nil {
			return spec, errors.Join(err, errors.New("could not reconcile destination secret"))
		}

		u, err := url.Parse(monitoringConfig.Spec.PMM.URL)
		if err != nil {
			return spec, errors.Join(err, errors.New("failed to parse PMM URL"))
		}
		url := u.JoinPath("victoriametrics/api/v1/write").String()

		// Check if this URL exists.
		if idx := slices.IndexFunc(remoteWrites, func(rw vmv1beta1.VMAgentRemoteWriteSpec) bool {
			return rw.URL == url
		}); idx >= 0 {
			// already exists.
			continue
		}

		skipTLS := false
		if monitoringConfig.Spec.VerifyTLS != nil {
			skipTLS = !*monitoringConfig.Spec.VerifyTLS
		}
		remoteWrites = append(remoteWrites, vmv1beta1.VMAgentRemoteWriteSpec{
			BasicAuth: &vmv1beta1.BasicAuth{
				Password: corev1.SecretKeySelector{
					Key: everestv1alpha1.MonitoringConfigCredentialsSecretAPIKeyKey,
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secretName,
					},
				},
				Username: corev1.SecretKeySelector{
					Key: everestv1alpha1.MonitoringConfigCredentialsSecretUsernameKey,
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secretName,
					},
				},
			},
			TLSConfig: &vmv1beta1.TLSConfig{
				InsecureSkipVerify: skipTLS,
			},
			URL: url,
		})
	}
	spec.RemoteWrite = remoteWrites

	// Use the kube-system namespace ID as the k8s_cluster_id label.
	kubeSystemNamespace := &corev1.Namespace{}
	err := r.Get(ctx, types.NamespacedName{Name: "kube-system"}, kubeSystemNamespace)
	if err != nil {
		return spec, errors.Join(err, errors.New("could not get kube-system namespace"))
	}
	spec.ExternalLabels = map[string]string{
		"k8s_cluster_id": string(kubeSystemNamespace.UID),
	}
	return spec, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MonitoringConfigReconciler) SetupWithManager(mgr ctrl.Manager, monitoringNamespace string) error {
	if err := r.initIndexers(context.Background(), mgr); err != nil {
		return err
	}

	// Predicate to trigger reconciliation only on .spec.monitoring.monitoringConfigName changes in the DatabaseCluster resource.
	dbClusterEventsPredicate := predicate.Funcs{
		// Allow create events only if the .spec.monitoring.monitoringConfigName is set
		CreateFunc: func(e event.CreateEvent) bool {
			db, ok := e.Object.(*everestv1alpha1.DatabaseCluster)
			if !ok {
				return false
			}
			return pointer.Get(db.Spec.Monitoring).MonitoringConfigName != ""
		},

		// Only allow updates when the .spec.monitoring.monitoringConfigName of the DatabaseCluster resource changes
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldDB, oldOk := e.ObjectOld.(*everestv1alpha1.DatabaseCluster)
			newDB, newOk := e.ObjectNew.(*everestv1alpha1.DatabaseCluster)
			if !oldOk || !newOk {
				return false
			}

			// Trigger reconciliation only if the .spec.monitoring.monitoringConfigName field has changed
			return pointer.Get(oldDB.Spec.Monitoring).MonitoringConfigName !=
				pointer.Get(newDB.Spec.Monitoring).MonitoringConfigName
		},

		// Allow delete events only if the .spec.monitoring.monitoringConfigName is set
		DeleteFunc: func(e event.DeleteEvent) bool {
			db, ok := e.Object.(*everestv1alpha1.DatabaseCluster)
			if !ok {
				return false
			}
			return pointer.Get(db.Spec.Monitoring).MonitoringConfigName != ""
		},

		// Nothing to process on generic events
		GenericFunc: func(_ event.GenericEvent) bool {
			return false
		},
	}
	r.monitoringNamespace = monitoringNamespace
	return ctrl.NewControllerManagedBy(mgr).
		Named("MonitoringConfig").
		For(&everestv1alpha1.MonitoringConfig{}).
		Watches(&vmv1beta1.VMAgent{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueMonitoringConfigs),
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Namespace{},
			common.EnqueueObjectsInNamespace(r.Client, &everestv1alpha1.MonitoringConfigList{})).
		// need to watch DBClusters that reference MonitoringConfig to update the status.
		Watches(
			&everestv1alpha1.DatabaseCluster{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				db, ok := obj.(*everestv1alpha1.DatabaseCluster)
				if !ok {
					return []reconcile.Request{}
				}

				if pointer.Get(db.Spec.Monitoring).MonitoringConfigName == "" {
					// No monitoringConfigName specified, no need to enqueue
					return []reconcile.Request{}
				}
				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      db.Spec.Monitoring.MonitoringConfigName,
							Namespace: db.GetNamespace(),
						},
					},
				}
			}),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}, dbClusterEventsPredicate),
		).
		WithEventFilter(common.DefaultNamespaceFilter).
		Complete(r)
}

func (r *MonitoringConfigReconciler) initIndexers(ctx context.Context, mgr ctrl.Manager) error {
	// Index the credentialsSecretName field in MonitoringConfig.
	err := mgr.GetFieldIndexer().IndexField(
		ctx, &everestv1alpha1.MonitoringConfig{}, monitoringConfigSecretNameField,
		func(o client.Object) []string {
			var res []string
			mc, ok := o.(*everestv1alpha1.MonitoringConfig)
			if !ok {
				return res
			}
			res = append(res, mc.Spec.CredentialsSecretName)
			return res
		},
	)

	return err
}

// enqueueMonitoringConfigs enqueues MonitoringConfig objects for reconciliation when a VMAgent is created/updated/deleted.
func (r *MonitoringConfigReconciler) enqueueMonitoringConfigs(ctx context.Context, o client.Object) []reconcile.Request {
	vmAgent, ok := o.(*vmv1beta1.VMAgent)
	if !ok {
		return nil
	}
	if vmAgent.GetNamespace() != r.monitoringNamespace {
		return nil
	}

	list := &everestv1alpha1.MonitoringConfigList{}
	err := r.List(ctx, list)
	if err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, mc := range list.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      mc.GetName(),
				Namespace: mc.GetNamespace(),
			},
		})
	}
	return requests
}

// cleanupSecrets deletes all secrets in the monitoring namespace that belong to the given MonitoringConfig.
func (r *MonitoringConfigReconciler) cleanupSecrets(ctx context.Context, mc *everestv1alpha1.MonitoringConfig) error {
	// List secrets in the monitoring namespace that belong to this MonitoringConfig.
	secrets := &corev1.SecretList{}
	err := r.List(ctx, secrets, &client.ListOptions{
		Namespace: r.monitoringNamespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			consts.MonitoringConfigRefNameLabel:      mc.GetName(),
			consts.MonitoringConfigRefNamespaceLabel: mc.GetNamespace(),
		}),
	})
	if err != nil {
		return err
	}
	for _, secret := range secrets.Items {
		if err := r.Delete(ctx, &secret); err != nil {
			return err
		}
	}
	// Remove the finalizer from the MonitoringConfig.
	if controllerutil.RemoveFinalizer(mc, consts.MonitoringConfigSecretCleanupFinalizer) {
		return r.Update(ctx, mc)
	}
	return nil
}

// reconcileSecret copies the source MonitoringConfig secret onto the monitoring namespace.
// Returns the name of the newly created/updated secret.
func (r *MonitoringConfigReconciler) reconcileSecret(ctx context.Context, mc *everestv1alpha1.MonitoringConfig) (string, error) {
	// If the MonitoringConfig is in the monitoring namespace, we don't have to do any additional work.
	// Use the secret as-is.
	if mc.GetNamespace() == r.monitoringNamespace {
		return mc.Spec.CredentialsSecretName, nil
	}

	// Get the secret in the MonitoringConfig namespace.
	src := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      mc.Spec.CredentialsSecretName,
		Namespace: mc.GetNamespace(),
	}, src)
	if err != nil {
		return "", err
	}

	// Create a copy in the monitoring namespace.
	dstSecretName := src.GetName() + "-" + src.GetNamespace()
	dst := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dstSecretName,
			Namespace: r.monitoringNamespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, dst, func() error {
		dst.Data = src.DeepCopy().Data
		// Attach labels to identify the parent MonitoringConfig.
		// This is useful for garbage collection, since we cannot have cross-namespace OwnerRefs.
		labels := dst.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[consts.MonitoringConfigRefNameLabel] = mc.GetName()
		labels[consts.MonitoringConfigRefNamespaceLabel] = mc.GetNamespace()
		dst.SetLabels(labels)
		return nil
	}); err != nil {
		return "", err
	}
	// Add a clean-up finalizer in the parent MonitoringConfig.
	if controllerutil.AddFinalizer(mc, consts.MonitoringConfigSecretCleanupFinalizer) {
		return dstSecretName, r.Update(ctx, mc)
	}
	return dstSecretName, nil
}
