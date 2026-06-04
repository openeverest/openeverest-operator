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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

// SecretReconciler reconciles a Secret object.
type SecretReconciler struct {
	client.Client

	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *SecretReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	secret := &corev1.Secret{}
	if err := r.Get(ctx, req.NamespacedName, secret); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "unable to fetch Secret")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Check if the Secret has the cleanup annotation
	cleanupTimeStr, ok := secret.Annotations[consts.CleanupAfterAnnotation]
	if !ok {
		return ctrl.Result{}, nil
	}

	// If the Secret has an OwnerReference, it's not orphaned, no need to clean up
	if controller := metav1.GetControllerOf(secret); controller != nil {
		// Optionally we could strip the annotation here, but it's handled by BackupStorage controller
		return ctrl.Result{}, nil
	}

	cleanupTime, err := time.Parse(time.RFC3339, cleanupTimeStr)
	if err != nil {
		logger.Error(err, "invalid cleanup time format, ignoring", "annotation", consts.CleanupAfterAnnotation, "value", cleanupTimeStr)
		// Strip the invalid annotation so we don't keep failing
		delete(secret.Annotations, consts.CleanupAfterAnnotation)
		if updateErr := r.Update(ctx, secret); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}

	if time.Now().After(cleanupTime) {
		logger.Info("Deleting orphaned Secret", "secret", secret.Name)
		if err := r.Delete(ctx, secret); client.IgnoreNotFound(err) != nil {
			logger.Error(err, "unable to delete orphaned Secret")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Requeue until the cleanup time
	return ctrl.Result{RequeueAfter: time.Until(cleanupTime)}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SecretReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("Secret").
		For(&corev1.Secret{}).
		WithEventFilter(common.DefaultNamespaceFilter).
		Complete(r)
}
