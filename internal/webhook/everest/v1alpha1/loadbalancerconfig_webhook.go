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

// Package v1alpha1 ...
//
//nolint:lll
package v1alpha1

import (
	"context"
	"fmt"
	"reflect"
	"regexp"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	fielderrors "k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/utils"
)

var (
	// Default configs errors.
	errUpdateDefaultLBC = func(name string) error {
		return fmt.Errorf("load balancer config with name='%s' is default and cannot be updated", name)
	}
	errDeleteDefaultLBC = func(name string) error {
		return fmt.Errorf("load balancer config with name='%s' is default and cannot be deleted", name)
	}
	// Used config error.
	errDeleteInUseLBC = func(name string) error {
		return fmt.Errorf("load balancer config with name='%s' is used by some DB cluster and cannot be deleted", name)
	}

	errUnexpectedObject = func(obj runtime.Object) error {
		return fmt.Errorf("expected a LoadBalancerConfig, got %T", obj)
	}
)

// SetupLoadBalancerConfigWebhookWithManager sets up the webhook with the manager.
func SetupLoadBalancerConfigWebhookWithManager(mgr manager.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&everestv1alpha1.LoadBalancerConfig{}).
		WithValidator(&LoadBalancerConfigValidator{
			Client: mgr.GetClient(),
		}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-everest-percona-com-v1alpha1-loadbalancerconfig,mutating=false,failurePolicy=fail,sideEffects=None,groups=everest.percona.com,resources=loadbalancerconfigs,verbs=create;update;delete,versions=v1alpha1,name=vloadbalancerconfig-v1alpha1.everest.percona.com,admissionReviewVersions=v1

// LoadBalancerConfigValidator validates the LoadBalancerConfig resource.
type LoadBalancerConfigValidator struct {
	Client client.Client
}

// ValidateCreate validates the creation of a DatabaseCluster.
func (v *LoadBalancerConfigValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	lbc, ok := obj.(*everestv1alpha1.LoadBalancerConfig)
	if !ok {
		return nil, errUnexpectedObject(obj)
	}

	return nil, v.validateLoadBalancerConfig(ctx, lbc)
}

// ValidateUpdate validates the update of a DatabaseCluster.
func (v *LoadBalancerConfigValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldLbc, ok := oldObj.(*everestv1alpha1.LoadBalancerConfig)
	if !ok {
		return nil, errUnexpectedObject(oldObj)
	}

	newLbc, ok := newObj.(*everestv1alpha1.LoadBalancerConfig)
	if !ok {
		return nil, errUnexpectedObject(newObj)
	}

	if utils.IsEverestReadOnlyObject(oldLbc) && !reflect.DeepEqual(oldLbc.Spec, newLbc.Spec) {
		// default config update is not allowed
		return nil, errUpdateDefaultLBC(newLbc.Name)
	}

	return nil, v.validateLoadBalancerConfig(ctx, newLbc)
}

// ValidateDelete validates the deletion of a DatabaseCluster.
func (v *LoadBalancerConfigValidator) ValidateDelete(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	lbc, ok := obj.(*everestv1alpha1.LoadBalancerConfig)
	if !ok {
		return nil, errUnexpectedObject(obj)
	}

	if utils.IsEverestReadOnlyObject(lbc) {
		// default config deletion is not allowed
		return nil, errDeleteDefaultLBC(lbc.Name)
	}

	if utils.IsEverestObjectInUse(lbc) {
		// config is used by some DB cluster
		return nil, errDeleteInUseLBC(lbc.Name)
	}

	return nil, nil
}

var annotationKeyRegex = regexp.MustCompile(`^(?:(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?/)?([a-z0-9](?:[a-z0-9_.-]{0,61}[a-z0-9])?)$`) //nolint:lll

func (v *LoadBalancerConfigValidator) validateLoadBalancerConfig(_ context.Context, lbc *everestv1alpha1.LoadBalancerConfig) error {
	if err := utils.ValidateRFC1035(lbc.GetName(), "metadata.name"); err != nil {
		return err
	}

	if !lbc.DeletionTimestamp.IsZero() {
		return nil
	}

	var allErrs []error

	for key := range lbc.Spec.Annotations {
		if !annotationKeyRegex.MatchString(key) {
			allErrs = append(allErrs, fmt.Errorf("invalid annotation key: %s", key))
		}
	}

	if len(allErrs) > 0 {
		return apierrors.NewInvalid(
			everestv1alpha1.GroupVersion.WithKind(consts.LoadBalancerConfigKind).GroupKind(),
			lbc.Name,
			toAggregateFieldErrors(allErrs),
		)
	}

	return nil
}

func toAggregateFieldErrors(errs []error) fielderrors.ErrorList {
	fieldErrList := fielderrors.ErrorList{}
	for _, err := range errs {
		fieldErrList = append(fieldErrList, &fielderrors.Error{
			Type:     fielderrors.ErrorTypeInvalid,
			Field:    "spec.annotations",
			BadValue: err.Error(),
		})
	}

	return fieldErrList
}
