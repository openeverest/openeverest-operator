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

// Package v1alpha1 contains a set of WebHooks for the enginefeatures.everest.percona.com API group
package v1alpha1

import (
	"context"
	"encoding/base64"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	enginefeatureseverestv1alpha1 "github.com/percona/everest-operator/api/enginefeatures.everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

// errors for SplitHorizonDNSConfig defaulter webhook.
var (
	// .spec.tls.secretName errors.
	secretNamePath = field.NewPath("spec", "tls", "secretName")
)

var (
	groupKind                         = enginefeatureseverestv1alpha1.GroupVersion.WithKind(consts.SplitHorizonDNSConfigKind).GroupKind()
	_         webhook.CustomDefaulter = &SplitHorizonDNSConfigDefaulter{}
)

// SetupSplitHorizonDNSConfigMutationWebhookWithManager registers the mutation webhook for SplitHorizonDNSConfig in the manager.
func SetupSplitHorizonDNSConfigMutationWebhookWithManager(mgr manager.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{}).
		WithDefaulter(&SplitHorizonDNSConfigDefaulter{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		}).
		Complete()
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
//nolint:lll
// +kubebuilder:webhook:path=/mutate-enginefeatures-everest-percona-com-v1alpha1-splithorizondnsconfig,mutating=true,failurePolicy=fail,sideEffects=None,groups=enginefeatures.everest.percona.com,resources=splithorizondnsconfigs,verbs=create;update,versions=v1alpha1,name=msplithorizondnsconfig-v1alpha1.enginefeatures.everest.percona.com,admissionReviewVersions=v1
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=create;update;get;list
// +kubebuilder:rbac:groups=enginefeatures.everest.percona.com,resources=splithorizondnsconfigs,verbs=get;list;watch;update

// SplitHorizonDNSConfigDefaulter struct is responsible for mutating the SplitHorizonDNSConfig resource.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type SplitHorizonDNSConfigDefaulter struct {
	Client client.Client
	Scheme *runtime.Scheme
}

// Default implements a mutating webhook for SplitHorizonDNSConfig resources.
// The main goal of this func - copy certificate data (if provided) into secret (secretName) and reset certificate.
func (d *SplitHorizonDNSConfigDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	shdc, ok := obj.(*enginefeatureseverestv1alpha1.SplitHorizonDNSConfig)
	if !ok {
		return fmt.Errorf("expected a SplitHorizonDNSConfig object but got %T", obj)
	}

	logger := logf.FromContext(ctx).WithName("SplitHorizonDNSConfigDefaulter").WithValues(
		"name", shdc.GetName(),
		"namespace", shdc.GetNamespace(),
	)

	logger.Info("Mutating SplitHorizonDNSConfig")

	// validate some fields
	var allErrs field.ErrorList
	secretName := shdc.Spec.TLS.SecretName
	if secretName == "" {
		// secretName is mandatory
		allErrs = append(allErrs, errRequiredField(secretNamePath))
		return apierrors.NewInvalid(groupKind, shdc.GetName(), allErrs)
	}

	if shdc.Spec.TLS.Certificate != nil {
		// Both Certificate and SecretName are provided.
		// Certificate fields must be non-empty.
		// Secret content is not checked, as Certificate is provided.
		if errs := validateCertificate(shdc.Spec.TLS.Certificate); errs != nil {
			allErrs = append(allErrs, errs...)
			return apierrors.NewInvalid(groupKind, shdc.GetName(), allErrs)
		}
	} else {
		// certificate is not provided - nothing to do
		return nil
	}

	// Move TLS certificate from the spec to a secret.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shdc.Spec.TLS.SecretName,
			Namespace: shdc.GetNamespace(),
		},
	}

	var op controllerutil.OperationResult
	var err error
	// errors are checked during validateCertificate func call
	tlsKeyDec, _ := base64.StdEncoding.DecodeString(shdc.Spec.TLS.Certificate.CAKey)
	caCertDec, _ := base64.StdEncoding.DecodeString(shdc.Spec.TLS.Certificate.CACert)

	if op, err = controllerutil.CreateOrUpdate(ctx, d.Client, secret, func() error {
		secret.StringData = map[string]string{
			"ca.key": string(tlsKeyDec),
			"ca.crt": string(caCertDec),
		}
		secret.Type = corev1.SecretTypeOpaque
		return nil
	}); err != nil {
		return fmt.Errorf("failed to create or update TLS certificate secret: %w", err)
	}

	if op == controllerutil.OperationResultCreated {
		// The secret was absent - need to set finalizer to clean up secret once
		// SplitHorizonDNSConfig resource deletion is requested.
		controllerutil.AddFinalizer(shdc, consts.EngineFeaturesSplitHorizonDNSConfigSecretCleanupFinalizer)
	}

	// Reset Certificate in the spec to avoid storing sensitive data in the CR.
	shdc.Spec.TLS.Certificate = nil
	return nil
}
