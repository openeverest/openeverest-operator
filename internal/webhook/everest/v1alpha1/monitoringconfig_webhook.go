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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// SetupMonitoringConfigWebhookWithManager sets up the webhook with the manager.
func SetupMonitoringConfigWebhookWithManager(mgr manager.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&everestv1alpha1.MonitoringConfig{}).
		WithValidator(&MonitoringConfigValidator{
			Client:    mgr.GetClient(),
			apiReader: mgr.GetAPIReader(),
		}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-everest-percona-com-v1alpha1-monitoringconfig,mutating=false,failurePolicy=fail,sideEffects=None,groups=everest.percona.com,resources=monitoringconfigs,verbs=create;update,versions=v1alpha1,name=vmonitoringconfig-v1alpha1.everest.percona.com,admissionReviewVersions=v1

// MonitoringConfigValidator validates the MonitoringConfig resource.
type MonitoringConfigValidator struct {
	Client client.Client
	// apiReader bypasses the cache and directly reads from the API server.
	apiReader client.Reader
}

// ValidateCreate validates the creation of a DatabaseCluster.
func (v *MonitoringConfigValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, v.validateMonitoringConfig(ctx, obj)
}

// ValidateUpdate validates the update of a DatabaseCluster.
func (v *MonitoringConfigValidator) ValidateUpdate(ctx context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	return nil, v.validateMonitoringConfig(ctx, newObj)
}

// ValidateDelete validates the deletion of a DatabaseCluster.
func (v *MonitoringConfigValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (v *MonitoringConfigValidator) validateMonitoringConfig(ctx context.Context, obj runtime.Object) error {
	m, ok := obj.(*everestv1alpha1.MonitoringConfig)
	if !ok {
		return fmt.Errorf("expected a MonitoringConfig, got %T", obj)
	}

	if !m.DeletionTimestamp.IsZero() {
		return nil
	}

	secretName := m.Spec.CredentialsSecretName
	if secretName == "" {
		return errors.New("missing secret name")
	}

	// Ensure that the secret exists.
	// We read from the API server directly. This is because the webhook
	// may be called before the cached client has had the chance to sync
	// with the API server.
	secret := corev1.Secret{}
	if err := v.apiReader.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: m.GetNamespace(),
	}, &secret); err != nil {
		return fmt.Errorf("failed to get secret %s: %w", secretName, err)
	}

	apiKey, ok := secret.Data["apiKey"]
	if !ok {
		return fmt.Errorf("missing api key in the secret %s", secretName)
	}

	var insecure bool
	if m.Spec.VerifyTLS != nil {
		insecure = !*m.Spec.VerifyTLS
	}

	return checkAccess(ctx, m.Spec.PMM.URL, string(apiKey), insecure)
}

func checkAccess(ctx context.Context, url, apiKey string, insecure bool) error {
	debug := os.Getenv("DEPLOY_TYPE")
	if debug == "dev" {
		return nil
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		url+"/v1/version",
		nil,
	)
	if err != nil {
		return err
	}

	req.Close = true
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	httpClient := newHTTPClient(insecure)

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close() //nolint:errcheck

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusUnauthorized:
		return errors.New("authorization failed, please provide the correct credentials")
	default:
		var pmmErr *pmmErrorMessage

		if err := json.Unmarshal(data, &pmmErr); err != nil {
			return errors.Join(err, fmt.Errorf("PMM returned an unknown error. HTTP status code %d", resp.StatusCode))
		}

		return fmt.Errorf("PMM returned an error with message: %s", pmmErr.Message)
	}
}

func newHTTPClient(insecure bool) *http.Client {
	client := http.DefaultClient
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: insecure, //nolint:gosec
		},
	}

	return client
}

type pmmErrorMessage struct {
	Message string `json:"message"`
}
