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

package v1alpha1

import (
	"context"
	"fmt"
	"net/http"

	"github.com/AlekSi/pointer"
	goversion "github.com/hashicorp/go-version"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MonitoringType is a type of monitoring.
type MonitoringType string

const (
	// PMMMonitoringType represents monitoring via PMM.
	PMMMonitoringType MonitoringType = "pmm"
	// MonitoringConfigCredentialsSecretUsernameKey is the credentials secret's key that contains the username.
	MonitoringConfigCredentialsSecretUsernameKey = "username"
	// MonitoringConfigCredentialsSecretAPIKeyKey is the credentials secret's key that contains the API key.
	MonitoringConfigCredentialsSecretAPIKeyKey = "apiKey"

	// PMM2ClientImage is the image for PMM2 client.
	PMM2ClientImage = "percona/pmm-client:2"
	// PMM3ClientImage is the image for PMM3 client.
	PMM3ClientImage = "percona/pmm-client:3"
)

var pmmServerKeys = map[EngineType]pmmKeyPair{
	DatabaseEnginePSMDB:      {"PMM_SERVER_API_KEY", "PMM_SERVER_TOKEN"},
	DatabaseEnginePostgresql: {"PMM_SERVER_KEY", "PMM_SERVER_TOKEN"},
	DatabaseEnginePXC:        {"pmmserverkey", "pmmservertoken"},
}

type pmmKeyPair struct {
	Legacy string
	New    string
}

// MonitoringConfigSpec defines the desired state of MonitoringConfig.
type MonitoringConfigSpec struct {
	// Type is type of monitoring.
	// +kubebuilder:validation:Enum=pmm
	Type MonitoringType `json:"type"`
	// CredentialsSecretName is the name of the secret with credentials.
	CredentialsSecretName string `json:"credentialsSecretName"`
	// AllowedNamespaces is the list of namespaces where the operator will copy secrets provided in the CredentialsSecretsName.
	AllowedNamespaces []string `json:"allowedNamespaces,omitempty"`
	// PMM is configuration for the PMM monitoring type.
	PMM PMMConfig `json:"pmm,omitempty"`
	// VerifyTLS is set to ensure TLS/SSL verification.
	// If unspecified, the default value is true.
	//
	// +kubebuilder:default:=true
	VerifyTLS *bool `json:"verifyTLS,omitempty"`
}

// PMMConfig is configuration of the PMM monitoring type.
type PMMConfig struct {
	// URL is url to the monitoring instance.
	URL string `json:"url"`
	// Image is a Docker image name to use for deploying PMM client. Defaults to using the latest version.
	Image string `json:"image"`
}

// MonitoringConfigStatus defines the observed state of MonitoringConfig.
type MonitoringConfigStatus struct {
	// InUse is a flag that indicates if any DB cluster uses the monitoring config.
	// +kubebuilder:default=false
	InUse bool `json:"inUse,omitempty"`
	// LastObservedGeneration is the most recent generation observed for this MonitoringConfig.
	LastObservedGeneration int64 `json:"lastObservedGeneration,omitempty"`
	// PMMServerVersion shows PMM server version
	PMMServerVersion PMMServerVersion `json:"pmmServerVersion,omitempty"`
}

// CreatePMMApiKey creates a new API key in PMM by using the provided username and password.
func (c *PMMConfig) CreatePMMApiKey(
	ctx context.Context,
	apiKeyName, user, password string,
	skipTLSVerify bool,
) (string, error) {
	auth := basicAuth{
		user:     user,
		password: password,
	}
	version, err := c.getPMMVersion(ctx, auth, skipTLSVerify)
	if err != nil {
		return "", err
	}

	// PMM2 and PMM3 use different API to create tokens
	if version.UsesLegacyAuth() {
		return createKey(ctx, c.URL, apiKeyName, auth, skipTLSVerify)
	} else {
		return createServiceAccountAndToken(ctx, c.URL, apiKeyName, auth, skipTLSVerify)
	}
}

func (m *MonitoringConfig) GetPMMServerVersion(ctx context.Context, credentialsSecret *corev1.Secret) (PMMServerVersion, error) {
	if key := m.Spec.PMM.getPMMKey(credentialsSecret); key != "" {
		skipVerifyTLS := !pointer.Get(m.Spec.VerifyTLS)
		return m.Spec.PMM.getPMMVersion(ctx, bearerAuth{token: key}, skipVerifyTLS)
	}
	return "", nil
}

// getPMMKey finds the PMM key in the given secret
func (c *PMMConfig) getPMMKey(secret *corev1.Secret) string {
	if secret == nil || len(secret.Data) == 0 {
		return ""
	}
	// check for all engines, first the legacy keys, then the new case
	for _, pair := range pmmServerKeys {
		if val, ok := secret.Data[pair.Legacy]; ok {
			return string(val)
		}
		if val, ok := secret.Data[pair.New]; ok {
			return string(val)
		}
	}
	return ""
}

// getPMMVersion makes an API request to the PMM server to figure out the current version
func (c *PMMConfig) getPMMVersion(ctx context.Context, auth iAuth, skipTLSVerify bool) (PMMServerVersion, error) {
	resp, err := doJSONRequest[struct {
		Version string `json:"version"`
	}](ctx, http.MethodGet, fmt.Sprintf("%s/v1/version", c.URL), auth, "", skipTLSVerify)
	if err != nil {
		return "", err
	}

	return PMMServerVersion(resp.Version), nil
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Type",type="string",JSONPath=".spec.type",description="Monitoring instance type"
// +kubebuilder:printcolumn:name="InUse",type="string",JSONPath=".status.inUse",description="Indicates if any DB cluster uses the monitoring config"

// MonitoringConfig is the Schema for the monitoringconfigs API.
type MonitoringConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec MonitoringConfigSpec `json:"spec,omitempty"`
	// +kubebuilder:default={"inUse": false}
	Status MonitoringConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MonitoringConfigList contains a list of MonitoringConfig.
type MonitoringConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MonitoringConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MonitoringConfig{}, &MonitoringConfigList{})
}

type PMMServerVersion string

// UsesLegacyAuth returns true if the instance uses legacy auth (PMM2) otherwise it returns false
func (v *PMMServerVersion) UsesLegacyAuth() bool {
	ver, err := goversion.NewVersion(string(*v))
	if err != nil {
		return false
	}
	segments := ver.Segments()
	return len(segments) > 0 && segments[0] == 2
}

func (v *PMMServerVersion) DefaultPMMClientImage() string {
	if v.UsesLegacyAuth() {
		return PMM2ClientImage
	}
	return PMM3ClientImage
}

// PMMSecretKeyName returns the key name that should be used in the PMM secret
// depending on engine and auth type
func (v *PMMServerVersion) PMMSecretKeyName(engineType EngineType) string {
	if pair, ok := pmmServerKeys[engineType]; ok {
		if v.UsesLegacyAuth() {
			return pair.Legacy
		}
		return pair.New
	}
	return ""
}

// iAuth an interface to apply auth to a request.
type iAuth interface {
	apply(req *http.Request)
}

// basicAuth represents basic auth with User/Password
type basicAuth struct {
	user     string
	password string
}

func (a basicAuth) apply(req *http.Request) {
	req.SetBasicAuth(a.user, a.password)
}

// bearerAuth represents bearer auth with a token
type bearerAuth struct {
	token string
}

func (a bearerAuth) apply(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+a.token)
}
