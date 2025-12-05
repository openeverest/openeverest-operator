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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SplitHorizonDNSConfigTLSCertificateSpec defines TLS certificate parameters.
type SplitHorizonDNSConfigTLSCertificateSpec struct {
	// CA Private Key is based64 encoded ca-key.pem file content.
	// It is provided as a write-only input field for convenience.
	// When this field is set, a webhook writes this value in the Secret specified by `.spec.tls.secretName`
	// and empties this field.
	// This field is not stored in the API.
	// +kubebuilder:validation:Required
	CAKey string `json:"ca.key"`
	// CACert is based64 encoded ca.pem file content.
	// It is provided as a write-only input field for convenience.
	// When this field is set, a webhook writes this value in the Secret specified by `.spec.tls.secretName`
	// and empties this field.
	// This field is not stored in the API.
	// +kubebuilder:validation:Required
	CACert string `json:"ca.crt"`
}

// SplitHorizonDNSConfigTLSSpec defines TLS configuration for SplitHorizonDNSConfig.
// It can be provided either via a secret or directly as a certificate.
type SplitHorizonDNSConfigTLSSpec struct {
	// SecretName is the name of the secret containing the TLS certificate and key for the split-horizon DNS configuration.
	// +kubebuilder:example="my-tls-secret"
	// +kubebuilder:validation:Required
	SecretName string `json:"secretName"`
	// Certificate is the TLS certificate and key for the split-horizon DNS configuration.
	// +kubebuilder:validation:Optional
	Certificate *SplitHorizonDNSConfigTLSCertificateSpec `json:"certificate,omitempty"`
}

// SplitHorizonDNSConfigSpec defines the desired state of SplitHorizonDNSConfig.
type SplitHorizonDNSConfigSpec struct {
	// BaseDomainNameSuffix is the base domain name suffix for generating domain names for each Pod in ReplicaSet.
	// It should be a valid domain name suffix.
	// +kubebuilder:validation:Pattern=`^([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$`
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:example="example.com"
	// +kubebuilder:validation:Required
	BaseDomainNameSuffix string `json:"baseDomainNameSuffix,omitempty,omitzero"`
	// TLS is the TLS configuration for the split-horizon DNS configuration.
	// +kubebuilder:validation:Required
	TLS SplitHorizonDNSConfigTLSSpec `json:"tls"`
}

// SplitHorizonDomain defines SplitHorizon domain status (domain -> IPs mapping).
type SplitHorizonDomain struct {
	// Domain is the SplitHorizon domain name.
	Domain string `json:"domain,omitempty"`
	// PrivateIP is the private IP address for the domain.
	PrivateIP string `json:"privateIP,omitempty"`
	// PublicIP is the public IP address for the domain.
	PublicIP string `json:"publicIP,omitempty"`
}

// SplitHorizonStatus holds SplitHorizon engine feature status.
type SplitHorizonStatus struct {
	// ConnectionURL is the connection URL using SplitHorizon domains.
	Host string `json:"host,omitempty"`
	// SplitHorizon status of SplitHorizon feature.
	Domains []SplitHorizonDomain `json:"domains,omitempty"`
}

// SplitHorizonDNSConfigStatus defines the observed state of SplitHorizonDNSConfig resource.
type SplitHorizonDNSConfigStatus struct {
	// InUse is a flag that indicates if the config is used by any DB cluster.
	// +kubebuilder:default=false
	InUse bool `json:"inUse,omitempty"`
	// LastObservedGeneration is the most recent generation observed for this SplitHorizonDNSConfig.
	LastObservedGeneration int64 `json:"lastObservedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=splitdns
//nolint:lll
// +kubebuilder:printcolumn:name="BaseDomainSuffix",type="string",JSONPath=".spec.baseDomainNameSuffix",description="base domain name suffix for generating domain names for each Pod in ReplicaSet"
// +kubebuilder:printcolumn:name="InUse",type="string",JSONPath=".status.inUse",description="Indicates if the config is used by any DB cluster"

// SplitHorizonDNSConfig is the Schema for the splithorizondnsconfigs API.
type SplitHorizonDNSConfig struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is a standard object metadata
	// +kubebuilder:validation:Optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// Spec defines the desired state of SplitHorizonDNSConfig
	// +kubebuilder:validation:Required
	Spec SplitHorizonDNSConfigSpec `json:"spec"`
	// Status defines the observed state of SplitHorizonDNSConfig
	// +kubebuilder:default={"inUse": false}
	Status SplitHorizonDNSConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SplitHorizonDNSConfigList contains a list of SplitHorizonDNSConfig.
type SplitHorizonDNSConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []SplitHorizonDNSConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SplitHorizonDNSConfig{}, &SplitHorizonDNSConfigList{})
}
