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
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=dij
//+kubebuilder:printcolumn:name="TargetCluster",type="string",JSONPath=".spec.targetClusterName"
//+kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.state"

// DataImportJob is the schema for the dataimportjobs API.
type DataImportJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataImportJobSpec   `json:"spec,omitempty"`
	Status DataImportJobStatus `json:"status,omitempty"`
}

// DataImportJobSpec defines the desired state of DataImportJob.
type DataImportJobSpec struct {
	// TargetClusterName is the reference to the target cluster.
	// +kubebuilder:validation:Required
	TargetClusterName      string `json:"targetClusterName,omitempty"`
	*DataImportJobTemplate `json:",inline"`
}

// DataImportJobTemplate defines a shared template for the data import job.
type DataImportJobTemplate struct {
	// DataImporterName is the data importer to use for the import.
	// +kubebuilder:validation:Required
	DataImporterName string `json:"dataImporterName,omitempty"`
	// Source is the source of the data to import.
	// +kubebuilder:validation:Required
	Source *DataImportJobSource `json:"source,omitempty"`
	// Config defines the configuration for the data import job.
	// These options are specific to the DataImporter being used and must conform to
	// the schema defined in the DataImporter's .spec.config.openAPIV3Schema.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// DataImportJobSource defines the source of the data to import.
type DataImportJobSource struct {
	// S3 contains the S3 information for the data import.
	// +optional
	S3 *DataImportJobS3Source `json:"s3,omitempty"`
	// Path is the path to the directory to import the data from.
	// This may be a path to a file or a directory, depending on the data importer.
	// Only absolute file paths are allowed. Leading and trailing '/' are optional.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self.matches('^/?([^/]+(/[^/]+)*)/?$')",message="path must be an absolute file or directory path"
	Path string `json:"path,omitempty"`
}

// DataImportJobS3Source defines the S3 source for the data import job.
type DataImportJobS3Source struct {
	// Bucket is the name of the S3 bucket.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket,omitempty"`
	// Region is the region of the S3 bucket.
	// +kubebuilder:validation:Required
	Region string `json:"region,omitempty"`
	// EndpointURL is an endpoint URL of backup storage.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="isURL(self)",message="endpointURL must be a valid URL"
	EndpointURL string `json:"endpointURL,omitempty"`
	// VerifyTLS is set to ensure TLS/SSL verification.
	// If unspecified, the default value is true.
	//
	// +kubebuilder:default:=true
	VerifyTLS *bool `json:"verifyTLS,omitempty"`
	// ForcePathStyle is set to use path-style URLs.
	// If unspecified, the default value is false.
	//
	// +kubebuilder:default:=false
	ForcePathStyle *bool `json:"forcePathStyle,omitempty"`
	// CredentialsSecreName is the reference to the secret containing the S3 credentials.
	// The Secret must contain the keys `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
	// +kubebuilder:validation:Required
	CredentialsSecretName string `json:"credentialsSecretName,omitempty"`

	// AccessKeyID allows specifying the S3 access key ID inline.
	// It is provided as a write-only input field for convenience.
	// When this field is set, a webhook writes this value in the Secret specified by `credentialsSecretName`
	// and empties this field.
	// This field is not stored in the API.
	// +optional
	AccessKeyID string `json:"accessKeyId,omitempty"`
	// SecretAccessKey allows specifying the S3 secret access key inline.
	// It is provided as a write-only input field for convenience.
	// When this field is set, a webhook writes this value in the Secret specified by `credentialsSecretName`
	// and empties this field.
	// This field is not stored in the API.
	// +optional
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
}

// DataImportJobList contains a list of DataImportJob.
// +kubebuilder:object:root=true
type DataImportJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataImportJob `json:"items"`
}

// DataImportJobState is a type representing the state of a data import job.
type DataImportJobState string

const (
	// DataImportJobStatePending indicates that the data import job is pending.
	DataImportJobStatePending DataImportJobState = "Pending"
	// DataImportJobStateRunning indicates that the data import job is currently running.
	DataImportJobStateRunning DataImportJobState = "Running"
	// DataImportJobStateSucceeded indicates that the data import job has completed successfully.
	DataImportJobStateSucceeded DataImportJobState = "Succeeded"
	// DataImportJobStateFailed indicates that the data import job has failed.
	// Once the job is in this phase, it cannot be retried.
	DataImportJobStateFailed DataImportJobState = "Failed"
	// DataImportJobStateError indicates that the data import job has encountered an error.
	// This phase is used for transient errors that may allow the job to be retried.
	DataImportJobStateError DataImportJobState = "Error"
)

// DataImportJobStatus defines the observed state of DataImportJob.
type DataImportJobStatus struct {
	// StartedAt is the time when the data import job started.
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// CompletedAt is the time when the data import job completed successfully.
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// LastObservedGeneration is the last observed generation of the data import job.
	LastObservedGeneration int64 `json:"lastObservedGeneration,omitempty"`
	// State is the current state of the data import job.
	State DataImportJobState `json:"state,omitempty"`
	// Message is the message of the data import job.
	Message string `json:"message,omitempty"`
	// JobName is the reference to the job that is running the data import.
	// +optional
	JobName string `json:"jobName,omitempty"`
}

func init() {
	SchemeBuilder.Register(&DataImportJob{}, &DataImportJobList{})
}
