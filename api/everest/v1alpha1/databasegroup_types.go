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
	"k8s.io/apimachinery/pkg/runtime"
)

// DatabaseGroupSpec defines the desired state of DatabaseGroup.
// DatabaseGroup is a logical grouping of DatabaseClusters that share common
// configurations and can be operated on as a unit.
type DatabaseGroupSpec struct {
	// Description provides a human-readable description of the group
	// +optional
	Description string `json:"description,omitempty"`

	// Tags for categorization and filtering
	// +optional
	Tags map[string]string `json:"tags,omitempty"`

	// SharedBackupStorage defines the default backup storage for all clusters in the group
	// +optional
	SharedBackupStorage string `json:"sharedBackupStorage,omitempty"`

	// SharedMonitoring defines the default monitoring configuration for all clusters in the group
	// +optional
	SharedMonitoring string `json:"sharedMonitoring,omitempty"`
}

// DatabaseGroupStatus defines the observed state of DatabaseGroup.
// This status is updated by the DatabaseCluster controller when clusters in the group change.
type DatabaseGroupStatus struct {
	// ObservedGeneration is the most recent generation observed for this DatabaseGroup
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Clusters lists all clusters that are members of this group
	// This is updated by DatabaseCluster controller when clusters join/leave the group
	Clusters []ClusterInGroup `json:"clusters,omitempty"`

	// Conditions represent the latest available observations of the group's state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Message provides additional information about the current state
	// +optional
	Message string `json:"message,omitempty"`
}

// ClusterInGroup represents a single cluster within the group.
type ClusterInGroup struct {
	// Name is the name of the DatabaseCluster resource
	Name string `json:"name"`

	// Engine is the database engine type (postgresql, mongodb, mysql, etc)
	Engine string `json:"engine"`

	// Phase is the current phase of the cluster
	Phase string `json:"phase"`

	// Ready is the number of ready instances
	Ready int32 `json:"ready,omitempty"`

	// Size is the total number of instances
	Size int32 `json:"size,omitempty"`

	// CreatedAt is the timestamp when the cluster was created
	CreatedAt string `json:"createdAt"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=dbg;dbgroup
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
//+kubebuilder:printcolumn:name="Clusters",type="integer",JSONPath=".status.clusters[*]"
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// DatabaseGroup is the Schema for the databasegroups API.
// It provides a way to logically group multiple DatabaseClusters and manage them collectively.
type DatabaseGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatabaseGroupSpec   `json:"spec,omitempty"`
	Status DatabaseGroupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DatabaseGroupList contains a list of DatabaseGroup.
type DatabaseGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabaseGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DatabaseGroup{}, &DatabaseGroupList{})
}
