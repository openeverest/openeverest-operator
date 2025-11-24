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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/xeipuuv/gojsonschema"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=di
//+kubebuilder:printcolumn:name="DisplayName",type="string",JSONPath=".spec.displayName"
//+kubebuilder:printcolumn:name="Description",type="string",JSONPath=".spec.description"
//+kubebuilder:printcolumn:name="SupportedEngines",type="string",JSONPath=".spec.supportedEngines"
//+kubebuilder:resource:scope=Cluster

// DataImporter defines a reusable strategy for importing data into a DatabaseCluster.
type DataImporter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataImporterSpec   `json:"spec,omitempty"`
	Status DataImporterStatus `json:"status,omitempty"`
}

// EngineList is a type alias for a list of EngineType.
type EngineList []EngineType

// Has checks if the list contains the specified engine.
func (e EngineList) Has(engine EngineType) bool {
	return slices.Contains(e, engine)
}

// DataImporterSpec defines the specification of a DataImporter.
type DataImporterSpec struct {
	// DisplayName is a human-readable name for the data importer.
	DisplayName string `json:"displayName,omitempty"`
	// Description is the description of the data importer.
	Description string `json:"description,omitempty"`
	// SupportedEngines is the list of engines that the data importer supports.
	SupportedEngines EngineList `json:"supportedEngines,omitempty"`
	// Config contains additional configuration defined for the data importer.
	Config DataImporterConfig `json:"config,omitempty"`
	// JobSpec is the specification of the data importer job.
	JobSpec DataImporterJobSpec `json:"jobSpec,omitempty"`
	// DatabaseClusterConstraints defines compatibility requirements and prerequisites that must be satisfied
	// by a DatabaseCluster before this data importer can be used with it. This allows the data importer to
	// express specific requirements about the database configuration needed for successful import operations,
	// such as required database fields, specific engine configurations, or other database properties.
	// When a DatabaseCluster references this data importer, the operator will validate the DatabaseCluster
	// against these constraints before proceeding with the import operation.
	// +optional
	DatabaseClusterConstraints DataImporterDatabaseClusterConstraints `json:"databaseClusterConstraints,omitempty"`
	// Permissions defines the permissions required by the data importer.
	// These permissions are used to generate a Role for the data importer job.
	// +optional
	Permissions []rbacv1.PolicyRule `json:"permissions,omitempty"`
	// ClusterPermissions defines the cluster-wide permissions required by the data importer.
	// These permissions are used to generate a ClusterRole for the data importer job.
	// +optional
	ClusterPermissions []rbacv1.PolicyRule `json:"clusterPermissions,omitempty"`
}

// DataImporterConfig contains additional configuration defined for the data importer.
type DataImporterConfig struct {
	// OpenAPIV3Schema is the OpenAPI v3 schema of the data importer.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	// +optional
	OpenAPIV3Schema *apiextensionsv1.JSONSchemaProps `json:"openAPIV3Schema,omitempty"`
}

// ErrSchemaValidationFailure is returned when the parameters do not conform to the DataImporter schema defined in .spec.config.
var ErrSchemaValidationFailure = errors.New("schema validation failed")

// Validate the config for the data importer.
func (cfg *DataImporterConfig) Validate(params *runtime.RawExtension) error {
	schema := cfg.OpenAPIV3Schema
	if schema == nil && params != nil {
		return ErrSchemaValidationFailure
	}
	if schema == nil && params == nil {
		return nil
	}

	// Additional properties are implicitly disallowed
	schema.AdditionalProperties = &apiextensionsv1.JSONSchemaPropsOrBool{
		Allows: false,
	}

	// Unmarshal the parameters into a generic map
	var paramsMap map[string]interface{}
	if err := json.Unmarshal(params.Raw, &paramsMap); err != nil {
		return fmt.Errorf("failed to unmarshal parameters: %w", err)
	}

	// Convert the OpenAPI v3 schema to a JSON schema validator
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal OpenAPI v3 schema: %w", err)
	}

	schemaLoader := gojsonschema.NewStringLoader(string(schemaJSON))
	paramsLoader := gojsonschema.NewGoLoader(paramsMap)

	// Validate the parameters against the schema
	result, err := gojsonschema.Validate(schemaLoader, paramsLoader)
	if err != nil {
		return fmt.Errorf("failed to validate parameters: %w", err)
	}

	if !result.Valid() {
		var validationErrors []string
		for _, err := range result.Errors() {
			validationErrors = append(validationErrors, err.String())
		}
		return errors.Join(ErrSchemaValidationFailure, fmt.Errorf("validation errors: %s", strings.Join(validationErrors, "; ")))
	}
	return nil
}

// DataImporterJobSpec defines the specification for the Kubernetes job.
type DataImporterJobSpec struct {
	// Image is the image of the data importer.
	Image string `json:"image,omitempty"`
	// Command is the command to run the data importer.
	// +optional
	Command []string `json:"command,omitempty"`
}

// DataImporterDatabaseClusterConstraints defines compatibility requirements and prerequisites
// that must be satisfied by a DatabaseCluster before this data importer can be used with it.
type DataImporterDatabaseClusterConstraints struct {
	// RequiredFields contains a list of fields that must be set in the DatabaseCluster spec.
	// Each key is a JSON path expressions that points to a field in the DatabaseCluster spec.
	// For example, ".spec.engine.type" or ".spec.dataSource.dataImport.config.someField".
	// +optional
	RequiredFields []string `json:"requiredFields,omitempty"`
}

// DataImporterList contains a list of DataImporter.
// +kubebuilder:object:root=true
type DataImporterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataImporter `json:"items"`
}

// DataImporterStatus defines the status of the DataImporter.
type DataImporterStatus struct{}

func init() {
	SchemeBuilder.Register(&DataImporter{}, &DataImporterList{})
}
