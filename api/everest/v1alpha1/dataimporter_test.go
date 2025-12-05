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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestValidateSchema(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		schemaJSON string
		configJSON string
		expectErr  bool
	}{
		{
			name:       "Empty schema, no parameters",
			schemaJSON: `{}`,
			configJSON: `{}`,
			expectErr:  false,
		},
		{
			name:       "Empty schema with parameters",
			schemaJSON: `{}`,
			configJSON: `{"key": "value"}`,
			expectErr:  true,
		},
		{
			name: "Valid parameters",
			schemaJSON: `{
				"type": "object",
				"properties": {
					"key": {"type": "string"}
				},
				"required": ["key"]
			}`,
			configJSON: `{"key": "value"}`,
			expectErr:  false,
		},
		{
			name: "Unknown fields",
			schemaJSON: `{
				"type": "object",
				"properties": {
					"key": {"type": "string"}
				}
			}`,
			configJSON: `{"someUnknownKey": "value"}`,
			expectErr:  true,
		},
		{
			name: "Missing required field",
			schemaJSON: `{
				"type": "object",
				"properties": {
					"key": {"type": "string"}
				},
				"required": ["key"]
			}`,
			configJSON: `{}`,
			expectErr:  true,
		},
		{
			name: "Invalid type",
			schemaJSON: `{
				"type": "object",
				"properties": {
					"key": {"type": "string"}
				}
			}`,
			configJSON: `{"key": 123}`,
			expectErr:  true,
		},
		{
			name: "Object with multiple keys and nested objects",
			schemaJSON: `{
				"type": "object",
				"properties": {
					"key1": {"type": "string"},
					"key2": {"type": "integer"},
					"nested": {
						"type": "object",
						"properties": {
							"subkey1": {"type": "boolean"},
							"subkey2": {"type": "array", "items": {"type": "string"}}
						},
						"required": ["subkey1"]
					}
				},
				"required": ["key1", "nested"]
			}`,
			configJSON: `{
				"key1": "value1",
				"key2": 42,
				"nested": {
					"subkey1": true,
					"subkey2": ["item1", "item2"]
				}
			}`,
			expectErr: false,
		},
		{
			name: "Object with missing nested required field",
			schemaJSON: `{
				"type": "object",
				"properties": {
					"key1": {"type": "string"},
					"key2": {"type": "integer"},
					"nested": {
						"type": "object",
						"properties": {
							"subkey1": {"type": "boolean"},
							"subkey2": {"type": "array", "items": {"type": "string"}}
						},
						"required": ["subkey1"]
					}
				},
				"required": ["key1", "nested"]
			}`,
			configJSON: `{
				"key1": "value1",
				"key2": 42,
				"nested": {
					"subkey2": ["item1", "item2"]
				}
			}`,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var schema apiextensionsv1.JSONSchemaProps
			var config runtime.RawExtension

			// Unmarshal inputs
			require.NoError(t, json.Unmarshal([]byte(tt.schemaJSON), &schema))
			config.Raw = []byte(tt.configJSON)

			cfg := DataImporterConfig{
				OpenAPIV3Schema: &schema,
			}

			err := cfg.Validate(&config)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
