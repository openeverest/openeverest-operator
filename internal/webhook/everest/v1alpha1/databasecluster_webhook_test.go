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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apiSchema "k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	enginefeatureseverestv1alpha1 "github.com/percona/everest-operator/api/enginefeatures.everest/v1alpha1"
	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

const (
	dbcTestDbName         = "db-test"
	dbcTestDbNamespace    = "default"
	dbcTestUserSecretName = "user-secret"
	dbcTestPsmdbDbVersion = "7.0.15-9"
	dbcTestPgDbVersion    = "17.1"
	dbcTestPxcDbVersion   = "8.0.42-33.1"
)

func TestCheckJSONKeyExists(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		obj      any
		key      string
		expected bool
	}{
		{
			obj: everestv1alpha1.DatabaseCluster{
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{},
				},
			},
			key: ".spec.engine.type",
		},
		{
			obj: everestv1alpha1.DatabaseCluster{
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type: everestv1alpha1.DatabaseEnginePSMDB,
					},
				},
			},
			key:      ".spec.engine.type",
			expected: true,
		},
		{
			obj: everestv1alpha1.DatabaseCluster{
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						UserSecretsName: "my-user-secrets",
					},
				},
			},
			key:      ".spec.engine.userSecretsName",
			expected: true,
		},
		{
			obj: everestv1alpha1.DatabaseCluster{
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{},
				},
			},
			key: ".spec.engine.userSecretsName",
		},
	}
	for _, tc := range testCases {
		exists, err := checkJSONKeyExists(tc.key, tc.obj)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, exists, "Key existence check failed for key: %s", tc.key)
	}
}

func TestDatabaseClusterValidator_ValidateCreate(t *testing.T) { //nolint:maintidx
	t.Parallel()

	userSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbcTestUserSecretName,
			Namespace: dbcTestDbNamespace,
		},
	}
	// Minimal OpenAPI schema for config with a required string field "foo"
	schema := map[string]any{
		"type":       "object",
		"properties": map[string]any{"foo": map[string]any{"type": "string"}},
		"required":   []string{"foo"},
	}
	schemaBytes, _ := json.Marshal(schema) //nolint:errchkjson
	var openAPISchema apiextensionsv1.JSONSchemaProps
	_ = json.Unmarshal(schemaBytes, &openAPISchema)

	dataImporter := &everestv1alpha1.DataImporter{
		ObjectMeta: metav1.ObjectMeta{
			Name: "importer",
		},
		Spec: everestv1alpha1.DataImporterSpec{
			SupportedEngines: everestv1alpha1.EngineList{everestv1alpha1.DatabaseEnginePSMDB},
			DatabaseClusterConstraints: everestv1alpha1.DataImporterDatabaseClusterConstraints{
				RequiredFields: []string{".spec.engine.userSecretsName"},
			},
			Config: everestv1alpha1.DataImporterConfig{
				OpenAPIV3Schema: &openAPISchema,
			},
		},
	}

	apiObjects := []ctrlclient.Object{ //nolint:dupl
		&everestv1alpha1.DatabaseEngine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      consts.PSMDBDeploymentName,
				Namespace: dbcTestDbNamespace,
			},
			Spec: everestv1alpha1.DatabaseEngineSpec{
				Type: everestv1alpha1.DatabaseEnginePSMDB,
			},
			Status: everestv1alpha1.DatabaseEngineStatus{
				AvailableVersions: everestv1alpha1.Versions{
					Engine: everestv1alpha1.ComponentsMap{
						dbcTestPsmdbDbVersion: {
							Status: everestv1alpha1.DBEngineComponentRecommended,
						},
					},
				},
			},
		},
		&everestv1alpha1.DatabaseEngine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      consts.PGDeploymentName,
				Namespace: dbcTestDbNamespace,
			},
			Spec: everestv1alpha1.DatabaseEngineSpec{
				Type: everestv1alpha1.DatabaseEnginePostgresql,
			},
			Status: everestv1alpha1.DatabaseEngineStatus{
				AvailableVersions: everestv1alpha1.Versions{
					Engine: everestv1alpha1.ComponentsMap{
						dbcTestPgDbVersion: {
							Status: everestv1alpha1.DBEngineComponentRecommended,
						},
					},
				},
			},
		},
		&everestv1alpha1.DatabaseEngine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      consts.PXCDeploymentName,
				Namespace: dbcTestDbNamespace,
			},
			Spec: everestv1alpha1.DatabaseEngineSpec{
				Type: everestv1alpha1.DatabaseEnginePXC,
			},
			Status: everestv1alpha1.DatabaseEngineStatus{
				AvailableVersions: everestv1alpha1.Versions{
					Engine: everestv1alpha1.ComponentsMap{
						dbcTestPxcDbVersion: {
							Status: everestv1alpha1.DBEngineComponentRecommended,
						},
					},
				},
			},
		},
	}

	type testCase struct {
		name      string
		objects   []ctrlclient.Object
		modify    func(*everestv1alpha1.DatabaseCluster)
		wantError error
	}

	testCases := []testCase{
		// DB Engine Version
		{
			name:    "invalid DBEngine version",
			objects: nil,
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePostgresql
				db.Spec.Engine.Version = "17.100"
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				field.NotSupported(dbcEngineVersionPath, "17.100", []string{dbcTestPgDbVersion}),
			}),
		},
		{
			name:    "missing user secret",
			objects: nil,
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.Engine.UserSecretsName = dbcTestUserSecretName
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errInvalidField(dbcUserSecretsNamePath, dbcTestUserSecretName, apierrors.NewNotFound(apiSchema.GroupResource{
					Group:    corev1.SchemeGroupVersion.Group,
					Resource: "secrets",
				},
					dbcTestUserSecretName,
				).Error()),
			}),
		},
		{
			name:    "present user secret",
			objects: []ctrlclient.Object{userSecret},
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.Engine.UserSecretsName = dbcTestUserSecretName
			},
			wantError: nil,
		},
		{
			name:    "missing DataImporter",
			objects: []ctrlclient.Object{userSecret},
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				config := &runtime.RawExtension{Raw: []byte(`{"foo":"bar"}`)}
				db.Spec.DataSource = &everestv1alpha1.DataSource{
					DataImport: &everestv1alpha1.DataImportJobTemplate{
						DataImporterName: "importer",
						Config:           config,
					},
				}
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errInvalidField(dbcDataImportPath, "importer", apierrors.NewNotFound(apiSchema.GroupResource{
					Group:    everestv1alpha1.GroupVersion.Group,
					Resource: "dataimporters",
				},
					"importer",
				).Error()),
			}),
		},
		{
			name:    "unsupported engine",
			objects: []ctrlclient.Object{userSecret, dataImporter},
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				config := &runtime.RawExtension{Raw: []byte(`{"foo":"bar"}`)}
				db.Spec.DataSource = &everestv1alpha1.DataSource{
					DataImport: &everestv1alpha1.DataImportJobTemplate{
						DataImporterName: "importer",
						Config:           config,
					},
				}
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePostgresql
				db.Spec.Engine.Version = dbcTestPgDbVersion
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errInvalidField(dbcDataImportPath, "importer",
					fmt.Sprintf("data importer %s does not support engine type %s", "importer", everestv1alpha1.DatabaseEnginePostgresql)),
			}),
		},
		{
			name:    "missing required field",
			objects: []ctrlclient.Object{userSecret, dataImporter},
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				config := &runtime.RawExtension{Raw: []byte(`{}`)}
				db.Spec.DataSource = &everestv1alpha1.DataSource{
					DataImport: &everestv1alpha1.DataImportJobTemplate{
						DataImporterName: "importer",
						Config:           config,
					},
				}
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePSMDB
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errRequiredField(field.NewPath(".spec", "engine", "userSecretsName")),
			}),
		},
		{
			name:    "valid config",
			objects: []ctrlclient.Object{userSecret, dataImporter},
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				config := &runtime.RawExtension{Raw: []byte(`{"foo":"bar"}`)}
				db.Spec.DataSource = &everestv1alpha1.DataSource{
					DataImport: &everestv1alpha1.DataImportJobTemplate{
						DataImporterName: "importer",
						Config:           config,
					},
				}
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePSMDB
				db.Spec.Engine.UserSecretsName = dbcTestUserSecretName
			},
			wantError: nil,
		},
		// LoadBalancer
		{
			name:    "missing LoadBalancerConfig",
			objects: nil,
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.Proxy.Expose.LoadBalancerConfigName = "lbc-test"
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePSMDB
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errInvalidField(dbcProxyExposeLbcPath, "lbc-test", apierrors.NewNotFound(apiSchema.GroupResource{
					Group:    everestv1alpha1.GroupVersion.Group,
					Resource: "loadbalancerconfigs",
				},
					"lbc-test",
				).Error()),
			}),
		},
		{
			name: "valid LoadBalancerConfig",
			objects: []ctrlclient.Object{
				&everestv1alpha1.LoadBalancerConfig{
					ObjectMeta: metav1.ObjectMeta{
						Name: "lbc-test",
					},
				},
			},
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.Proxy.Expose.LoadBalancerConfigName = "lbc-test"
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePSMDB
			},
			wantError: nil,
		},
		// Engine features
		{
			name:    "wrong engine type - PXC",
			objects: nil,
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.EngineFeatures = &everestv1alpha1.EngineFeatures{
					PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
						SplitHorizonDNSConfigName: "shdc-test",
					},
				}
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePXC
				db.Spec.Engine.Version = dbcTestPxcDbVersion
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errInvalidField(dbcPsmdbShdcEngineFeaturePath, "",
					fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePXC)),
			}),
		},
		{
			name:    "wrong engine type - Postgresql",
			objects: nil,
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.EngineFeatures = &everestv1alpha1.EngineFeatures{
					PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
						SplitHorizonDNSConfigName: "shdc-test",
					},
				}
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePostgresql
				db.Spec.Engine.Version = dbcTestPgDbVersion
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errInvalidField(dbcPsmdbShdcEngineFeaturePath, "",
					fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePostgresql)),
			}),
		},
		// PSMDB engine features - SplitHorizonDNSConfig
		{
			name:    "missing SplitHorizonDNSConfig",
			objects: nil,
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.EngineFeatures = &everestv1alpha1.EngineFeatures{
					PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
						SplitHorizonDNSConfigName: "shdc-test",
					},
				}
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePSMDB
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errInvalidField(dbcPsmdbShdcEngineFeaturePath, "shdc-test", apierrors.NewNotFound(apiSchema.GroupResource{
					Group:    enginefeatureseverestv1alpha1.GroupVersion.Group,
					Resource: "splithorizondnsconfigs",
				},
					"shdc-test",
				).Error()),
			}),
		},
		{
			name:    "SplitHorizonDNSConfig is not supported in Sharded cluster",
			objects: nil,
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.EngineFeatures = &everestv1alpha1.EngineFeatures{
					PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
						SplitHorizonDNSConfigName: "shdc-test",
					},
				}
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePSMDB
				db.Spec.Sharding = &everestv1alpha1.Sharding{
					Enabled: true,
				}
			},
			wantError: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				field.Forbidden(dbcPsmdbShdcEngineFeaturePath, "SplitHorizonDNSConfig and Sharding configuration is not supported"),
			}),
		},
		{
			name: "valid SplitHorizonDNSConfig",
			objects: []ctrlclient.Object{
				&enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "shdc-test",
						Namespace: dbcTestDbNamespace,
					},
				},
			},
			modify: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.EngineFeatures = &everestv1alpha1.EngineFeatures{
					PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
						SplitHorizonDNSConfigName: "shdc-test",
					},
				}
				db.Spec.Engine.Type = everestv1alpha1.DatabaseEnginePSMDB
			},
			wantError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			utilruntime.Must(corev1.AddToScheme(scheme))
			utilruntime.Must(everestv1alpha1.AddToScheme(scheme))
			utilruntime.Must(enginefeatureseverestv1alpha1.AddToScheme(scheme))

			db := &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePSMDB,
						Version: dbcTestPsmdbDbVersion,
					},
				},
			}
			if tc.modify != nil {
				tc.modify(db)
			}
			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(apiObjects...).
				WithObjects(tc.objects...).
				Build()
			validator := &DatabaseClusterValidator{Client: fakeClient}
			_, err := validator.ValidateCreate(t.Context(), db)

			if tc.wantError == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantError.Error(), err.Error())
		})
	}
}

func TestDatabaseClusterValidator_ValidateUpdate(t *testing.T) {
	t.Parallel()

	apiObjects := []ctrlclient.Object{ //nolint:dupl
		&everestv1alpha1.DatabaseEngine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      consts.PSMDBDeploymentName,
				Namespace: dbcTestDbNamespace,
			},
			Spec: everestv1alpha1.DatabaseEngineSpec{
				Type: everestv1alpha1.DatabaseEnginePSMDB,
			},
			Status: everestv1alpha1.DatabaseEngineStatus{
				AvailableVersions: everestv1alpha1.Versions{
					Engine: everestv1alpha1.ComponentsMap{
						dbcTestPsmdbDbVersion: {
							Status: everestv1alpha1.DBEngineComponentRecommended,
						},
					},
				},
			},
		},
		&everestv1alpha1.DatabaseEngine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      consts.PGDeploymentName,
				Namespace: dbcTestDbNamespace,
			},
			Spec: everestv1alpha1.DatabaseEngineSpec{
				Type: everestv1alpha1.DatabaseEnginePostgresql,
			},
			Status: everestv1alpha1.DatabaseEngineStatus{
				AvailableVersions: everestv1alpha1.Versions{
					Engine: everestv1alpha1.ComponentsMap{
						dbcTestPgDbVersion: {
							Status: everestv1alpha1.DBEngineComponentRecommended,
						},
					},
				},
			},
		},
		&everestv1alpha1.DatabaseEngine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      consts.PXCDeploymentName,
				Namespace: dbcTestDbNamespace,
			},
			Spec: everestv1alpha1.DatabaseEngineSpec{
				Type: everestv1alpha1.DatabaseEnginePXC,
			},
			Status: everestv1alpha1.DatabaseEngineStatus{
				AvailableVersions: everestv1alpha1.Versions{
					Engine: everestv1alpha1.ComponentsMap{
						dbcTestPxcDbVersion: {
							Status: everestv1alpha1.DBEngineComponentRecommended,
						},
					},
				},
			},
		},
	}

	type testCase struct {
		name    string
		objs    []ctrlclient.Object
		oldDb   *everestv1alpha1.DatabaseCluster
		newDb   *everestv1alpha1.DatabaseCluster
		wantErr error
	}

	testCases := []testCase{
		// invalid cases
		// db engine type change
		{
			name: "Change DB engine type from PSMDB to PXC",
			oldDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePSMDB,
						Version: dbcTestPsmdbDbVersion,
					},
				},
			},
			newDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePXC,
						Version: dbcTestPxcDbVersion,
					},
				},
			},
			wantErr: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errImmutableField(dbcEngineTypePath),
			}),
		},

		// engine features
		// PSMDB
		// change SplitHorizonDNSConfig engine feature
		{
			name: "Enable SplitHorizonDNSConfig PSMDB engine feature for existing cluster",
			oldDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePSMDB,
						Version: dbcTestPsmdbDbVersion,
					},
				},
			},
			newDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePSMDB,
						Version: dbcTestPsmdbDbVersion,
					},
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
							SplitHorizonDNSConfigName: "test",
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errImmutableField(dbcPsmdbShdcEngineFeaturePath),
			}),
		},
		{
			name: "Disable SplitHorizonDNSConfig PSMDB engine feature for existing cluster",
			oldDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePSMDB,
						Version: dbcTestPsmdbDbVersion,
					},
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
							SplitHorizonDNSConfigName: "test",
						},
					},
				},
			},
			newDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePSMDB,
						Version: dbcTestPsmdbDbVersion,
					},
				},
			},
			wantErr: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errImmutableField(dbcPsmdbShdcEngineFeaturePath),
			}),
		},
		// Enable PSMDB engine features with other engine type
		{
			name: "Enable PSMDB engine features for PXC cluster",
			oldDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePXC,
						Version: dbcTestPxcDbVersion,
					},
				},
			},
			newDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePXC,
						Version: dbcTestPxcDbVersion,
					},
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
							SplitHorizonDNSConfigName: "test",
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errInvalidField(dbcPsmdbShdcEngineFeaturePath, "",
					fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePXC)),
			}),
		},
		{
			name: "Enable PSMDB engine features for Postgresql cluster",
			oldDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePostgresql,
						Version: dbcTestPgDbVersion,
					},
				},
			},
			newDb: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcTestDbName,
					Namespace: dbcTestDbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:    everestv1alpha1.DatabaseEnginePostgresql,
						Version: dbcTestPgDbVersion,
					},
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
							SplitHorizonDNSConfigName: "test",
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(dbClusterGroupKind, dbcTestDbName, field.ErrorList{
				errInvalidField(dbcPsmdbShdcEngineFeaturePath, "",
					fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePostgresql)),
			}),
		},
		// there are no valid cases so far when SplitHorizonDNSConfig can be changed for existing cluster.
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			utilruntime.Must(clientgoscheme.AddToScheme(scheme))
			utilruntime.Must(everestv1alpha1.AddToScheme(scheme))

			mockClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(apiObjects...).
				WithObjects(tc.objs...).
				Build()

			validator := DatabaseClusterValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateUpdate(t.Context(), tc.oldDb, tc.newDb)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}
