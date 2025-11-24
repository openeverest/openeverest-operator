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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	shdcName             = "my-shdc"
	shdcNamespace        = "default"
	shdcBaseDomainSuffix = "example.com"
	certSecretName       = "my-tls-secret" //nolint:gosec
	tlsCACertBase64      = "dGxzQ0FDZXJ0Cg=="
	tlsCAKeyBase64       = "dGxzS2V5Cg=="

	tlsCACertNewBase64 = "dGxzQ0FDZXJ0TmV3Cg=="
	tlsCAKeyNewBase64  = "dGxzS2V5TmV3Cg=="
)

func TestSplitHorizonDNSConfigCustomValidator_ValidateCreate(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		objs         []ctrlclient.Object
		shdcToCreate *enginefeatureseverestv1alpha1.SplitHorizonDNSConfig
		wantErr      error
	}

	testCases := []testCase{
		// .spec.baseDomainNameSuffix
		{
			name: ".spec.baseDomainNameSuffix is missing",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errRequiredField(baseDomainNameSuffixPath),
				errRequiredField(secretNamePath),
			}),
		},
		{
			name: ".spec.baseDomainNameSuffix is empty",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: "",
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errRequiredField(baseDomainNameSuffixPath),
				errRequiredField(secretNamePath),
			}),
		},
		{
			name: ".spec.baseDomainNameSuffix is invalid",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: "invalid_domain",
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errInvalidBaseDomainNameSuffix("invalid_domain", []string{errors.New("a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')").Error()}),
				errRequiredField(secretNamePath),
			}),
		},
		// .spec.tls.secretName
		{
			name: ".spec.tls.secretName is missing",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS:                  enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errRequiredField(secretNamePath),
			}),
		},
		{
			name: ".spec.tls.secretName is empty",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: "",
					},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errRequiredField(secretNamePath),
			}),
		},
		// .spec.tls.certificate
		{
			name: ".spec.tls.certificate is missing, secret has wrong type",
			objs: []ctrlclient.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      certSecretName,
						Namespace: shdcNamespace,
					},
					Type: corev1.SecretTypeTLS,
				},
			},
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				field.Invalid(secretNamePath, certSecretName, fmt.Sprintf("the secret must be of type '%s'", corev1.SecretTypeOpaque)),
				field.Required(secretNamePath, "ca.crt field is missed in the secret"),
				field.Required(secretNamePath, "ca.key field is missed in the secret"),
			}),
		},
		{
			name: ".spec.tls.certificate is missing, secret exists but lacks required keys",
			objs: []ctrlclient.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      certSecretName,
						Namespace: shdcNamespace,
					},
					Data: map[string][]byte{
						"extraField1": []byte("extraValue1"),
					},
					Type: corev1.SecretTypeOpaque,
				},
			},
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				field.Required(secretNamePath, "ca.crt field is missed in the secret"),
				field.Required(secretNamePath, "ca.key field is missed in the secret"),
			}),
		},
		// valid cases
		{
			name: "all fields are valid, secret with .spec.tls.secretName exists",
			objs: []ctrlclient.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      certSecretName,
						Namespace: shdcNamespace,
					},
					Data: map[string][]byte{
						"ca.key": []byte(tlsCAKeyBase64),
						"ca.crt": []byte(tlsCACertBase64),
					},
					Type: corev1.SecretTypeOpaque,
				},
			},
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			utilruntime.Must(clientgoscheme.AddToScheme(scheme))
			utilruntime.Must(enginefeatureseverestv1alpha1.AddToScheme(scheme))

			mockClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objs...).
				Build()

			validator := SplitHorizonDNSConfigCustomValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateCreate(context.TODO(), tc.shdcToCreate)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}

func TestSplitHorizonDNSConfigCustomValidator_ValidateUpdate(t *testing.T) { //nolint:maintidx
	t.Parallel()

	type testCase struct {
		name    string
		objs    []ctrlclient.Object
		oldShdc *enginefeatureseverestv1alpha1.SplitHorizonDNSConfig
		newShdc *enginefeatureseverestv1alpha1.SplitHorizonDNSConfig
		wantErr error
	}

	testCases := []testCase{
		// .spec can't be updated for in-used SHDC
		// .spec immutability
		{
			name: ".spec.baseDomainNameSuffix update is not allowed",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:       shdcName,
					Namespace:  shdcNamespace,
					Finalizers: []string{everestv1alpha1.InUseResourceFinalizer},
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: "my-newcompany.com",
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errImmutableField(specPath),
			}),
		},
		{
			name: ".spec.tls.secretName update is not allowed",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:       shdcName,
					Namespace:  shdcNamespace,
					Finalizers: []string{everestv1alpha1.InUseResourceFinalizer},
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: "my-tls-secret-new",
					},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errImmutableField(specPath),
			}),
		},

		// .spec can be updated for non-used SHDC
		// invalid .spec.baseDomainSuffix values
		{
			name: "invalid .spec.baseDomainNameSuffix",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: "invalid_domain",
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errInvalidBaseDomainNameSuffix("invalid_domain", []string{"a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"}),
			}),
		},
		// invalid .spec.tls.certificate values
		{
			name: ".spec.tls.certificate value is missing",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName:  certSecretName,
						Certificate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSCertificateSpec{},
					},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errRequiredField(caCertFilePath),
				errRequiredField(caKeyFilePath),
			}),
		},
		// invalid .spec.tls.certificate.ca.crt values
		{
			name: ".spec.tls.certificate.ca.crt value is missing",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
						Certificate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSCertificateSpec{
							CACert: "",
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errRequiredField(caCertFilePath),
				errRequiredField(caKeyFilePath),
			}),
		},
		{
			name: ".spec.tls.certificate.ca.crt value is not base64-encoded",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
						Certificate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSCertificateSpec{
							CACert: "123435",
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errCertWrongEncodingField(caCertFilePath, "123435"),
				errRequiredField(caKeyFilePath),
			}),
		},
		// invalid .spec.tls.certificate.ca.key values
		{
			name: ".spec.tls.certificate.ca.key value is missing",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
						Certificate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSCertificateSpec{
							CACert: tlsCACertBase64,
							CAKey:  "",
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errRequiredField(caKeyFilePath),
			}),
		},
		{
			name: ".spec.tls.certificate.ca.key value is not base64-encoded",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
						Certificate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSCertificateSpec{
							CACert: tlsCACertBase64,
							CAKey:  "123456",
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(groupKind, shdcName, field.ErrorList{
				errCertWrongEncodingField(caKeyFilePath, "123456"),
			}),
		},
		// valid update of .spec.tls.certificate
		{
			name: ".spec.tls.certificate update is allowed",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
						Certificate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSCertificateSpec{
							CACert: tlsCACertNewBase64,
							CAKey:  tlsCAKeyNewBase64,
						},
					},
				},
			},
			wantErr: nil,
		},
		// finalizers
		{
			name: "set finalizers",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:       shdcName,
					Namespace:  shdcNamespace,
					Finalizers: []string{consts.EngineFeaturesSplitHorizonDNSConfigSecretCleanupFinalizer},
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "remove finalizers",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:       shdcName,
					Namespace:  shdcNamespace,
					Finalizers: []string{consts.EngineFeaturesSplitHorizonDNSConfigSecretCleanupFinalizer},
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			wantErr: nil,
		},
		// status
		{
			name: "update status",
			oldShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			newShdc: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:       shdcName,
					Namespace:  shdcNamespace,
					Finalizers: []string{consts.EngineFeaturesSplitHorizonDNSConfigSecretCleanupFinalizer},
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
				Status: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigStatus{
					InUse: true,
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			utilruntime.Must(clientgoscheme.AddToScheme(scheme))
			utilruntime.Must(enginefeatureseverestv1alpha1.AddToScheme(scheme))

			mockClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objs...).
				Build()

			validator := SplitHorizonDNSConfigCustomValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateUpdate(context.TODO(), tc.oldShdc, tc.newShdc)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}

func TestSplitHorizonDNSConfigCustomValidator_ValidateDelete(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		objs         []ctrlclient.Object
		shdcToDelete *enginefeatureseverestv1alpha1.SplitHorizonDNSConfig
		wantErr      error
	}

	testCases := []testCase{
		// SHDC used by some DB Clusters
		{
			name: "SplitHorizonDNSConfig is used by some DB Clusters",
			shdcToDelete: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
					Finalizers: []string{
						everestv1alpha1.InUseResourceFinalizer,
					},
				},
				Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
					BaseDomainNameSuffix: shdcBaseDomainSuffix,
					TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
						SecretName: certSecretName,
					},
				},
			},
			wantErr: apierrors.NewForbidden(
				enginefeatureseverestv1alpha1.GroupVersion.WithResource("splithorizondnsconfig").GroupResource(),
				shdcName,
				errDeleteInUse),
		},
		// SHDC not used by any DB Cluster
		{
			name: "SplitHorizonDNSConfig is not used by any DB Cluster",
			objs: []ctrlclient.Object{
				&enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
					ObjectMeta: metav1.ObjectMeta{
						Name:      shdcName,
						Namespace: shdcNamespace,
					},
					Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
						BaseDomainNameSuffix: shdcBaseDomainSuffix,
						TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
							SecretName: certSecretName,
							Certificate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSCertificateSpec{
								CACert: tlsCACertBase64,
								CAKey:  tlsCAKeyBase64,
							},
						},
					},
				},
			},
			shdcToDelete: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      shdcName,
					Namespace: shdcNamespace,
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			utilruntime.Must(clientgoscheme.AddToScheme(scheme))
			utilruntime.Must(enginefeatureseverestv1alpha1.AddToScheme(scheme))

			mockClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objs...).
				Build()

			validator := SplitHorizonDNSConfigCustomValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateDelete(context.TODO(), tc.shdcToDelete)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}
