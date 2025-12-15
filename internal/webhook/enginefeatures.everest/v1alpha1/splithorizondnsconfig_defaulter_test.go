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
	"github.com/percona/everest-operator/internal/consts"
)

func TestSplitHorizonDNSConfigDefaulter_Default(t *testing.T) { //nolint:maintidx
	t.Parallel()

	type testCase struct {
		name         string
		objs         []ctrlclient.Object
		shdcToCreate *enginefeatureseverestv1alpha1.SplitHorizonDNSConfig
		wantErr      error
	}

	testCases := []testCase{
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
			name: ".spec.tls.certificate is missing, secret is absent",
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
		},
		{
			name: ".spec.tls.certificate is provided but has empty fields",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
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
		// .spec.tls.certificate.ca.crt
		{
			name: ".spec.tls.certificate.ca.crt value is missing",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
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
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
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
		// .spec.tls.certificate.ca.key
		{
			name: ".spec.tls.certificate.ca.key value is missing",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
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
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
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
		// valid cases
		{
			name: "all fields are valid, .spec.tls.certificate is provided",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
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
			wantErr: nil,
		},
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
			wantErr: nil,
		},
		{
			name: "all fields are valid, secret with .spec.tls.secretName is absent",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
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
			wantErr: nil,
		},
		{
			name: "update certificate, secret with .spec.tls.secretName exists",
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
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
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
		// status
		{
			name: "update status",
			shdcToCreate: &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
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

			defaulter := SplitHorizonDNSConfigDefaulter{
				Client: mockClient,
				Scheme: scheme,
			}

			err := defaulter.Default(context.TODO(), tc.shdcToCreate)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}
