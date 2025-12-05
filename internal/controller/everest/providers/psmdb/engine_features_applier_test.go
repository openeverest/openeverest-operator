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

package psmdb

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"testing"
	"time"

	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8sError "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	enginefeatureseverestv1alpha1 "github.com/percona/everest-operator/api/enginefeatures.everest/v1alpha1"
	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

const (
	namespace            = "default"
	dbName               = "test-db"
	shdcName             = "test-shdc"
	shdcBaseDomainSuffix = "mycompany.com"
	certSecretName       = "shdc-secret"
)

func Test_engineFeaturesApplier_applySplitHorizonDNSConfig(t *testing.T) { //nolint:maintidx
	t.Parallel()

	caCert, caKey, err := generateCA()
	require.NoError(t, err)

	caSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      certSecretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"ca.crt": []byte(caCert),
			"ca.key": []byte(caKey),
		},
	}

	type testCase struct {
		name        string
		objs        []ctrlclient.Object
		db          *everestv1alpha1.DatabaseCluster
		psmdb       *psmdbv1.PerconaServerMongoDB
		wantErr     error
		wantPsmdb   *psmdbv1.PerconaServerMongoDB
		checkSecret bool
	}

	tests := []testCase{
		// SplitHorizonDNSConfigName
		{
			name: "SplitHorizonDNSConfig is missing",
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      dbName,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{},
					},
				},
			},
			psmdb:       &psmdbv1.PerconaServerMongoDB{},
			wantPsmdb:   &psmdbv1.PerconaServerMongoDB{},
			wantErr:     nil,
			checkSecret: false,
		},
		{
			name: "SplitHorizonDNSConfig is empty",
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      dbName,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
							SplitHorizonDNSConfigName: "",
						},
					},
				},
			},
			psmdb:       &psmdbv1.PerconaServerMongoDB{},
			wantPsmdb:   &psmdbv1.PerconaServerMongoDB{},
			wantErr:     nil,
			checkSecret: false,
		},
		{
			name: "SplitHorizonDNSConfig is absent",
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      dbName,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
							SplitHorizonDNSConfigName: shdcName,
						},
					},
				},
			},
			psmdb:     &psmdbv1.PerconaServerMongoDB{},
			wantPsmdb: &psmdbv1.PerconaServerMongoDB{},
			wantErr: k8sError.NewNotFound(schema.GroupResource{
				Group:    enginefeatureseverestv1alpha1.GroupVersion.Group,
				Resource: "splithorizondnsconfigs",
			},
				shdcName,
			),
		},
		// Sharding is enabled
		{
			name: "Sharding is enabled",
			db: &everestv1alpha1.DatabaseCluster{
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Sharding: &everestv1alpha1.Sharding{
						Enabled: true,
					},
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
							SplitHorizonDNSConfigName: shdcName,
						},
					},
				},
			},
			psmdb:       &psmdbv1.PerconaServerMongoDB{},
			wantPsmdb:   &psmdbv1.PerconaServerMongoDB{},
			wantErr:     errShardingNotSupported,
			checkSecret: false,
		},
		// Valid cases
		// 3 replicaset member
		{
			name: "SplitHorizonDNSConfig is present",
			objs: []ctrlclient.Object{
				caSecret,
				&enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
					ObjectMeta: metav1.ObjectMeta{
						Name:      shdcName,
						Namespace: namespace,
					},
					Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
						BaseDomainNameSuffix: shdcBaseDomainSuffix,
						TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
							SecretName: certSecretName,
						},
					},
				},
			},
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      dbName,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:     everestv1alpha1.DatabaseEnginePSMDB,
						Replicas: 3,
					},
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
							SplitHorizonDNSConfigName: shdcName,
						},
					},
				},
			},
			psmdb: &psmdbv1.PerconaServerMongoDB{
				Spec: psmdbv1.PerconaServerMongoDBSpec{
					Replsets: []*psmdbv1.ReplsetSpec{
						{},
					},
				},
			},
			wantPsmdb: &psmdbv1.PerconaServerMongoDB{
				Spec: psmdbv1.PerconaServerMongoDBSpec{
					Replsets: []*psmdbv1.ReplsetSpec{
						{
							Horizons: psmdbv1.HorizonsSpec{
								dbName + "-rs0-0": {
									splitHorizonExternalKey: fmt.Sprintf("%s-rs0-0-%s.%s", dbName, namespace, shdcBaseDomainSuffix),
								},
								dbName + "-rs0-1": {
									splitHorizonExternalKey: fmt.Sprintf("%s-rs0-1-%s.%s", dbName, namespace, shdcBaseDomainSuffix),
								},
								dbName + "-rs0-2": {
									splitHorizonExternalKey: fmt.Sprintf("%s-rs0-2-%s.%s", dbName, namespace, shdcBaseDomainSuffix),
								},
							},
						},
					},
					Secrets: &psmdbv1.SecretsSpec{
						SSL: getSplitHorizonDNSConfigSecretName(dbName),
					},
				},
			},
			checkSecret: true,
		},
		// 1 replicaset members
		{
			name: "SplitHorizonDNSConfig is present",
			objs: []ctrlclient.Object{
				caSecret,
				&enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{
					ObjectMeta: metav1.ObjectMeta{
						Name:      shdcName,
						Namespace: namespace,
					},
					Spec: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigSpec{
						BaseDomainNameSuffix: shdcBaseDomainSuffix,
						TLS: enginefeatureseverestv1alpha1.SplitHorizonDNSConfigTLSSpec{
							SecretName: certSecretName,
						},
					},
				},
			},
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      dbName,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Type:     everestv1alpha1.DatabaseEnginePSMDB,
						Replicas: 1,
					},
					EngineFeatures: &everestv1alpha1.EngineFeatures{
						PSMDB: &everestv1alpha1.PSMDBEngineFeatures{
							SplitHorizonDNSConfigName: shdcName,
						},
					},
				},
			},
			psmdb: &psmdbv1.PerconaServerMongoDB{
				Spec: psmdbv1.PerconaServerMongoDBSpec{
					Replsets: []*psmdbv1.ReplsetSpec{
						{},
					},
				},
			},
			wantPsmdb: &psmdbv1.PerconaServerMongoDB{
				Spec: psmdbv1.PerconaServerMongoDBSpec{
					Replsets: []*psmdbv1.ReplsetSpec{
						{
							Horizons: psmdbv1.HorizonsSpec{
								dbName + "-rs0-0": {
									splitHorizonExternalKey: fmt.Sprintf("%s-rs0-0-%s.%s", dbName, namespace, shdcBaseDomainSuffix),
								},
							},
						},
					},
					Secrets: &psmdbv1.SecretsSpec{
						SSL: getSplitHorizonDNSConfigSecretName(dbName),
					},
				},
			},
			checkSecret: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			utilruntime.Must(clientgoscheme.AddToScheme(scheme))
			utilruntime.Must(everestv1alpha1.AddToScheme(scheme))
			utilruntime.Must(enginefeatureseverestv1alpha1.AddToScheme(scheme))

			mockClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objs...).
				Build()

			a := NewEngineFeaturesApplier(
				&Provider{
					PerconaServerMongoDB: tc.psmdb,
					ProviderOptions: providers.ProviderOptions{
						C:  mockClient,
						DB: tc.db,
					},
				},
			)

			err := a.applySplitHorizonDNSConfig(context.Background())
			if tc.wantErr == nil {
				require.NoError(t, err)
				assert.Equal(t, tc.wantPsmdb, tc.psmdb)
				if tc.checkSecret {
					genSecret := &corev1.Secret{}
					err = mockClient.Get(context.Background(), ctrlclient.ObjectKey{
						Namespace: namespace,
						Name:      getSplitHorizonDNSConfigSecretName(tc.db.GetName()),
					}, genSecret)
					require.NoError(t, err)
					assert.Equal(t, getSplitHorizonDNSConfigSecretName(dbName), genSecret.GetName())
					assert.Equal(t, dbName, genSecret.OwnerReferences[0].Name)
					assert.Equal(t, []byte(caCert), genSecret.Data["ca.crt"])
					assert.Equal(t, corev1.SecretTypeTLS, genSecret.Type)
				}
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}

func generateCA() (string, string, error) {
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization: []string{"PSMDB"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return "", "", err
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return "", "", err
	}

	caPEM := new(bytes.Buffer)
	_ = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})

	caPrivKeyPEM := new(bytes.Buffer)
	_ = pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})

	return caPEM.String(), caPrivKeyPEM.String(), nil
}
