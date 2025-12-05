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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/utils"
)

func TestIsBase64Encoded(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input    string
		expected bool
	}{
		{"", false},
		{"Zm9v", true},     // "foo" in base64
		{"Zm9vYmFy", true}, // "foobar" in base64
		{"notbase64", false},
		{"Zm9vYmFyIQ==", true}, // "foobar!" in base64
		{"Zm9vYmFyIQ", false},  // invalid base64 (wrong padding)
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, utils.IsBase64Encoded(c.input), "input: %q", c.input)
	}
}

func TestDataImportJobDefaulter(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = everestv1alpha1.AddToScheme(scheme)

	const (
		ns         = "test-ns"
		secretName = "s3-creds"
		accessKey  = "ZmFrZUFjY2Vzc0tleQ==" // base64 for "fakeAccessKey"
		secretKey  = "ZmFrZVNlY3JldEtleQ==" //nolint:gosec // base64 for "fakeSecretKey"
	)

	dij := &everestv1alpha1.DataImportJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: ns,
		},
		Spec: everestv1alpha1.DataImportJobSpec{
			TargetClusterName: "target-cluster",
			DataImportJobTemplate: &everestv1alpha1.DataImportJobTemplate{
				DataImporterName: "importer",
				Source: &everestv1alpha1.DataImportJobSource{
					S3: &everestv1alpha1.DataImportJobS3Source{
						Bucket:                "bucket",
						Region:                "region",
						EndpointURL:           "https://s3.example.com",
						CredentialsSecretName: secretName,
						AccessKeyID:           accessKey,
						SecretAccessKey:       secretKey,
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	defaulter := &DataImportJobDefaulter{Client: client}

	err := defaulter.Default(t.Context(), dij)
	require.NoError(t, err)

	// Check that the credentials are removed from the spec
	assert.Empty(t, dij.Spec.DataImportJobTemplate.Source.S3.AccessKeyID)
	assert.Empty(t, dij.Spec.DataImportJobTemplate.Source.S3.SecretAccessKey)

	// Check that the secret was created and contains the expected data
	secret := &corev1.Secret{}
	err = client.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: secretName}, secret)
	require.NoError(t, err)
	assert.Equal(t, accessKey, string(secret.Data[accessKeyIDSecretKey]))
	assert.Equal(t, secretKey, string(secret.Data[secretAccessKeySecretKey]))
}
