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

package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// buildScheme returns a minimal scheme that covers corev1 resources.
func buildCoreScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	s := k8sruntime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	return s
}

// TestCreatePGBackrestSecretHasNoOwnerReference verifies that the
// package-level createPGBackrestSecret does NOT set an owner reference on the
// secret.  An owner reference would cause Kubernetes GC to delete the secret
// during foreground DatabaseCluster deletion while a backup is still running,
// deadlocking both the backup pods and the cluster object.
func TestCreatePGBackrestSecretHasNoOwnerReference(t *testing.T) {
	t.Parallel()

	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testdb",
			Namespace: "testns",
			UID:       types.UID("test-uid-1234"),
		},
	}

	cl := fake.NewClientBuilder().WithScheme(buildCoreScheme(t)).Build()

	secret, err := createPGBackrestSecret(
		t.Context(),
		cl,
		db,
		"s3.conf",
		[]byte("[global]\nrepo1-s3-bucket=test\n"),
		db.Name+"-pgbackrest-secrets",
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, secret.GetOwnerReferences(),
		"pgbackrest-secrets must not have an owner reference (see: PG cluster deletion deadlock)")
}

// TestStripPGBackrestSecretOwnerRef_RemovesOwnerRef verifies that a stale
// controller owner reference (left by an older operator version) is removed
// from an existing secret.
func TestStripPGBackrestSecretOwnerRef_RemovesOwnerRef(t *testing.T) {
	t.Parallel()

	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testdb",
			Namespace: "testns",
			UID:       types.UID("test-uid-abcd"),
		},
	}

	isTrue := true
	existingSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testdb-pgbackrest-secrets",
			Namespace: "testns",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "everest.percona.com/v1alpha1",
					Kind:               "DatabaseCluster",
					Name:               db.Name,
					UID:                db.UID,
					Controller:         &isTrue,
					BlockOwnerDeletion: &isTrue,
				},
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"s3.conf": []byte("test")},
	}

	cl := fake.NewClientBuilder().
		WithScheme(buildCoreScheme(t)).
		WithObjects(existingSecret).
		Build()

	err := stripPGBackrestSecretOwnerRef(t.Context(), cl, existingSecret.Name, existingSecret.Namespace, db)
	require.NoError(t, err)

	updated := &corev1.Secret{}
	require.NoError(t, cl.Get(t.Context(), types.NamespacedName{
		Name:      existingSecret.Name,
		Namespace: existingSecret.Namespace,
	}, updated))
	assert.Empty(t, updated.GetOwnerReferences(), "stale owner reference must be stripped")
}

// TestStripPGBackrestSecretOwnerRef_KeepsUnrelatedOwnerRefs verifies that
// owner references belonging to other objects are NOT removed.
func TestStripPGBackrestSecretOwnerRef_KeepsUnrelatedOwnerRefs(t *testing.T) {
	t.Parallel()

	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testdb",
			Namespace: "testns",
			UID:       types.UID("test-uid-abcd"),
		},
	}

	otherUID := types.UID("other-uid-5678")
	isTrue := true
	existingSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testdb-pgbackrest-secrets",
			Namespace: "testns",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "everest.percona.com/v1alpha1",
					Kind:               "DatabaseCluster",
					Name:               db.Name,
					UID:                db.UID,
					Controller:         &isTrue,
					BlockOwnerDeletion: &isTrue,
				},
				{
					APIVersion: "v1",
					Kind:       "ConfigMap",
					Name:       "other-owner",
					UID:        otherUID,
				},
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"s3.conf": []byte("test")},
	}

	cl := fake.NewClientBuilder().
		WithScheme(buildCoreScheme(t)).
		WithObjects(existingSecret).
		Build()

	err := stripPGBackrestSecretOwnerRef(t.Context(), cl, existingSecret.Name, existingSecret.Namespace, db)
	require.NoError(t, err)

	updated := &corev1.Secret{}
	require.NoError(t, cl.Get(t.Context(), types.NamespacedName{
		Name:      existingSecret.Name,
		Namespace: existingSecret.Namespace,
	}, updated))
	require.Len(t, updated.GetOwnerReferences(), 1, "only the DB owner reference should be removed")
	assert.Equal(t, otherUID, updated.GetOwnerReferences()[0].UID)
}

// TestStripPGBackrestSecretOwnerRef_NoopWhenSecretMissing verifies that the
// function is a no-op (no error) when the secret doesn't exist yet.
func TestStripPGBackrestSecretOwnerRef_NoopWhenSecretMissing(t *testing.T) {
	t.Parallel()

	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testdb",
			Namespace: "testns",
			UID:       types.UID("test-uid-abcd"),
		},
	}

	cl := fake.NewClientBuilder().WithScheme(buildCoreScheme(t)).Build()

	assert.NoError(t,
		stripPGBackrestSecretOwnerRef(t.Context(), cl, "testdb-pgbackrest-secrets", "testns", db),
		"missing secret must not return an error",
	)
}

// TestStripPGBackrestSecretOwnerRef_NoopWhenNoOwnerRef verifies that the
// function is a no-op when the secret exists but has no owner references.
func TestStripPGBackrestSecretOwnerRef_NoopWhenNoOwnerRef(t *testing.T) {
	t.Parallel()

	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testdb",
			Namespace: "testns",
			UID:       types.UID("test-uid-abcd"),
		},
	}

	existingSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testdb-pgbackrest-secrets",
			Namespace: "testns",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"s3.conf": []byte("test")},
	}

	cl := fake.NewClientBuilder().
		WithScheme(buildCoreScheme(t)).
		WithObjects(existingSecret).
		Build()

	assert.NoError(t,
		stripPGBackrestSecretOwnerRef(t.Context(), cl, existingSecret.Name, existingSecret.Namespace, db),
	)

	// Verify the secret is unmodified.
	updated := &corev1.Secret{}
	require.NoError(t, cl.Get(t.Context(), types.NamespacedName{
		Name:      existingSecret.Name,
		Namespace: existingSecret.Namespace,
	}, updated))
	assert.Empty(t, updated.GetOwnerReferences())
}
