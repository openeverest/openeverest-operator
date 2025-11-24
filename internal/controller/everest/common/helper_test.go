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

package common

import (
	"testing"

	"github.com/AlekSi/pointer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

func TestGetOperatorVersion(t *testing.T) {
	t.Parallel()
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "percona-xtradb-cluster-operator",
			Namespace: "super-x",
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Image: "percona/percona-xtradb-cluster-operator:1.12.0",
						},
					},
				},
			},
		},
	}
	cl := fake.NewClientBuilder().WithObjects(deployment).Build()
	s := scheme.Scheme
	s.AddKnownTypes(everestv1alpha1.GroupVersion, &everestv1alpha1.DatabaseCluster{})
	version, err := GetOperatorVersion(
		t.Context(),
		cl,
		types.NamespacedName{
			Namespace: "super-x",
			Name:      "percona-xtradb-cluster-operator",
		})
	require.NoError(t, err)
	assert.Equal(t, "1.12.0", version.String())
	assert.NotEqual(t, "1.11.0", version.String())

	_, err = GetOperatorVersion(
		t.Context(),
		cl,
		types.NamespacedName{
			Namespace: "non-existent",
			Name:      "percona-xtradb-cluster-operator",
		})
	require.Error(t, err)
}

func TestMergeMapSimple(t *testing.T) {
	t.Parallel()
	testDst := map[string]interface{}{
		"a": "apple",
		"b": "banana",
	}
	src := map[string]interface{}{
		"a": "avocado",
		"c": "cherry",
	}
	expDst := map[string]interface{}{
		"a": "avocado",
		"b": "banana",
		"c": "cherry",
	}
	err := mergeMap(testDst, src)
	require.NoError(t, err)
	assert.Equal(t, expDst, testDst)
}

func TestMergeMapNested(t *testing.T) {
	t.Parallel()
	testDst := map[string]interface{}{
		"a": "apple",
		"b": "banana",
		"dry": map[string]interface{}{
			"a": "almond",
			"p": "peanut",
		},
	}
	src := map[string]interface{}{
		"a": "avocado",
		"c": "cherry",
		"dry": map[string]interface{}{
			"c": "cashew",
			"p": "pecan",
		},
		"vegetables": map[string]interface{}{
			"a": "aspargus",
			"b": "beet",
		},
	}
	expDst := map[string]interface{}{
		"a": "avocado",
		"b": "banana",
		"c": "cherry",
		"dry": map[string]interface{}{
			"a": "almond",
			"c": "cashew",
			"p": "pecan",
		},
		"vegetables": map[string]interface{}{
			"a": "aspargus",
			"b": "beet",
		},
	}
	err := mergeMap(testDst, src)
	require.NoError(t, err)
	assert.Equal(t, expDst, testDst)
}

func TestMergeMapError(t *testing.T) {
	t.Parallel()
	testDst := map[string]interface{}{
		"dry": map[string]interface{}{
			"vegetables": map[string]interface{}{
				"a": 1,
			},
		},
	}
	src := map[string]interface{}{
		"dry": map[string]interface{}{
			"vegetables": map[string]interface{}{
				"a": "avocado",
			},
		},
	}
	err := mergeMap(testDst, src)
	require.Error(t, err)
}

func TestConfigureStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		db                         *everestv1alpha1.DatabaseCluster
		currentSize                resource.Quantity
		storageClassExists         bool
		storageClassAllowExpansion bool
		wantSize                   resource.Quantity
		expectFailureCond          bool
		wantFailureCondReason      string
		expectErr                  bool
	}{
		{
			name: "initial storage setup",
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-db",
					Namespace:  "default",
					Generation: 1,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Storage: everestv1alpha1.Storage{
							Size:  resource.MustParse("10Gi"),
							Class: pointer.To("standard"),
						},
					},
				},
			},
			currentSize:                resource.MustParse("0"),
			storageClassExists:         true,
			storageClassAllowExpansion: true,
			wantSize:                   resource.MustParse("10Gi"),
		},
		{
			name: "successful volume expansion",
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-db",
					Namespace:  "default",
					Generation: 1,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Storage: everestv1alpha1.Storage{
							Size:  resource.MustParse("20Gi"),
							Class: pointer.To("standard"),
						},
					},
				},
			},
			currentSize:                resource.MustParse("10Gi"),
			storageClassExists:         true,
			storageClassAllowExpansion: true,
			wantSize:                   resource.MustParse("20Gi"),
		},
		{
			name: "volume shrink not allowed",
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-db",
					Namespace:  "default",
					Generation: 1,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Storage: everestv1alpha1.Storage{
							Size:  resource.MustParse("10Gi"),
							Class: pointer.To("standard"),
						},
					},
				},
			},
			currentSize:                resource.MustParse("20Gi"),
			storageClassExists:         true,
			storageClassAllowExpansion: true,
			wantSize:                   resource.MustParse("20Gi"),
			expectFailureCond:          true,
			wantFailureCondReason:      everestv1alpha1.ReasonCannotShrinkVolume,
		},
		{
			name: "storage class doesn't support expansion",
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-db",
					Namespace:  "default",
					Generation: 1,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Storage: everestv1alpha1.Storage{
							Size:  resource.MustParse("20Gi"),
							Class: pointer.To("standard"),
						},
					},
				},
			},
			currentSize:                resource.MustParse("10Gi"),
			storageClassExists:         true,
			storageClassAllowExpansion: false,
			wantSize:                   resource.MustParse("10Gi"),
			expectFailureCond:          true,
			wantFailureCondReason:      everestv1alpha1.ReasonStorageClassDoesNotSupportExpansion,
		},
		{
			name: "storage class not found",
			db: &everestv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-db",
					Namespace:  "default",
					Generation: 1,
				},
				Spec: everestv1alpha1.DatabaseClusterSpec{
					Engine: everestv1alpha1.Engine{
						Storage: everestv1alpha1.Storage{
							Size:  resource.MustParse("20Gi"),
							Class: pointer.To("non-existent"),
						},
					},
				},
			},
			currentSize:        resource.MustParse("10Gi"),
			storageClassExists: false,
			expectErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup test objects
			var actualSize resource.Quantity
			setSize := func(size resource.Quantity) {
				actualSize = size
			}

			// Setup fake client with storage class if needed
			builder := fake.NewClientBuilder().WithScheme(scheme.Scheme)
			if tt.storageClassExists && tt.db.Spec.Engine.Storage.Class != nil {
				sc := &storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: *tt.db.Spec.Engine.Storage.Class,
					},
					AllowVolumeExpansion: &tt.storageClassAllowExpansion,
				}
				builder.WithObjects(sc)
			}
			client := builder.Build()

			// Run the test
			err := ConfigureStorage(t.Context(), client, tt.db, tt.currentSize, setSize)

			// Verify results
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Check if the size was set correctly
			assert.Equal(t, tt.wantSize, actualSize, "unexpected storage size")

			// Check conditions if expected
			if tt.expectFailureCond {
				cond := meta.FindStatusCondition(tt.db.Status.Conditions, everestv1alpha1.ConditionTypeCannotResizeVolume)
				assert.Equal(t, tt.wantFailureCondReason, cond.Reason)
				assert.Equal(t, tt.db.Generation, cond.ObservedGeneration)
				assert.NotEmpty(t, cond.LastTransitionTime)
			} else {
				assert.Empty(t, tt.db.Status.Conditions, "expected no conditions")
			}
		})
	}
}

func TestVerifyPVCResizeFailure(t *testing.T) {
	t.Parallel()

	pvcList := &corev1.PersistentVolumeClaimList{
		Items: []corev1.PersistentVolumeClaim{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc-1",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/instance": "test-db-1",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Conditions: []corev1.PersistentVolumeClaimCondition{},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc-2",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/instance": "test-db-2",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Conditions: []corev1.PersistentVolumeClaimCondition{
						{
							Type:   corev1.PersistentVolumeClaimResizing,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc-3",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/instance": "test-db-3",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Conditions: []corev1.PersistentVolumeClaimCondition{
						{
							Type:    corev1.PersistentVolumeClaimControllerResizeError,
							Status:  corev1.ConditionTrue,
							Message: "resize failed",
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc-4",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/instance": "test-db-3",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Conditions: []corev1.PersistentVolumeClaimCondition{
						{
							Type:   corev1.PersistentVolumeClaimResizing,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc-5",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/instance": "test-db-4",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Conditions: []corev1.PersistentVolumeClaimCondition{
						{
							Type:    corev1.PersistentVolumeClaimNodeResizeError,
							Status:  corev1.ConditionTrue,
							Message: "resize failed",
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc-6",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/instance": "test-db-4",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Conditions: []corev1.PersistentVolumeClaimCondition{
						{
							Type:   corev1.PersistentVolumeClaimResizing,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name         string
		dbName       string
		expectedFail bool
		expectedMsg  string
	}{
		{
			name:         "pvc not resizing",
			dbName:       "test-db-1",
			expectedFail: false,
			expectedMsg:  "",
		},
		{
			name:         "pvc resizing",
			dbName:       "test-db-2",
			expectedFail: false,
			expectedMsg:  "",
		},
		{
			name:         "pvc resize controller error",
			dbName:       "test-db-3",
			expectedFail: true,
			expectedMsg:  "resize failed",
		},
		{
			name:         "pvc resize node error",
			dbName:       "test-db-4",
			expectedFail: true,
			expectedMsg:  "resize failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			objects := make([]client.Object, len(pvcList.Items))
			for i := range pvcList.Items {
				obj, ok := pvcList.Items[i].DeepCopyObject().(client.Object)
				require.True(t, ok)
				objects[i] = obj
			}
			fakeClient := fake.NewClientBuilder().WithObjects(objects...).Build()
			failed, message, err := VerifyPVCResizeFailure(t.Context(), fakeClient, tt.dbName, "default")

			require.NoError(t, err)
			assert.Equal(t, tt.expectedFail, failed)
			assert.Equal(t, tt.expectedMsg, message)
		})
	}
}
