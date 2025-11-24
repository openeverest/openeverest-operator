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

package pxc

import (
	"errors"
	"fmt"
	"testing"

	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

func TestAddBackupStorages(t *testing.T) {
	t.Parallel()
	ns := "some_namespace"
	dbName := "test"
	uid := "some_uid"
	testPXCSpec := &pxcv1.BackupStorageSpec{
		Type: pxcv1.BackupStorageS3,
		S3: &pxcv1.BackupStorageS3Spec{
			Bucket: fmt.Sprintf("/%s/%s", dbName, uid),
		},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1G"),
				corev1.ResourceCPU:    resource.MustParse("600m"),
			},
		},
	}

	backup1 := everestv1alpha1.DatabaseClusterBackup{ObjectMeta: metav1.ObjectMeta{Name: "backup1", Namespace: ns}, Spec: everestv1alpha1.DatabaseClusterBackupSpec{
		BackupStorageName: "storage-1",
	}}
	backup2 := everestv1alpha1.DatabaseClusterBackup{ObjectMeta: metav1.ObjectMeta{Name: "backup2", Namespace: ns}, Spec: everestv1alpha1.DatabaseClusterBackupSpec{
		BackupStorageName: "storage-2",
	}}
	storage1 := everestv1alpha1.BackupStorage{ObjectMeta: metav1.ObjectMeta{Name: "storage-1", Namespace: ns}, Spec: everestv1alpha1.BackupStorageSpec{
		Type: everestv1alpha1.BackupStorageTypeS3,
	}}
	storage2 := everestv1alpha1.BackupStorage{ObjectMeta: metav1.ObjectMeta{Name: "storage-2", Namespace: ns}, Spec: everestv1alpha1.BackupStorageSpec{
		Type: everestv1alpha1.BackupStorageTypeS3,
	}}

	appl := mockApplier(t,
		metav1.ObjectMeta{
			Name:      dbName,
			UID:       types.UID(uid),
			Namespace: ns,
		},
		&backup1, &backup2, &storage1, &storage2)

	type tcase struct {
		name       string
		backups    []everestv1alpha1.DatabaseClusterBackup
		dataSource *everestv1alpha1.DataSource
		expected   map[string]*pxcv1.BackupStorageSpec
		backup     everestv1alpha1.DatabaseClusterBackup
		error      error
	}
	tcases := []tcase{
		{
			name:       "no backups no datasource",
			backups:    []everestv1alpha1.DatabaseClusterBackup{},
			dataSource: nil,
			expected:   map[string]*pxcv1.BackupStorageSpec{},
			error:      nil,
		},
		{
			name:    "no backups dataSource with backupSource",
			backups: []everestv1alpha1.DatabaseClusterBackup{},
			dataSource: &everestv1alpha1.DataSource{BackupSource: &everestv1alpha1.BackupSource{
				Path:              "some/path",
				BackupStorageName: "storage-1",
			}},
			expected: map[string]*pxcv1.BackupStorageSpec{
				"storage-1": testPXCSpec,
			},
			error: nil,
		},
		{
			name:    "no backups dataSource with backupName",
			backups: []everestv1alpha1.DatabaseClusterBackup{},
			dataSource: &everestv1alpha1.DataSource{
				DBClusterBackupName: "backup1",
			},
			expected: map[string]*pxcv1.BackupStorageSpec{
				"storage-1": testPXCSpec,
			},
			error: nil,
		},
		{
			name:    "err no such backup",
			backups: []everestv1alpha1.DatabaseClusterBackup{},
			dataSource: &everestv1alpha1.DataSource{
				DBClusterBackupName: "backup3",
			},
			expected: nil,
			error:    errors.New(`databaseclusterbackups.everest.percona.com "backup3" not found`),
		},
		{
			name:       "none of the backup name and backupSource defined",
			backups:    []everestv1alpha1.DatabaseClusterBackup{},
			dataSource: &everestv1alpha1.DataSource{BackupSource: &everestv1alpha1.BackupSource{}},
			expected:   nil,
			error:      errInvalidDataSourceConfiguration,
		},
		{
			name:       "one backup no backupSource",
			backups:    []everestv1alpha1.DatabaseClusterBackup{backup1},
			dataSource: nil,
			expected: map[string]*pxcv1.BackupStorageSpec{
				"storage-1": testPXCSpec,
			},
			error: nil,
		},
		{
			name:       "two backups different storages no backupSource",
			backups:    []everestv1alpha1.DatabaseClusterBackup{backup1, backup2},
			dataSource: nil,
			expected: map[string]*pxcv1.BackupStorageSpec{
				"storage-1": testPXCSpec,
				"storage-2": testPXCSpec,
			},
			error: nil,
		},
	}
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			storages := map[string]*pxcv1.BackupStorageSpec{}
			err := appl.addBackupStorages(tc.backups, tc.dataSource, storages)
			if tc.error != nil {
				require.Error(t, err)
				assert.Equal(t, tc.error.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, storages)
		})
	}
}

func mockApplier(t *testing.T, dbMeta metav1.ObjectMeta, objs ...client.Object) applier {
	t.Helper()
	ctx := t.Context()
	scheme := runtime.NewScheme()
	utilruntime.Must(everestv1alpha1.AddToScheme(scheme))
	utilruntime.Must(pxcv1.SchemeBuilder.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(storagev1.AddToScheme(scheme))

	mockDatabase := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: dbMeta,
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Engine: everestv1alpha1.Engine{Version: "8.0.0"},
			Proxy:  everestv1alpha1.Proxy{Type: "haproxy"},
		},
	}

	engine := everestv1alpha1.DatabaseEngine{ObjectMeta: metav1.ObjectMeta{Name: consts.PXCDeploymentName, Namespace: mockDatabase.GetNamespace()}, Spec: everestv1alpha1.DatabaseEngineSpec{}}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "percona-xtradb-cluster-operator",
			Namespace: "some_namespace",
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
	storageClassAWS := storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "aws-storage"},
		Provisioner: "kubernetes.io/aws-ebs",
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&engine, &deployment, &storageClassAWS).WithObjects(objs...).Build()

	dbEngine, err := common.GetDatabaseEngine(ctx, cl, consts.PXCDeploymentName, mockDatabase.GetNamespace())
	require.NoError(t, err)
	p, err := New(ctx, providers.ProviderOptions{
		C:        cl,
		DB:       mockDatabase,
		DBEngine: dbEngine,
	})
	require.NoError(t, err)
	return applier{
		Provider: p,
		ctx:      ctx,
	}
}

//nolint:maintidx
func TestGetPMMResources(t *testing.T) {
	t.Parallel()

	type res struct {
		memory string
		cpu    string
	}

	type resourceString struct {
		requests res
		limits   res
	}

	tests := []struct {
		name           string
		isNewDBCluster bool
		dbSpec         *everestv1alpha1.DatabaseClusterSpec
		curPxcSpec     *pxcv1.PerconaXtraDBClusterSpec
		want           corev1.ResourceRequirements
		wantString     resourceString
	}{
		// enable monitoring for new DB cluster w/o requested resources
		{
			name:           "New small cluster w/o requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
			},
			curPxcSpec: nil,
			want:       common.PmmResourceRequirementsSmall,
			wantString: resourceString{
				requests: res{
					memory: "99604Ki",
					cpu:    "95m",
				},
			},
		},
		{
			name:           "New medium cluster w/o requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPxcSpec: nil,
			want:       common.PmmResourceRequirementsMedium,
			wantString: resourceString{
				requests: res{
					memory: "199168Ki",
					cpu:    "228m",
				},
			},
		},
		{
			name:           "New large cluster w/o requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
			},
			curPxcSpec: nil,
			want:       common.PmmResourceRequirementsLarge,
			wantString: resourceString{
				requests: res{
					memory: "796907Ki",
					cpu:    "228m",
				},
			},
		},
		// enable monitoring for new DB cluster with requested resources
		{
			name:           "New small cluster with requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("100m"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("110M"),
						},
					},
				},
			},
			curPxcSpec: nil,
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("99604Ki"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("110M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "99604Ki",
					cpu:    "100m",
				},
				limits: res{
					memory: "110M",
				},
			},
		},
		{
			name:           "New medium cluster with requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("300M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("300m"),
						},
					},
				},
			},
			curPxcSpec: nil,
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("228m"),
					corev1.ResourceMemory: resource.MustParse("300M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("300m"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "300M",
					cpu:    "228m",
				},
				limits: res{
					cpu: "300m",
				},
			},
		},
		{
			name:           "New large cluster with requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("500M"),
						},
					},
				},
			},
			curPxcSpec: nil,
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("228m"),
					corev1.ResourceMemory: resource.MustParse("796907Ki"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("500M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "796907Ki",
					cpu:    "228m",
				},
				limits: res{
					memory: "500M",
					cpu:    "500m",
				},
			},
		},
		// enable monitoring for existing DB cluster w/o monitoring and w/o requested resources
		{
			name:           "Existing small cluster w/o monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("1G"),
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsSmall,
			wantString: resourceString{
				requests: res{
					memory: "99604Ki",
					cpu:    "95m",
				},
			},
		},
		{
			name:           "Existing medium cluster w/o monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("8G"),
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsMedium,
			wantString: resourceString{
				requests: res{
					memory: "199168Ki",
					cpu:    "228m",
				},
			},
		},
		{
			name:           "Existing large cluster w/o monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("32G"),
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsLarge,
			wantString: resourceString{
				requests: res{
					memory: "796907Ki",
					cpu:    "228m",
				},
			},
		},
		// enable monitoring for existing DB cluster w/o monitoring and with requested resources
		{
			name:           "Existing small cluster w/o monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("100M"),
						},
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("1G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("100M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "100M",
					cpu:    "100m",
				},
			},
		},
		{
			name:           "Existing medium cluster w/o monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("300m"),
							corev1.ResourceMemory: resource.MustParse("300M"),
						},
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("8G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("300m"),
					corev1.ResourceMemory: resource.MustParse("300M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "300M",
					cpu:    "300m",
				},
			},
		},
		{
			name:           "Existing large cluster w/o monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("500M"),
						},
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("32G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("500M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "500M",
					cpu:    "500m",
				},
			},
		},
		// enable monitoring and change DB engine size for existing DB cluster w/o monitoring and w/o requested resources
		{
			name:           "Existing small->medium cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("1G"),
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsMedium,
			wantString: resourceString{
				requests: res{
					memory: "199168Ki",
					cpu:    "228m",
				},
			},
		},
		{
			name:           "Existing medium->large cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("8G"),
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsLarge,
			wantString: resourceString{
				requests: res{
					memory: "796907Ki",
					cpu:    "228m",
				},
			},
		},
		{
			name:           "Existing large->medium cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("32G"),
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsMedium,
			wantString: resourceString{
				requests: res{
					memory: "199168Ki",
					cpu:    "228m",
				},
			},
		},
		// enable monitoring and change DB engine size for existing DB cluster w/o monitoring and with requested resources
		{
			name:           "Existing small->medium cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("100M"),
						},
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("1G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("100M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "100M",
					cpu:    "100m",
				},
			},
		},
		{
			name:           "Existing medium->large cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("300m"),
							corev1.ResourceMemory: resource.MustParse("300M"),
						},
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("8G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("300m"),
					corev1.ResourceMemory: resource.MustParse("300M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "300M",
					cpu:    "300m",
				},
			},
		},
		{
			name:           "Existing large ->medium cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("500M"),
						},
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: false,
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("32G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("500M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "500M",
					cpu:    "500m",
				},
			},
		},
		// enable monitoring for existing DB cluster with monitoring and w/o requested resources - keep current resources
		{
			name:           "Existing small cluster with monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("110m"),
							corev1.ResourceMemory: resource.MustParse("110M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("115m"),
							corev1.ResourceMemory: resource.MustParse("115M"),
						},
					},
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("1G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("110m"),
					corev1.ResourceMemory: resource.MustParse("110M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("115m"),
					corev1.ResourceMemory: resource.MustParse("115M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "110M",
					cpu:    "110m",
				},
				limits: res{
					memory: "115M",
					cpu:    "115m",
				},
			},
		},
		{
			name:           "Existing medium cluster with monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("220m"),
							corev1.ResourceMemory: resource.MustParse("220M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("225m"),
							corev1.ResourceMemory: resource.MustParse("225M"),
						},
					},
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("8G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("220m"),
					corev1.ResourceMemory: resource.MustParse("220M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("225m"),
					corev1.ResourceMemory: resource.MustParse("225M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "220M",
					cpu:    "220m",
				},
				limits: res{
					memory: "225M",
					cpu:    "225m",
				},
			},
		},
		{
			name:           "Existing large cluster with monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("330m"),
							corev1.ResourceMemory: resource.MustParse("330M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("335m"),
							corev1.ResourceMemory: resource.MustParse("335M"),
						},
					},
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("32G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("330m"),
					corev1.ResourceMemory: resource.MustParse("330M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("335m"),
					corev1.ResourceMemory: resource.MustParse("335M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "330M",
					cpu:    "330m",
				},
				limits: res{
					memory: "335M",
					cpu:    "335m",
				},
			},
		},
		// enable monitoring for existing DB cluster with monitoring and with requested resources - merge resources
		{
			name:           "Existing small cluster with monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("111m"),
							corev1.ResourceMemory: resource.MustParse("111M"),
						},
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("110m"),
							corev1.ResourceMemory: resource.MustParse("110M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("115m"),
							corev1.ResourceMemory: resource.MustParse("115M"),
						},
					},
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("1G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("111m"),
					corev1.ResourceMemory: resource.MustParse("111M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("115m"),
					corev1.ResourceMemory: resource.MustParse("115M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "111M",
					cpu:    "111m",
				},
				limits: res{
					memory: "115M",
					cpu:    "115m",
				},
			},
		},
		{
			name:           "Existing medium cluster with monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("222m"),
							corev1.ResourceMemory: resource.MustParse("222M"),
						},
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("220m"),
							corev1.ResourceMemory: resource.MustParse("220M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("225m"),
							corev1.ResourceMemory: resource.MustParse("225M"),
						},
					},
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("8G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("222m"),
					corev1.ResourceMemory: resource.MustParse("222M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("225m"),
					corev1.ResourceMemory: resource.MustParse("225M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "222M",
					cpu:    "222m",
				},
				limits: res{
					memory: "225M",
					cpu:    "225m",
				},
			},
		},
		{
			name:           "Existing large cluster with monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("333m"),
							corev1.ResourceMemory: resource.MustParse("333M"),
						},
					},
				},
			},
			curPxcSpec: &pxcv1.PerconaXtraDBClusterSpec{
				PMM: &pxcv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("330m"),
							corev1.ResourceMemory: resource.MustParse("330M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("335m"),
							corev1.ResourceMemory: resource.MustParse("335M"),
						},
					},
				},
				PXC: &pxcv1.PXCSpec{
					PodSpec: &pxcv1.PodSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("32G"),
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("333m"),
					corev1.ResourceMemory: resource.MustParse("333M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("335m"),
					corev1.ResourceMemory: resource.MustParse("335M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "333M",
					cpu:    "333m",
				},
				limits: res{
					memory: "335M",
					cpu:    "335m",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			calculatedResources := getPMMResources(tt.isNewDBCluster, tt.dbSpec, tt.curPxcSpec)
			assert.True(t, tt.want.Requests.Cpu().Equal(*calculatedResources.Requests.Cpu()))
			assert.True(t, tt.want.Requests.Memory().Equal(*calculatedResources.Requests.Memory()))
			assert.True(t, tt.want.Limits.Cpu().Equal(*calculatedResources.Limits.Cpu()))
			assert.True(t, tt.want.Limits.Memory().Equal(*calculatedResources.Limits.Memory()))

			if tt.wantString.requests.memory != "" {
				assert.Equal(t, tt.wantString.requests.memory, calculatedResources.Requests.Memory().String())
			}

			if tt.wantString.requests.cpu != "" {
				assert.Equal(t, tt.wantString.requests.cpu, calculatedResources.Requests.Cpu().String())
			}

			if tt.wantString.limits.memory != "" {
				assert.Equal(t, tt.wantString.limits.memory, calculatedResources.Limits.Memory().String())
			}

			if tt.wantString.limits.cpu != "" {
				assert.Equal(t, tt.wantString.limits.cpu, calculatedResources.Limits.Cpu().String())
			}
		})
	}
}
