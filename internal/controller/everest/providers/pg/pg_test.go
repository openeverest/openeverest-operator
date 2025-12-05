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

//nolint:dupl,goconst
package pg

import (
	"reflect"
	"testing"

	"github.com/AlekSi/pointer"
	pgv2 "github.com/percona/percona-postgresql-operator/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/pkg/apis/postgres-operator.crunchydata.com/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

func TestPGConfigParser_ParsePGConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  string
		want    map[string]any
		wantErr bool
	}{
		{
			name:   "parse one value",
			config: "name = value",
			want: map[string]any{
				"name": "value",
			},
			wantErr: false,
		},
		{
			name:   "parse one value with new line",
			config: "name = value\n",
			want: map[string]any{
				"name": "value",
			},
			wantErr: false,
		},
		{
			name: "parse many values",
			config: `
			name1 = value1
			name2 = value2
			name3 = value3
			`,
			want: map[string]any{
				"name1": "value1",
				"name2": "value2",
				"name3": "value3",
			},
			wantErr: false,
		},
		{
			name: "parse many values mixed spaces and equal signs",
			config: `
			name1 = value1
			name2 value2
			name3 = value3
			`,
			want: map[string]any{
				"name1": "value1",
				"name2": "value2",
				"name3": "value3",
			},
			wantErr: false,
		},
		{
			name: "parse complex config",
			config: `
			name1 = value1
			name2 value2
			# comment
			name3 = value3 # comment

			# comment
			name4 value4
			# name5 value5

			`,
			want: map[string]any{
				"name1": "value1",
				"name2": "value2",
				"name3": "value3",
				"name4": "value4",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &ConfigParser{
				config: tt.config,
			}
			got, err := p.ParsePGConfig()
			if err != nil {
				t.Errorf("ConfigParser.ParsePGConfig() error = %v wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConfigParser.parsePGConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigParser_lineUsesEqualSign(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		line     string
		useEqual bool
	}{
		{
			name:     "equal - standard",
			line:     "name = value",
			useEqual: true,
		},
		{
			name:     "equal - no spaces",
			line:     "name=value",
			useEqual: true,
		},
		{
			name:     "equal - no spaces but space in value",
			line:     "name=value abc",
			useEqual: true,
		},
		{
			name:     "equal - no spaces before",
			line:     "name= value",
			useEqual: true,
		},
		{
			name:     "equal - no spaces after",
			line:     "name =value",
			useEqual: true,
		},
		{
			name:     "equal - many spaces",
			line:     "name   =    value",
			useEqual: true,
		},
		{
			name:     "equal - many spaces before",
			line:     "name   = value",
			useEqual: true,
		},
		{
			name:     "equal - many spaces after",
			line:     "name =   value",
			useEqual: true,
		},
		{
			name:     "no equal - standard",
			line:     "name value",
			useEqual: false,
		},
		{
			name:     "no equal - more spaces",
			line:     "name  value",
			useEqual: false,
		},
		{
			name:     "no equal - equal in value with space",
			line:     "name value =",
			useEqual: false,
		},
		{
			name:     "no equal - equal in value with no space",
			line:     "name value=",
			useEqual: false,
		},
		{
			name:     "no equal - multiple spaces, equal in value with no space",
			line:     "name  value=",
			useEqual: false,
		},
		{
			name:     "no equal - multiple spaces, equal in value with space",
			line:     "name  value =",
			useEqual: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &ConfigParser{config: ""}
			got := p.lineUsesEqualSign([]byte(tt.line))
			if tt.useEqual != got {
				t.Errorf("Did not detect equal sign properly for test %s %q", tt.name, tt.line)
			}
		})
	}
}

func TestReconcilePGBackRestReposEmptyAddRequest(t *testing.T) {
	t.Parallel()
	testRepos := []crunchyv1beta1.PGBackRestRepo{}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposEmptyAddSchedule(t *testing.T) {
	t.Parallel()
	testRepos := []crunchyv1beta1.PGBackRestRepo{}
	testSchedule := "0 0 * * *"
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageOneRequestAddRequest(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageOneRequestAddSchedule(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
		},
	}
	testSchedule := "0 0 * * *"
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageOneScheduleAddRequest(t *testing.T) {
	t.Parallel()
	testSchedule := "0 0 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageOneScheduleAddSchedule(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testSchedule2 := "0 1 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule1,
			BackupStorageName: "backupStorage1",
		},
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule2,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposDifferentStorageOneScheduleAddRequest(t *testing.T) {
	t.Parallel()
	testSchedule := "0 0 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage2",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
		"backupStorage2": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket2",
				Region:      "region2",
				EndpointURL: "endpoint2",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
		"backupStorage2": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket2",
				Region:   "region2",
				Endpoint: "endpoint2",
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposDifferentStorageOneScheduleAddSchedule(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testSchedule2 := "0 1 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule1,
			BackupStorageName: "backupStorage1",
		},
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule2,
			BackupStorageName: "backupStorage2",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
		"backupStorage2": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket2",
				Region:      "region2",
				EndpointURL: "endpoint2",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
		"backupStorage2": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket2",
				Region:   "region2",
				Endpoint: "endpoint2",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposDifferentStorageOneScheduleAddScheduleNoOrder(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testSchedule2 := "0 1 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket2",
				Region:   "region2",
				Endpoint: "endpoint2",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule1,
			BackupStorageName: "backupStorage1",
		},
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule2,
			BackupStorageName: "backupStorage2",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
		"backupStorage2": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket2",
				Region:      "region2",
				EndpointURL: "endpoint2",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
		"backupStorage2": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket2",
				Region:   "region2",
				Endpoint: "endpoint2",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposDifferentStorageOneScheduleAddRequestNoOrder(t *testing.T) {
	t.Parallel()
	testSchedule2 := "0 1 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket2",
				Region:   "region2",
				Endpoint: "endpoint2",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule2,
			BackupStorageName: "backupStorage2",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
		"backupStorage2": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket2",
				Region:      "region2",
				EndpointURL: "endpoint2",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
		"backupStorage2": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket2",
				Region:   "region2",
				Endpoint: "endpoint2",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageOneScheduleChangeSchedule(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testSchedule2 := "0 1 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule2,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageOneScheduleChangeScheduleAddSchedule(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testSchedule2 := "0 1 * * *"
	testSchedule3 := "0 2 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule2,
			BackupStorageName: "backupStorage1",
		},
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule3,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule3,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageOneScheduleDeleteScheduleAddRequest(t *testing.T) {
	t.Parallel()
	testSchedule := "0 0 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageTwoSchedulesDelete2ndSchedule(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testSchedule2 := "0 1 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule1,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageTwoSchedulesDelete1stSchedule(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testSchedule2 := "0 1 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule2,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposOneScheduleDeleteSchedule(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{}
	testBackupStoragesSecrets := map[string]*corev1.Secret{}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposOneRequestDeleteRequest(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{}
	testBackupStoragesSecrets := map[string]*corev1.Secret{}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageThreeSchedulesAddSchedule(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testSchedule2 := "0 1 * * *"
	testSchedule3 := "0 2 * * *"
	testSchedule4 := "0 3 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
		{
			Name: "repo4",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule3,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule1,
			BackupStorageName: "backupStorage1",
		},
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule2,
			BackupStorageName: "backupStorage1",
		},
		{
			Enabled:           true,
			Name:              "schedule3",
			Schedule:          testSchedule3,
			BackupStorageName: "backupStorage1",
		},
		{
			Enabled:           true,
			Name:              "schedule4",
			Schedule:          testSchedule4,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposDifferentStorageThreeRequestsAddRequest(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket2",
				Region:   "region2",
				Endpoint: "endpoint2",
			},
		},
		{
			Name: "repo4",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket3",
				Region:   "region3",
				Endpoint: "endpoint3",
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage2",
			},
		},
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage3",
			},
		},
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage4",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
		"backupStorage2": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket2",
				Region:      "region2",
				EndpointURL: "endpoint2",
			},
		},
		"backupStorage3": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket3",
				Region:      "region3",
				EndpointURL: "endpoint3",
			},
		},
		"backupStorage4": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket4",
				Region:      "region4",
				EndpointURL: "endpoint4",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
		"backupStorage2": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
		"backupStorage3": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
		"backupStorage4": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposSameStorageThreeSchedulesAddRequest(t *testing.T) {
	t.Parallel()
	testSchedule1 := "0 0 * * *"
	testSchedule2 := "0 1 * * *"
	testSchedule3 := "0 2 * * *"
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
		{
			Name: "repo4",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule3,
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule1,
			BackupStorageName: "backupStorage1",
		},
		{
			Enabled:           true,
			Name:              "schedule2",
			Schedule:          testSchedule2,
			BackupStorageName: "backupStorage1",
		},
		{
			Enabled:           true,
			Name:              "schedule3",
			Schedule:          testSchedule3,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"backupStorage1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:        everestv1alpha1.BackupStorageTypeS3,
				Bucket:      "bucket1",
				Region:      "region1",
				EndpointURL: "endpoint1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"backupStorage1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule1,
			},
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule2,
			},
		},
		{
			Name: "repo4",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: &testSchedule3,
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposUnknownStorageRequest(t *testing.T) {
	t.Parallel()
	testRepos := []crunchyv1beta1.PGBackRestRepo{}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "backupStorage1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{}
	testBackupStoragesSecrets := map[string]*corev1.Secret{}
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposScheduleAfterOnDemandToAnotherStorage(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	s3Repo2 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket1",
	}
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
		},
		{
			Name: "repo2",
			S3:   s3Repo2,
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{{
		Enabled:           true,
		Name:              "sched1",
		Schedule:          "20 * * * *",
		BackupStorageName: "bs2",
	}}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{{
		Spec: everestv1alpha1.DatabaseClusterBackupSpec{
			BackupStorageName: "bs1",
		},
	}}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"bs1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket1",
			},
		},
		"bs2": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket2",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"bs1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		}, "bs2": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3:   s3Repo2,
		},
		{
			Name: "repo3",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: &crunchyv1beta1.RepoS3{
				Bucket: "bucket2",
			},
		},
	}

	repos, _, _, _ := reconcilePGBackRestRepos( //nolint:dogsled
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposOnDemandAfterScheduleToAnotherStorage(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	s3Repo2 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket1",
	}
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
		},
		{
			Name: "repo2",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: s3Repo2,
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{{
		Enabled:           true,
		Name:              "sched1",
		Schedule:          "20 * * * *",
		BackupStorageName: "bs1",
	}}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{{
		Spec: everestv1alpha1.DatabaseClusterBackupSpec{
			BackupStorageName: "bs2",
		},
	}}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"bs1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket1",
			},
		},
		"bs2": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket2",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"bs1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		}, "bs2": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: s3Repo2,
		},
		{
			Name: "repo3",
			S3: &crunchyv1beta1.RepoS3{
				Bucket: "bucket2",
			},
		},
	}

	repos, _, _, _ := reconcilePGBackRestRepos( //nolint:dogsled
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposScheduleAfter3OnDemands(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	s3Repo2 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket1",
	}
	s3Repo3 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket2",
	}
	s3Repo4 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket3",
	}
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
		},
		{
			Name: "repo2",
			S3:   s3Repo2,
		},
		{
			Name: "repo3",
			S3:   s3Repo3,
		},
		{
			Name: "repo4",
			S3:   s3Repo4,
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{{
		Enabled:           true,
		Name:              "sched1",
		Schedule:          "20 * * * *",
		BackupStorageName: "bs2",
	}}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs3",
			},
		},
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs2",
			},
		},
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"bs1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket1",
			},
		},
		"bs2": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket2",
			},
		},
		"bs3": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket3",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"bs1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		}, "bs2": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
		"bs3": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3:   s3Repo2,
		},
		{
			Name: "repo3",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: s3Repo3,
		},
		{
			Name: "repo4",
			S3:   s3Repo4,
		},
	}

	repos, _, _, _ := reconcilePGBackRestRepos( //nolint:dogsled
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposOnDemand3OnDemandsAndSchedule(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	s3Repo2 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket1",
	}
	s3Repo3 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket2",
	}
	s3Repo4 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket3",
	}
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
		},
		{
			Name: "repo2",
			S3:   s3Repo2,
		},
		{
			Name: "repo3",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: s3Repo3,
		},
		{
			Name: "repo4",
			S3:   s3Repo4,
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{{
		Enabled:           true,
		Name:              "sched1",
		Schedule:          "20 * * * *",
		BackupStorageName: "bs2",
	}}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs3",
			},
		},
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs2",
			},
		},
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs1",
			},
		},
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs3",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"bs1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket1",
			},
		},
		"bs2": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket2",
			},
		},
		"bs3": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket3",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"bs1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		}, "bs2": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
		"bs3": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			S3:   s3Repo2,
		},
		{
			Name: "repo3",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: s3Repo3,
		},
		{
			Name: "repo4",
			S3:   s3Repo4,
		},
	}

	repos, _, _, _ := reconcilePGBackRestRepos( //nolint:dogsled
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestBackupAndScheduleToTheSameStorage(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	s3Repo2 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket1",
	}
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
		},
		{
			Name: "repo2",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: s3Repo2,
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{{
		Enabled:           true,
		Name:              "sched1",
		Schedule:          "20 * * * *",
		BackupStorageName: "bs1",
	}}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"bs1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"bs1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: s3Repo2,
		},
	}

	repos, _, _, _ := reconcilePGBackRestRepos( //nolint:dogsled
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestNewBackupAndNewScheduleToTheSameStorage(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	s3Repo2 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket1",
	}
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{{
		Enabled:           true,
		Name:              "sched1",
		Schedule:          "20 * * * *",
		BackupStorageName: "bs1",
	}}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"bs1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"bs1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: s3Repo2,
		},
	}

	repos, _, _, _ := reconcilePGBackRestRepos( //nolint:dogsled
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestBackupAndNewScheduleToTheSameStorage(t *testing.T) {
	t.Parallel()
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	s3Repo2 := &crunchyv1beta1.RepoS3{
		Bucket: "bucket1",
	}
	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
		},
		{
			Name: "repo2",
			S3:   s3Repo2,
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{{
		Enabled:           true,
		Name:              "sched1",
		Schedule:          "20 * * * *",
		BackupStorageName: "bs1",
	}}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{
		{
			Spec: everestv1alpha1.DatabaseClusterBackupSpec{
				BackupStorageName: "bs1",
			},
		},
	}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{
		"bs1": {
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   "s3",
				Bucket: "bucket1",
			},
		},
	}
	testBackupStoragesSecrets := map[string]*corev1.Secret{
		"bs1": {
			Data: map[string][]byte{
				"AWS_ACCESS_KEY_ID":     []byte("SomeAccessKeyID"),
				"AWS_SECRET_ACCESS_KEY": []byte("SomeSecretAccessKey"),
			},
		},
	}
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
		{
			Name: "repo2",
			BackupSchedules: &crunchyv1beta1.PGBackRestBackupSchedules{
				Full: pointer.To("20 * * * *"),
			},
			S3: s3Repo2,
		},
	}

	repos, _, _, _ := reconcilePGBackRestRepos( //nolint:dogsled
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposUnknownStorageSchedule(t *testing.T) {
	t.Parallel()
	testSchedule := "0 0 * * *"
	testRepos := []crunchyv1beta1.PGBackRestRepo{}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{
		{
			Enabled:           true,
			Name:              "schedule1",
			Schedule:          testSchedule,
			BackupStorageName: "backupStorage1",
		},
	}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{}
	testBackupStoragesSecrets := map[string]*corev1.Secret{}
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposEmpty(t *testing.T) {
	t.Parallel()
	testRepos := []crunchyv1beta1.PGBackRestRepo{}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{}
	testBackupStoragesSecrets := map[string]*corev1.Secret{}
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}
	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			Volume: &crunchyv1beta1.RepoPVC{
				VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &testEngineStorageClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testEngineStorageSize,
						},
					},
				},
			},
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func TestReconcilePGBackRestReposAfterDataSource_withoutSchedule(t *testing.T) {
	t.Parallel()

	testRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name: "repo1",
			S3: &crunchyv1beta1.RepoS3{
				Bucket:   "bucket1",
				Region:   "region1",
				Endpoint: "endpoint1",
			},
		},
	}
	testBackupSchedules := []everestv1alpha1.BackupSchedule{}
	testBackupRequests := []everestv1alpha1.DatabaseClusterBackup{}
	testBackupStorages := map[string]everestv1alpha1.BackupStorage{}
	testBackupStoragesSecrets := map[string]*corev1.Secret{}

	expRepos := []crunchyv1beta1.PGBackRestRepo{
		{
			Name:   "repo1",
			Volume: pvcVolume,
		},
	}

	repos, _, _, err := reconcilePGBackRestRepos(
		testRepos,
		testBackupSchedules,
		testBackupRequests,
		testBackupStorages,
		testBackupStoragesSecrets,
		testEngineStorage,
		&everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
				UID:  "123",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, expRepos, repos)
}

func Test_globalDatasourceDestination(t *testing.T) {
	t.Parallel()

	t.Run("empty dest", func(t *testing.T) {
		t.Parallel()

		db := &everestv1alpha1.DatabaseCluster{}
		db.Name = "db-name"
		db.UID = "db-uid"

		bs := &everestv1alpha1.BackupStorage{}

		dest := globalDatasourceDestination("", db, bs)
		assert.Equal(t, "/"+common.BackupStoragePrefix(db), dest)
	})

	t.Run("not-empty dest s3", func(t *testing.T) {
		t.Parallel()

		db := &everestv1alpha1.DatabaseCluster{}
		db.Name = "db-name"
		db.UID = "db-uid"

		bs := &everestv1alpha1.BackupStorage{
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   everestv1alpha1.BackupStorageTypeS3,
				Bucket: "some/bucket/here",
			},
		}

		dest := globalDatasourceDestination("s3://some/bucket/here/db-name/db-uid/some/folders/later", db, bs)
		assert.Equal(t, "/db-name/db-uid", dest)
	})

	t.Run("not-empty dest azure", func(t *testing.T) {
		t.Parallel()

		db := &everestv1alpha1.DatabaseCluster{}
		db.Name = "db-name"
		db.UID = "db-uid"

		bs := &everestv1alpha1.BackupStorage{
			Spec: everestv1alpha1.BackupStorageSpec{
				Type:   everestv1alpha1.BackupStorageTypeAzure,
				Bucket: "some/bucket/here",
			},
		}

		dest := globalDatasourceDestination("azure://some/bucket/here/db-name/db-uid/some/folders/later", db, bs)
		assert.Equal(t, "/db-name/db-uid", dest)
	})
}

func TestSortPGBackRestReposByName(t *testing.T) {
	t.Parallel()
	type testCase struct {
		repos       []crunchyv1beta1.PGBackRestRepo
		sortedRepos []crunchyv1beta1.PGBackRestRepo
	}

	testCases := []testCase{
		{
			repos: []crunchyv1beta1.PGBackRestRepo{
				{Name: "repo4"},
				{Name: "repo3"},
				{Name: "repo2"},
				{Name: "repo1"},
			},
			sortedRepos: []crunchyv1beta1.PGBackRestRepo{
				{Name: "repo1"},
				{Name: "repo2"},
				{Name: "repo3"},
				{Name: "repo4"},
			},
		},
		{
			repos: []crunchyv1beta1.PGBackRestRepo{
				{Name: "repo1"},
				{Name: "repo3"},
				{Name: "repo2"},
				{Name: "repo4"},
			},
			sortedRepos: []crunchyv1beta1.PGBackRestRepo{
				{Name: "repo1"},
				{Name: "repo2"},
				{Name: "repo3"},
				{Name: "repo4"},
			},
		},
	}

	for _, tc := range testCases {
		input := tc.repos
		sortByName(input)
		assert.Equal(t, input, tc.sortedRepos)
	}
}

var pvcVolume, testEngineStorage = pvcVolumeAndEngineStorage()

func pvcVolumeAndEngineStorage() (*crunchyv1beta1.RepoPVC, everestv1alpha1.Storage) {
	testEngineStorageSize, _ := resource.ParseQuantity("15G")
	testEngineStorageClass := "someSC"
	testEngineStorage := everestv1alpha1.Storage{
		Size:  testEngineStorageSize,
		Class: &testEngineStorageClass,
	}

	return &crunchyv1beta1.RepoPVC{
		VolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			StorageClassName: &testEngineStorageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: testEngineStorageSize,
				},
			},
		},
	}, testEngineStorage
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
		curPgSpec      *pgv2.PerconaPGClusterSpec
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
			curPgSpec: nil,
			want:      common.PmmResourceRequirementsSmall,
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
			curPgSpec: nil,
			want:      common.PmmResourceRequirementsMedium,
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
			curPgSpec: nil,
			want:      common.PmmResourceRequirementsLarge,
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
			curPgSpec: nil,
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
			curPgSpec: nil,
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
			curPgSpec: nil,
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
					Enabled: false,
				},
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
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
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
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
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
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
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
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
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
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
				InstanceSets: pgv2.PGInstanceSets{
					{
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
			curPgSpec: &pgv2.PerconaPGClusterSpec{
				PMM: &pgv2.PMMSpec{
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
				InstanceSets: pgv2.PGInstanceSets{
					{
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

			calculatedResources := getPMMResources(tt.isNewDBCluster, tt.dbSpec, tt.curPgSpec)
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
