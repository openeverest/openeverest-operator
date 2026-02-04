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

package everest

import (
	"testing"

	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/postgres-operator.crunchydata.com/v1beta1"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

func TestBackupStorageName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		repoName     string
		pgCluster    *pgv2.PerconaPGCluster
		storages     *everestv1alpha1.BackupStorageList
		expectedName string
		expectError  bool
	}{
		{
			name:     "S3 storage match",
			repoName: "repo1",
			pgCluster: &pgv2.PerconaPGCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec: pgv2.PerconaPGClusterSpec{
					Backups: pgv2.Backups{
						PGBackRest: pgv2.PGBackRestArchive{
							Repos: []crunchyv1beta1.PGBackRestRepo{
								{
									Name: "repo1",
									S3: &crunchyv1beta1.RepoS3{
										Bucket:   "my-s3-bucket",
										Endpoint: "s3.amazonaws.com",
										Region:   "us-east-1",
									},
								},
							},
						},
					},
				},
			},
			storages: &everestv1alpha1.BackupStorageList{
				Items: []everestv1alpha1.BackupStorage{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "s3-storage",
							Namespace: "default",
						},
						Spec: everestv1alpha1.BackupStorageSpec{
							Type:        everestv1alpha1.BackupStorageTypeS3,
							Bucket:      "my-s3-bucket",
							Region:      "us-east-1",
							EndpointURL: "s3.amazonaws.com",
						},
					},
				},
			},
			expectedName: "s3-storage",
			expectError:  false,
		},
		{
			name:     "Azure storage match",
			repoName: "repo2",
			pgCluster: &pgv2.PerconaPGCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec: pgv2.PerconaPGClusterSpec{
					Backups: pgv2.Backups{
						PGBackRest: pgv2.PGBackRestArchive{
							Repos: []crunchyv1beta1.PGBackRestRepo{
								{
									Name: "repo2",
									Azure: &crunchyv1beta1.RepoAzure{
										Container: "database-backups",
									},
								},
							},
						},
					},
				},
			},
			storages: &everestv1alpha1.BackupStorageList{
				Items: []everestv1alpha1.BackupStorage{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "azure-storage",
							Namespace: "default",
						},
						Spec: everestv1alpha1.BackupStorageSpec{
							Type:   everestv1alpha1.BackupStorageTypeAzure,
							Bucket: "database-backups",
						},
					},
				},
			},
			expectedName: "azure-storage",
			expectError:  false,
		},
		{
			name:     "Namespace mismatch",
			repoName: "repo1",
			pgCluster: &pgv2.PerconaPGCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec: pgv2.PerconaPGClusterSpec{
					Backups: pgv2.Backups{
						PGBackRest: pgv2.PGBackRestArchive{
							Repos: []crunchyv1beta1.PGBackRestRepo{
								{
									Name: "repo1",
									S3: &crunchyv1beta1.RepoS3{
										Bucket:   "my-bucket",
										Endpoint: "s3.amazonaws.com",
										Region:   "us-east-1",
									},
								},
							},
						},
					},
				},
			},
			storages: &everestv1alpha1.BackupStorageList{
				Items: []everestv1alpha1.BackupStorage{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "s3-storage",
							Namespace: "other-namespace",
						},
						Spec: everestv1alpha1.BackupStorageSpec{
							Type:        everestv1alpha1.BackupStorageTypeS3,
							Bucket:      "my-bucket",
							Region:      "us-east-1",
							EndpointURL: "s3.amazonaws.com",
						},
					},
				},
			},
			expectedName: "",
			expectError:  true,
		},
		{
			name:     "Repo not found",
			repoName: "nonexistent-repo",
			pgCluster: &pgv2.PerconaPGCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec: pgv2.PerconaPGClusterSpec{
					Backups: pgv2.Backups{
						PGBackRest: pgv2.PGBackRestArchive{
							Repos: []crunchyv1beta1.PGBackRestRepo{
								{
									Name: "repo1",
									S3: &crunchyv1beta1.RepoS3{
										Bucket: "my-bucket",
									},
								},
							},
						},
					},
				},
			},
			storages: &everestv1alpha1.BackupStorageList{
				Items: []everestv1alpha1.BackupStorage{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "s3-storage",
							Namespace: "default",
						},
						Spec: everestv1alpha1.BackupStorageSpec{
							Type:   everestv1alpha1.BackupStorageTypeS3,
							Bucket: "my-bucket",
						},
					},
				},
			},
			expectedName: "",
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name, err := backupStorageName(tt.repoName, tt.pgCluster, tt.storages)

			if tt.expectError {
				require.Error(t, err)
				assert.Empty(t, name)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedName, name)
			}
		})
	}
}
