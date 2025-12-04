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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

const (
	dbcbName              = "my-dbcb"
	dbcbNamespace         = "default"
	dbcbSourceDbName      = "target-database-cluster"
	dbcbBackupStorageName = "my-backup-storage"
)

func TestDatabaseClusterBackupCustomValidator_ValidateCreate(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		objs         []ctrlclient.Object
		dbcbToCreate *everestv1alpha1.DatabaseClusterBackup
		wantErr      error
	}

	testCases := []testCase{
		// invalid cases
		// .spec.backupStorageName is empty
		{
			name: ".spec.backupStorageName is empty",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbSourceDbName,
						Namespace: dbcbNamespace,
					},
				},
			},
			dbcbToCreate: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					DBClusterName: dbcbSourceDbName,
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterBackupGroupKind, dbcbName, field.ErrorList{
				errRequiredField(dbcbBackupStorageNamePath),
			}),
		},
		// .spec.backupStorageName is not found
		{
			name: ".spec.backupStorageName is not found",
			objs: []ctrlclient.Object{
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbBackupStorageName,
						Namespace: dbcbNamespace,
					},
				},
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbSourceDbName,
						Namespace: dbcbNamespace,
					},
				},
			},
			dbcbToCreate: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					DBClusterName:     dbcbSourceDbName,
					BackupStorageName: "non-existent-backup-storage",
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterBackupGroupKind, dbcbName, field.ErrorList{
				field.NotFound(dbcbBackupStorageNamePath,
					fmt.Sprintf("failed to fetch BackupStorage='%s'", types.NamespacedName{
						Name:      "non-existent-backup-storage",
						Namespace: dbcbNamespace,
					})),
			}),
		},
		// .spec.backupStorageName is being deleted
		{
			name: ".spec.backupStorageName is being deleted",
			objs: []ctrlclient.Object{
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:              dbcbBackupStorageName,
						Namespace:         dbcbNamespace,
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().UTC()},
						Finalizers:        []string{"some-finalizer"},
					},
				},
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbSourceDbName,
						Namespace: dbcbNamespace,
					},
				},
			},
			dbcbToCreate: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					DBClusterName:     dbcbSourceDbName,
					BackupStorageName: dbcbBackupStorageName,
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterBackupGroupKind, dbcbName, field.ErrorList{
				field.Forbidden(dbcbBackupStorageNamePath,
					fmt.Sprintf("BackupStorage='%s' is being deleted",
						types.NamespacedName{
							Name:      dbcbBackupStorageName,
							Namespace: dbcbNamespace,
						})),
			}),
		},
		// .spec.dbClusterName is empty
		{
			name: ".spec.dbClusterName is empty",
			objs: []ctrlclient.Object{
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbBackupStorageName,
						Namespace: dbcbNamespace,
					},
				},
			},
			dbcbToCreate: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: dbcbBackupStorageName,
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterBackupGroupKind, dbcbName, field.ErrorList{
				errRequiredField(dbcbDBClusterNamePath),
			}),
		},
		// .spec.dbClusterName is not found
		{
			name: ".spec.dbClusterName is not found",
			objs: []ctrlclient.Object{
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbBackupStorageName,
						Namespace: dbcbNamespace,
					},
				},
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbSourceDbName,
						Namespace: dbcbNamespace,
					},
				},
			},
			dbcbToCreate: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: dbcbBackupStorageName,
					DBClusterName:     "non-existent-db-cluster",
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterBackupGroupKind, dbcbName, field.ErrorList{
				field.NotFound(dbcbDBClusterNamePath,
					fmt.Sprintf("failed to fetch DatabaseCluster='%s'", types.NamespacedName{
						Name:      "non-existent-db-cluster",
						Namespace: dbcbNamespace,
					})),
			}),
		},
		// .spec.dbClusterName is being deleted
		{
			name: ".spec.dbClusterName is being deleted",
			objs: []ctrlclient.Object{
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbBackupStorageName,
						Namespace: dbcbNamespace,
					},
				},
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:              dbcbSourceDbName,
						Namespace:         dbcbNamespace,
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().UTC()},
						Finalizers:        []string{"some-finalizer"},
					},
				},
			},
			dbcbToCreate: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: dbcbBackupStorageName,
					DBClusterName:     dbcbSourceDbName,
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterBackupGroupKind, dbcbName, field.ErrorList{
				field.Invalid(dbcbDBClusterNamePath, dbcbSourceDbName, "the target DatabaseCluster is being deleted"),
			}),
		},
		// valid cases
		{
			name: "valid case",
			objs: []ctrlclient.Object{
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbBackupStorageName,
						Namespace: dbcbNamespace,
					},
				},
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcbSourceDbName,
						Namespace: dbcbNamespace,
					},
				},
			},
			dbcbToCreate: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: dbcbBackupStorageName,
					DBClusterName:     dbcbSourceDbName,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			utilruntime.Must(clientgoscheme.AddToScheme(scheme))
			utilruntime.Must(everestv1alpha1.AddToScheme(scheme))

			mockClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objs...).
				Build()

			validator := DatabaseClusterBackupCustomValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateCreate(context.TODO(), tc.dbcbToCreate)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}

func TestDatabaseClusterBackupCustomValidator_ValidateUpdate(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name    string
		objs    []ctrlclient.Object
		oldDbcb *everestv1alpha1.DatabaseClusterBackup
		newDbcb *everestv1alpha1.DatabaseClusterBackup
		wantErr error
	}

	testCases := []testCase{
		{
			name: "update already marked for deletion DBCB",
			oldDbcb: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:              dbcbName,
					Namespace:         dbcbNamespace,
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().UTC()},
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					DBClusterName: dbcbSourceDbName,
				},
			},
			newDbcb: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					DBClusterName: dbcbSourceDbName,
				},
			},
		},
		{
			name: "update .spec",
			oldDbcb: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: dbcbBackupStorageName,
					DBClusterName:     dbcbSourceDbName,
				},
			},
			newDbcb: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: "new-backup-storage-name",
					DBClusterName:     dbcbSourceDbName,
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterBackupGroupKind, dbcbName, field.ErrorList{
				errImmutableField(specPath),
			}),
		},
		{
			name: "update .metadata",
			oldDbcb: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: dbcbBackupStorageName,
					DBClusterName:     dbcbSourceDbName,
				},
			},
			newDbcb: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
					Labels:    map[string]string{"new-label": "new-value"},
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: dbcbBackupStorageName,
					DBClusterName:     dbcbSourceDbName,
				},
			},
		},
		{
			name: "update status",
			oldDbcb: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: dbcbBackupStorageName,
					DBClusterName:     dbcbSourceDbName,
				},
			},
			newDbcb: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterBackupSpec{
					BackupStorageName: dbcbBackupStorageName,
					DBClusterName:     dbcbSourceDbName,
				},
				Status: everestv1alpha1.DatabaseClusterBackupStatus{
					State: everestv1alpha1.BackupNew,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			utilruntime.Must(clientgoscheme.AddToScheme(scheme))
			utilruntime.Must(everestv1alpha1.AddToScheme(scheme))

			mockClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objs...).
				Build()

			validator := DatabaseClusterBackupCustomValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateUpdate(context.TODO(), tc.oldDbcb, tc.newDbcb)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}

func TestDatabaseClusterBackupCustomValidator_ValidateDelete(t *testing.T) { //nolint:dupl
	t.Parallel()

	type testCase struct {
		name         string
		objs         []ctrlclient.Object
		dbcbToDelete *everestv1alpha1.DatabaseClusterBackup
		wantErr      error
	}

	testCases := []testCase{
		// already marked for deletion
		{
			name: "already marked to delete",
			dbcbToDelete: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:              dbcbName,
					Namespace:         dbcbNamespace,
					DeletionTimestamp: &metav1.Time{},
				},
			},
		},
		// used by DatabaseCluster
		{
			name: "used by DatabaseCluster",
			dbcbToDelete: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:       dbcbName,
					Namespace:  dbcbNamespace,
					Finalizers: []string{everestv1alpha1.InUseResourceFinalizer},
				},
			},
			wantErr: apierrors.NewForbidden(everestv1alpha1.GroupVersion.WithResource("databaseclusterbackup").GroupResource(),
				dbcbName,
				errDeleteInUse),
		},
		// not used by any DatabaseCluster
		{
			name: "not used by any DatabaseCluster",
			dbcbToDelete: &everestv1alpha1.DatabaseClusterBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcbName,
					Namespace: dbcbNamespace,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			utilruntime.Must(clientgoscheme.AddToScheme(scheme))
			utilruntime.Must(everestv1alpha1.AddToScheme(scheme))

			mockClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objs...).
				Build()

			validator := DatabaseClusterBackupCustomValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateDelete(context.TODO(), tc.dbcbToDelete)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}
