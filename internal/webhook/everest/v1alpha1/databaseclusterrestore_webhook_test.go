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
	dbcrName               = "my-dbcr"
	dbcrNamespace          = "default"
	dbcrTargetDbName       = "target-database-cluster"
	dbcrBackupSourceDbName = "backup-source-db"
	dbcrBackupName         = "my-backup"
	dbcrBackupStorageName  = "my-backup-storage"
)

func TestDatabaseClusterRestoreCustomValidator_ValidateCreate(t *testing.T) { //nolint:maintidx
	t.Parallel()

	type testCase struct {
		name         string
		objs         []ctrlclient.Object
		dbcrToCreate *everestv1alpha1.DatabaseClusterRestore
		wantErr      error
	}

	testCases := []testCase{
		// invalid cases

		// .spec.dbClusterName is empty
		{
			name: ".spec.dbClusterName is empty",
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				errRequiredField(dbcrDbClusterNamePath),
				field.Invalid(dbcDataSourcePath, "",
					fmt.Sprintf("either %s or %s must be specified", dbcrDbClusterBackupNamePath, dbcrBackupSourcePath)),
			}),
		},
		// .spec.dbClusterName is absent
		{
			name: ".spec.dbClusterName is absent",
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				field.NotFound(dbcrDbClusterNamePath,
					fmt.Sprintf("failed to fetch target DatabaseCluster='%s'", types.NamespacedName{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					})),
			}),
		},
		// .spec.dataSource.dbClusterBackupName and .spec.dataSource.backupSource are both specified
		{
			name: ".spec.dataSource.dbClusterBackupName and .spec.dataSource.backupSource are both specified",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupStorageName,
						Namespace: dbcrNamespace,
					},
				},
				&everestv1alpha1.DatabaseClusterBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterBackupSpec{
						DBClusterName: dbcrTargetDbName,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
						BackupSource: &everestv1alpha1.BackupSource{
							Path:              "path/to/backup",
							BackupStorageName: "my-backup-storage",
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				field.Invalid(dbcDataSourcePath, "",
					fmt.Sprintf("either %s or %s must be specified, but not both", dbcrDbClusterBackupNamePath, dbcrBackupSourcePath)),
			}),
		},
		// .spec.dataSource.dbClusterBackupName is absent
		{
			name: "source DbClusterBackup is absent",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				field.NotFound(dbcrDbClusterBackupNamePath, fmt.Sprintf("failed to fetch DatabaseClusterBackup='%s'", types.NamespacedName{
					Name:      dbcrBackupName,
					Namespace: dbcrNamespace,
				})),
			}),
		},
		// .spec.dataSource.dbClusterBackupName is not in Succeeded state
		{
			name: "source DbClusterBackup is not in Succeeded state",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.DatabaseClusterBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterBackupSpec{
						DBClusterName: dbcrTargetDbName,
					},
					Status: everestv1alpha1.DatabaseClusterBackupStatus{
						State: everestv1alpha1.BackupRunning,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				field.Forbidden(dbcrDbClusterBackupNamePath, "DatabaseClusterBackup must be in Succeeded state"),
			}),
		},
		// .spec.dataSource.pitr without date
		{
			name: ".spec.dataSource.pitr without date",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupStorageName,
						Namespace: dbcrNamespace,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						BackupSource: &everestv1alpha1.BackupSource{
							Path:              "path/to/backup",
							BackupStorageName: "my-backup-storage",
						},
						PITR: &everestv1alpha1.PITR{
							Type: everestv1alpha1.PITRTypeDate,
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				errRequiredField(dbcrDataSourcePitrDatePath),
			}),
		},
		// .spec.dataSource.pitr with wrong type
		{
			name: ".spec.dataSource.pitr with wrong type",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupStorageName,
						Namespace: dbcrNamespace,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						BackupSource: &everestv1alpha1.BackupSource{
							Path:              "path/to/backup",
							BackupStorageName: "my-backup-storage",
						},
						PITR: &everestv1alpha1.PITR{
							Type: everestv1alpha1.PITRTypeLatest,
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				field.NotSupported(dbcrDataSourcePitrTypePath, everestv1alpha1.PITRTypeLatest, []everestv1alpha1.PITRType{everestv1alpha1.PITRTypeDate}),
			}),
		},
		// .spec.dataSource.backupSource is empty
		{
			name: ".spec.dataSource.backupSource is empty",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						BackupSource: &everestv1alpha1.BackupSource{},
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				errRequiredField(dbcrBackupSourcePathPath),
				errRequiredField(dbcrBackupSourceStorageNamePath),
			}),
		},
		// .spec.dataSource.backupSource.dbcrBackupStorageName is empty
		{
			name: ".spec.dataSource.backupSource.dbcrBackupStorageName is absent",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						BackupSource: &everestv1alpha1.BackupSource{
							Path: "path/to/backup",
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				errRequiredField(dbcrBackupSourceStorageNamePath),
			}),
		},
		// .spec.dataSource.backupSource.dbcrBackupStorageName is absent
		{
			name: ".spec.dataSource.backupSource.dbcrBackupStorageName is absent",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						BackupSource: &everestv1alpha1.BackupSource{
							Path:              "path/to/backup",
							BackupStorageName: dbcrBackupStorageName,
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				field.NotFound(dbcrBackupSourceStorageNamePath,
					fmt.Sprintf("failed to fetch BackupStorage='%s'", types.NamespacedName{
						Name:      dbcrBackupStorageName,
						Namespace: dbcrNamespace,
					})),
			}),
		},
		// .spec.dataSource.backupSource.path is empty
		{
			name: ".spec.dataSource.backupSource.dbcrBackupStorageName is absent",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupStorageName,
						Namespace: dbcrNamespace,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						BackupSource: &everestv1alpha1.BackupSource{
							BackupStorageName: dbcrBackupStorageName,
						},
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				errRequiredField(dbcrBackupSourcePathPath),
			}),
		},
		// restore from backup that already marked for deletion
		{
			name: "restore from backup that already marked for deletion",
			objs: []ctrlclient.Object{
				// target DB
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterSpec{
						Engine: everestv1alpha1.Engine{
							Type: everestv1alpha1.DatabaseEnginePostgresql,
						},
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.DatabaseClusterBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:              dbcrBackupName,
						Namespace:         dbcrNamespace,
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().UTC()},
						// Fake-client requires at least one finalizer to consider the object as "being deleted"
						Finalizers: []string{"finalizer.everest.percona.com/dbclusterbackup"},
					},
					Spec: everestv1alpha1.DatabaseClusterBackupSpec{
						DBClusterName: dbcrTargetDbName,
					},
					Status: everestv1alpha1.DatabaseClusterBackupStatus{
						State: everestv1alpha1.BackupSucceeded,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				field.Forbidden(dbcrDbClusterBackupNamePath,
					fmt.Sprintf("DatabaseClusterBackup='%s' is being deleted and cannot be used for restoration", types.NamespacedName{
						Name:      dbcrBackupName,
						Namespace: dbcrNamespace,
					})),
			}),
		},
		// restore from backup of another DatabaseCluster
		// dbEngine mismatch
		{
			name: "restore from backup of another DatabaseCluster - engine mismatch",
			objs: []ctrlclient.Object{
				// target DB
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterSpec{
						Engine: everestv1alpha1.Engine{
							Type: everestv1alpha1.DatabaseEnginePostgresql,
						},
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				// source DB
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupSourceDbName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterSpec{
						Engine: everestv1alpha1.Engine{
							Type: everestv1alpha1.DatabaseEnginePSMDB,
						},
					},
				},
				&everestv1alpha1.DatabaseClusterBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterBackupSpec{
						DBClusterName: dbcrBackupSourceDbName,
					},
					Status: everestv1alpha1.DatabaseClusterBackupStatus{
						State: everestv1alpha1.BackupSucceeded,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				field.Invalid(dbcrDbClusterBackupNamePath, dbcrBackupName,
					fmt.Sprintf("the engine of the target DatabaseCluster %s (%s) and the engine of the backup source DatabaseCluster %s (%s) do not match",
						dbcrTargetDbName, everestv1alpha1.DatabaseEnginePostgresql, dbcrBackupSourceDbName, everestv1alpha1.DatabaseEnginePSMDB)),
			}),
		},
		// target DBCluster state is not Ready
		{
			name: "target DatabaseCluster is not in Ready state",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateCreating,
					},
				},
				&everestv1alpha1.DatabaseClusterBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterBackupSpec{
						DBClusterName: dbcrTargetDbName,
					},
					Status: everestv1alpha1.DatabaseClusterBackupStatus{
						State: everestv1alpha1.BackupSucceeded,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				field.Forbidden(dbcrDbClusterNamePath, "the target DatabaseCluster's state does not allow restoration from backup"),
			}),
		},

		// valid cases
		// use backupSource and without pitr
		{
			name: "restore from backupSource without pitr",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupStorageName,
						Namespace: dbcrNamespace,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						BackupSource: &everestv1alpha1.BackupSource{
							Path:              "path/to/backup",
							BackupStorageName: dbcrBackupStorageName,
						},
					},
				},
			},
			wantErr: nil,
		},
		// use backupSource and with pitr
		{
			name: "restore from backupSource without pitr",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.BackupStorage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupStorageName,
						Namespace: dbcrNamespace,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						BackupSource: &everestv1alpha1.BackupSource{
							Path:              "path/to/backup",
							BackupStorageName: dbcrBackupStorageName,
						},
						PITR: &everestv1alpha1.PITR{
							Type: everestv1alpha1.PITRTypeDate,
							Date: &everestv1alpha1.RestoreDate{
								Time: metav1.Time{Time: metav1.Now().UTC()},
							},
						},
					},
				},
			},
			wantErr: nil,
		},

		// restore from backup of the same DatabaseCluster
		// use dbClusterBackupName and without pitr
		{
			name: "restore from DbClusterBackup of the same DatabaseCluster without pitr",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.DatabaseClusterBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterBackupSpec{
						DBClusterName: dbcrTargetDbName,
					},
					Status: everestv1alpha1.DatabaseClusterBackupStatus{
						State: everestv1alpha1.BackupSucceeded,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
					},
				},
			},
			wantErr: nil,
		},
		// use dbClusterBackupName and with pitr
		{
			name: "restore from DbClusterBackup of the same DatabaseCluster with pitr",
			objs: []ctrlclient.Object{
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				&everestv1alpha1.DatabaseClusterBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterBackupSpec{
						DBClusterName: dbcrTargetDbName,
					},
					Status: everestv1alpha1.DatabaseClusterBackupStatus{
						State: everestv1alpha1.BackupSucceeded,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
						PITR: &everestv1alpha1.PITR{
							Type: everestv1alpha1.PITRTypeDate,
							Date: &everestv1alpha1.RestoreDate{
								Time: metav1.Time{Time: metav1.Now().UTC()},
							},
						},
					},
				},
			},
			wantErr: nil,
		},

		// restore from backup of another DatabaseCluster
		// use dbClusterBackupName and without pitr
		{
			name: "restore from DbClusterBackup of another DatabaseCluster without pitr",
			objs: []ctrlclient.Object{
				// targetDB
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterSpec{
						Engine: everestv1alpha1.Engine{
							Type: everestv1alpha1.DatabaseEnginePostgresql,
						},
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				// sourceDB
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupSourceDbName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterSpec{
						Engine: everestv1alpha1.Engine{
							Type: everestv1alpha1.DatabaseEnginePostgresql,
						},
					},
				},
				&everestv1alpha1.DatabaseClusterBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterBackupSpec{
						DBClusterName: dbcrBackupSourceDbName,
					},
					Status: everestv1alpha1.DatabaseClusterBackupStatus{
						State: everestv1alpha1.BackupSucceeded,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
					},
				},
			},
			wantErr: nil,
		},
		// use dbClusterBackupName and with pitr
		{
			name: "restore from DbClusterBackup of another DatabaseCluster with pitr",
			objs: []ctrlclient.Object{
				// targetDB
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrTargetDbName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterSpec{
						Engine: everestv1alpha1.Engine{
							Type: everestv1alpha1.DatabaseEnginePostgresql,
						},
					},
					Status: everestv1alpha1.DatabaseClusterStatus{
						Status: everestv1alpha1.AppStateReady,
					},
				},
				// sourceDB
				&everestv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupSourceDbName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterSpec{
						Engine: everestv1alpha1.Engine{
							Type: everestv1alpha1.DatabaseEnginePostgresql,
						},
					},
				},
				&everestv1alpha1.DatabaseClusterBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      dbcrBackupName,
						Namespace: dbcrNamespace,
					},
					Spec: everestv1alpha1.DatabaseClusterBackupSpec{
						DBClusterName: dbcrBackupSourceDbName,
					},
					Status: everestv1alpha1.DatabaseClusterBackupStatus{
						State: everestv1alpha1.BackupSucceeded,
					},
				},
			},
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
						PITR: &everestv1alpha1.PITR{
							Type: everestv1alpha1.PITRTypeDate,
							Date: &everestv1alpha1.RestoreDate{
								Time: metav1.Time{Time: metav1.Now().UTC()},
							},
						},
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
			utilruntime.Must(everestv1alpha1.AddToScheme(scheme))

			mockClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objs...).
				Build()

			validator := DatabaseClusterRestoreCustomValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateCreate(context.TODO(), tc.dbcrToCreate)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}

func TestDatabaseClusterRestoreCustomValidator_ValidateUpdate(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name    string
		objs    []ctrlclient.Object
		oldDbcr *everestv1alpha1.DatabaseClusterRestore
		newDbcr *everestv1alpha1.DatabaseClusterRestore
		wantErr error
	}

	testCases := []testCase{
		{
			name: "update already marked for deletion DBCR",
			oldDbcr: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:              dbcrName,
					Namespace:         dbcrNamespace,
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().UTC()},
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
				},
			},
			newDbcr: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
				},
			},
		},
		{
			name: "update .spec",
			oldDbcr: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
				},
			},
			newDbcr: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						DBClusterBackupName: dbcrBackupName,
					},
				},
			},
			wantErr: apierrors.NewInvalid(databaseClusterRestoreGroupKind, dbcrName, field.ErrorList{
				errImmutableField(specPath),
			}),
		},
		{
			name: "update .metadata",
			oldDbcr: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
				},
			},
			newDbcr: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
					Labels:    map[string]string{"new-label": "new-value"},
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
				},
			},
		},
		{
			name: "update status",
			oldDbcr: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
				},
			},
			newDbcr: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DBClusterName: dbcrTargetDbName,
				},
				Status: everestv1alpha1.DatabaseClusterRestoreStatus{
					State: everestv1alpha1.RestoreStarting,
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

			validator := DatabaseClusterRestoreCustomValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateUpdate(context.TODO(), tc.oldDbcr, tc.newDbcr)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}

func TestDatabaseClusterRestoreCustomValidator_ValidateDelete(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		objs         []ctrlclient.Object
		dbcrToDelete *everestv1alpha1.DatabaseClusterRestore
		wantErr      error
	}

	testCases := []testCase{
		// already marked for deletion
		{
			name: "already marked to delete",
			dbcrToDelete: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:              dbcrName,
					Namespace:         dbcrNamespace,
					DeletionTimestamp: &metav1.Time{},
				},
			},
		},
		// used by DatabaseCluster
		{
			name: "used by DatabaseCluster",
			dbcrToDelete: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:       dbcrName,
					Namespace:  dbcrNamespace,
					Finalizers: []string{everestv1alpha1.InUseResourceFinalizer},
				},
			},
			wantErr: apierrors.NewForbidden(everestv1alpha1.GroupVersion.WithResource("databaseclusterrestore").GroupResource(),
				dbcrName,
				errDeleteInUse),
		},
		// not used by any DatabaseCluster
		{
			name: "not used by any DatabaseCluster",
			dbcrToDelete: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
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

			validator := DatabaseClusterRestoreCustomValidator{
				Client: mockClient,
			}

			_, err := validator.ValidateDelete(context.TODO(), tc.dbcrToDelete)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}
