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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

func TestDatabaseClusterRestoreCustomDefaulter_Default(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		dbcrToCreate *everestv1alpha1.DatabaseClusterRestore
		wantDbcr     *everestv1alpha1.DatabaseClusterRestore
		wantErr      error
	}

	testCases := []testCase{
		{
			name: ".spec.dataSource.pitr.type is absent",
			dbcrToCreate: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						PITR: &everestv1alpha1.PITR{
							Date: &everestv1alpha1.RestoreDate{
								Time: metav1.Time{},
							},
						},
					},
				},
			},
			wantDbcr: &everestv1alpha1.DatabaseClusterRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dbcrName,
					Namespace: dbcrNamespace,
				},
				Spec: everestv1alpha1.DatabaseClusterRestoreSpec{
					DataSource: everestv1alpha1.DatabaseClusterRestoreDataSource{
						PITR: &everestv1alpha1.PITR{
							Type: everestv1alpha1.PITRTypeDate,
							Date: &everestv1alpha1.RestoreDate{
								Time: metav1.Time{},
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

			defaulter := DatabaseClusterRestoreCustomDefaulter{}

			err := defaulter.Default(context.TODO(), tc.dbcrToCreate)
			if tc.wantErr == nil {
				require.NoError(t, err)
				assert.Equal(t, tc.wantDbcr.Spec, tc.dbcrToCreate.Spec)
				return
			}
			assert.Equal(t, tc.wantErr.Error(), err.Error())
		})
	}
}
