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
package controllers

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

func TestValidatePitrRestoreSpec(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data everestv1alpha1.DatabaseClusterRestoreDataSource
		err  error
	}{
		{
			name: "empty date",
			data: everestv1alpha1.DatabaseClusterRestoreDataSource{
				PITR: &everestv1alpha1.PITR{},
			},
			err: errPitrEmptyDate,
		},
		{
			name: "type latest",
			data: everestv1alpha1.DatabaseClusterRestoreDataSource{
				PITR: &everestv1alpha1.PITR{
					Type: "latest",
				},
			},
			err: errPitrTypeLatest,
		},
		{
			name: "type unknown",
			data: everestv1alpha1.DatabaseClusterRestoreDataSource{
				PITR: &everestv1alpha1.PITR{
					Type: "some-type",
				},
			},
			err: errPitrTypeIsNotSupported,
		},
		{
			name: "no error with type",
			data: everestv1alpha1.DatabaseClusterRestoreDataSource{
				PITR: &everestv1alpha1.PITR{
					Type: "date",
					Date: &everestv1alpha1.RestoreDate{},
				},
			},
			err: nil,
		},
		{
			name: "no error without type",
			data: everestv1alpha1.DatabaseClusterRestoreDataSource{
				PITR: &everestv1alpha1.PITR{
					Date: &everestv1alpha1.RestoreDate{},
				},
			},
			err: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validatePitrRestoreSpec(tc.data)
			if tc.err == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, err.Error(), tc.err.Error())
		})
	}
}

func TestGetPGRestoreOptions(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		data    everestv1alpha1.DatabaseClusterRestoreDataSource
		options []string
	}{
		{
			name:    "only backup",
			data:    everestv1alpha1.DatabaseClusterRestoreDataSource{},
			options: []string{"--set=smth", "--type=immediate"},
		},
		{
			name: "pitr with date",
			data: everestv1alpha1.DatabaseClusterRestoreDataSource{
				PITR: &everestv1alpha1.PITR{
					Date: &everestv1alpha1.RestoreDate{},
				},
			},
			options: []string{"--set=smth", "--type=time", "--target=\"0001-01-01 00:00:00\""},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res, err := getPGRestoreOptions(tc.data, "smth")
			require.NoError(t, err)
			require.Equal(t, tc.options, res)
		})
	}
}

func Test_parsePrefixFromDestination(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"s3://bucketname/dbname/db-uid/backupname": "dbname/db-uid",
		"s3://percona-test-backup-storage-2/mongodb-jxs/b6968af3-dbf4-431f-a8a8-630835081abd/2024-01-10T10:56:13Z": "mongodb-jxs/b6968af3-dbf4-431f-a8a8-630835081abd",
	}
	for source, expected := range cases {
		assert.Equal(t, expected, parsePrefixFromDestination(source))
	}
}

func Test_isCRVersionGreaterOrEqual(t *testing.T) {
	t.Parallel()
	type tCase struct {
		name             string
		currentVersion   string
		desiredVersion   string
		isGreaterOrEqual bool
		err              error
	}
	cases := []tCase{
		{
			name:             "version smaller",
			currentVersion:   "1.19.0",
			desiredVersion:   "1.20.0",
			err:              nil,
			isGreaterOrEqual: false,
		},
		{
			name:             "version equal",
			currentVersion:   "1.20.0",
			desiredVersion:   "1.20.0",
			err:              nil,
			isGreaterOrEqual: true,
		},
		{
			name:             "version greater patch",
			currentVersion:   "1.20.1",
			desiredVersion:   "1.20.0",
			err:              nil,
			isGreaterOrEqual: true,
		},
		{
			name:             "version greater minor",
			currentVersion:   "1.21.0",
			desiredVersion:   "1.20.0",
			err:              nil,
			isGreaterOrEqual: true,
		},
		{
			name:             "incorrect current version",
			currentVersion:   "aaa",
			desiredVersion:   "1.20.0",
			err:              errors.New("Malformed version: aaa"),
			isGreaterOrEqual: false,
		},
		{
			name:             "incorrect desired version",
			currentVersion:   "1.20.0",
			desiredVersion:   "bbb",
			err:              errors.New("Malformed version: bbb"),
			isGreaterOrEqual: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res, err := isCRVersionGreaterOrEqual(tc.currentVersion, tc.desiredVersion)
			if tc.err != nil {
				require.Error(t, err)
				require.Equal(t, err.Error(), tc.err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.isGreaterOrEqual, res)
		})
	}
}
