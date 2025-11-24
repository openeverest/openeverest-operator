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

import "testing"

func TestGetNextUpgradeVersion(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		status *DatabaseEngineStatus
		want   string
	}{
		{
			status: &DatabaseEngineStatus{
				PendingOperatorUpgrades: []OperatorUpgrade{},
			},
			want: "",
		},
		{
			status: &DatabaseEngineStatus{
				PendingOperatorUpgrades: []OperatorUpgrade{
					{TargetVersion: "1.0.0"},
				},
			},
			want: "1.0.0",
		},
		{
			status: &DatabaseEngineStatus{
				PendingOperatorUpgrades: []OperatorUpgrade{
					{TargetVersion: "1.0.0"},
					{TargetVersion: "1.1.0"},
				},
			},
			want: "1.0.0",
		},
		{
			status: &DatabaseEngineStatus{
				PendingOperatorUpgrades: []OperatorUpgrade{
					{TargetVersion: "1.0.0"},
					{TargetVersion: "1.0.1"},
					{TargetVersion: "1.1.0"},
				},
			},
			want: "1.0.1",
		},
	}

	for _, tc := range testCases {
		got := tc.status.GetNextUpgradeVersion()
		if got != tc.want {
			t.Errorf("got %s, want %s", got, tc.want)
		}
	}
}

func TestBestBackupVersion_PXC(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		availableBackupVersions ComponentsMap
		engineVersion           string
		want                    string
	}{
		{
			availableBackupVersions: ComponentsMap{
				"8.0.5": {
					Status: DBEngineComponentRecommended,
				},
				"2.4.29": {
					Status: DBEngineComponentRecommended,
				},
			},
			engineVersion: "8.0.31-23.2",
			want:          "8.0.5",
		},
		{
			availableBackupVersions: ComponentsMap{
				"8.0.5": {
					Status: DBEngineComponentRecommended,
				},
				"2.4.29": {
					Status: DBEngineComponentRecommended,
				},
			},
			engineVersion: "8.0.35-27.1",
			want:          "8.0.5",
		},
		{
			availableBackupVersions: ComponentsMap{
				"8.0.5": {
					Status: DBEngineComponentRecommended,
				},
				"2.4.29": {
					Status: DBEngineComponentRecommended,
				},
			},
			engineVersion: "8.0.41-32.1",
			want:          "8.0.5",
		},
		{
			availableBackupVersions: ComponentsMap{
				"8.0.5": {
					Status: DBEngineComponentRecommended,
				},
				"2.4.29": {
					Status: DBEngineComponentRecommended,
				},
				"8.4.0": {
					Status: DBEngineComponentAvailable,
				},
			},
			engineVersion: "8.4.5-5.1",
			want:          "8.4.0",
		},
	}

	for _, tc := range testCases {
		dbEngine := &DatabaseEngine{
			Spec: DatabaseEngineSpec{
				Type: DatabaseEnginePXC,
			},
			Status: DatabaseEngineStatus{
				AvailableVersions: Versions{
					Backup: tc.availableBackupVersions,
				},
			},
		}
		got := dbEngine.BestBackupVersion(tc.engineVersion)
		if got != tc.want {
			t.Errorf("got %s, want %s", got, tc.want)
		}
	}
}
