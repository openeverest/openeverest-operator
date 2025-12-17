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
	"fmt"
	"testing"

	"github.com/aws/smithy-go/ptr"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseBackupPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		backupPath      string
		wantBackupName  string
		wantDBDirectory string
	}{
		{
			name:            "standard path",
			backupPath:      "/my-database/backup/db/backup-1",
			wantBackupName:  "backup-1",
			wantDBDirectory: "/my-database/",
		},
		{
			name:            "path with trailing slash",
			backupPath:      "/my-database/backup/db/backup-1/",
			wantBackupName:  "backup-1",
			wantDBDirectory: "/my-database/",
		},
		{
			name:            "path without leading slash",
			backupPath:      "my-database/backup/db/backup-1",
			wantBackupName:  "backup-1",
			wantDBDirectory: "/my-database/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backupName, dbDir := parseBackupPath(tt.backupPath)
			assert.Equal(t, tt.wantBackupName, backupName)
			assert.Equal(t, tt.wantDBDirectory, dbDir)
		})
	}
}

func TestAddPGDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		secretName string
		repoPath   string
		repoName   string
		backupName string
		bucket     string
		endpoint   string
		region     string
		uriStyle   string
	}{
		{
			name:       "basic configuration",
			secretName: "test-secret",
			repoPath:   "/my-database/",
			repoName:   "repo1",
			backupName: "backup-20231201",
			bucket:     "my-bucket",
			endpoint:   "s3.amazonaws.com",
			region:     "us-east-1",
			uriStyle:   "host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a basic PerconaPGCluster
			pg := &pgv2.PerconaPGCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec: pgv2.PerconaPGClusterSpec{},
			}

			// Call the function under test
			addPGDataSource(
				tt.secretName,
				tt.repoPath,
				tt.repoName,
				tt.backupName,
				tt.bucket,
				tt.endpoint,
				tt.region,
				tt.uriStyle,
				false,
				pg,
			)

			// Verify that DataSource was set
			assert.NotNil(t, pg.Spec.DataSource, "DataSource should be set")
			assert.NotNil(t, pg.Spec.DataSource.PGBackRest, "PGBackRest should be set")

			pgBackRest := pg.Spec.DataSource.PGBackRest

			// Verify that the BackupsSection was set
			assert.NotNil(t, pg.Spec.Backups, "Backups should be set")
			assert.True(t, ptr.ToBool(pg.Spec.Backups.Enabled), "Backups.Enabled should be true")
			assert.Equal(t, len(pg.Spec.Backups.PGBackRest.Repos), 1, "PGBackRest.Repos should have 1 element")
			assert.Equal(t, pg.Spec.Backups.PGBackRest.Repos[0], pgBackRest.Repo, "PGBackRest.Repos[0] should be the same as in DataSource")

			// Verify Configuration
			assert.Len(t, pgBackRest.Configuration, 1, "Should have one configuration entry")
			assert.NotNil(t, pgBackRest.Configuration[0].Secret, "Secret configuration should be set")
			assert.Equal(t, tt.secretName, pgBackRest.Configuration[0].Secret.LocalObjectReference.Name, "Secret name should match")

			// Verify Global settings
			expectedPathKey := tt.repoName + "-path"
			expectedStyleKey := tt.repoName + "-s3-uri-style"
			assert.Equal(t, tt.repoPath, pgBackRest.Global[expectedPathKey], "Repo path should match")
			assert.Equal(t, tt.uriStyle, pgBackRest.Global[expectedStyleKey], "URI style should match")

			// Verify Options
			expectedSetOption := "--set=" + tt.backupName
			assert.Contains(t, pgBackRest.Options, "--type=immediate", "Should contain immediate type option")
			assert.Contains(t, pgBackRest.Options, expectedSetOption, "Should contain backup set option")
			assert.Contains(t, pgBackRest.Options, fmt.Sprintf("--no-%s-storage-verify-tls", tt.repoName), "Should contain no verify TLS option")

			// Verify Repo configuration
			assert.Equal(t, tt.repoName, pgBackRest.Repo.Name, "Repo name should match")
			assert.NotNil(t, pgBackRest.Repo.S3, "S3 configuration should be set")
			assert.Equal(t, tt.bucket, pgBackRest.Repo.S3.Bucket, "Bucket should match")
			assert.Equal(t, tt.endpoint, pgBackRest.Repo.S3.Endpoint, "Endpoint should match")
			assert.Equal(t, tt.region, pgBackRest.Repo.S3.Region, "Region should match")

			// Verify Resources
			assert.NotNil(t, pgBackRest.Resources, "Resources should be set")
			expectedCPU := resource.MustParse("200m")
			expectedMemory := resource.MustParse("128Mi")
			assert.Equal(t, expectedCPU, pgBackRest.Resources.Limits[corev1.ResourceCPU], "CPU limit should match")
			assert.Equal(t, expectedMemory, pgBackRest.Resources.Limits[corev1.ResourceMemory], "Memory limit should match")
		})
	}
}
