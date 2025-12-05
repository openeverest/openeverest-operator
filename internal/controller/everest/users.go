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

import everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"

var userSecretKeys = map[everestv1alpha1.EngineType][]everestv1alpha1.SecretKey{
	everestv1alpha1.DatabaseEnginePXC:   pxcUserKeys,
	everestv1alpha1.DatabaseEnginePSMDB: psmdbUserKeys,

	// not supported until K8SPG-570 is fixed.
	// See - https://perconadev.atlassian.net/browse/K8SPG-570
	everestv1alpha1.DatabaseEnginePostgresql: {},
}

var (
	pxcUserKeys = []everestv1alpha1.SecretKey{
		{
			Name:        "monitor",
			Description: "Password for monitoring user",
		},
		{
			Name:        "root",
			Description: "Password for root user",
		},
		{
			Name:        "proxyadmin",
			Description: "Password for ProxySQL admin user",
		},
		{
			Name:        "xtrabackup",
			Description: "Password for backup user",
		},
		{
			Name:        "operator",
			Description: "Password for the operator user",
		},
		{
			Name:        "replication",
			Description: "Password for the replication user",
		},
	}
	psmdbUserKeys = []everestv1alpha1.SecretKey{
		{
			Name:        "MONGODB_BACKUP_USER",
			Description: "User for MongoDB backup operations",
		},
		{
			Name:        "MONGODB_BACKUP_PASSWORD",
			Description: "Password for MongoDB backup user",
		},
		{
			Name:        "MONGODB_CLUSTER_ADMIN_USER",
			Description: "User for MongoDB cluster administration",
		},
		{
			Name:        "MONGODB_CLUSTER_ADMIN_PASSWORD",
			Description: "Password for MongoDB cluster admin user",
		},
		{
			Name:        "MONGODB_CLUSTER_MONITOR_USER",
			Description: "User for MongoDB cluster monitoring",
		},
		{
			Name:        "MONGODB_CLUSTER_MONITOR_PASSWORD",
			Description: "Password for MongoDB cluster monitor user",
		},
		{
			Name:        "MONGODB_DATABASE_ADMIN_USER",
			Description: "User for MongoDB database administration",
		},
		{
			Name:        "MONGODB_DATABASE_ADMIN_PASSWORD",
			Description: "Password for MongoDB database admin user",
		},
		{
			Name:        "MONGODB_USER_ADMIN_USER",
			Description: "User for MongoDB user administration",
		},
		{
			Name:        "MONGODB_USER_ADMIN_PASSWORD",
			Description: "Password for MongoDB user admin user",
		},
	}
)
