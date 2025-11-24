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

// Package consts provides constants used across the operator.
package consts

const (
	// EverestFinalizerPrefix is the prefix for all Everest operator finalizers.
	EverestFinalizerPrefix = "everest.percona.com/"

	// ForegroundDeletionFinalizer is the finalizer that ensures foreground deletion for the resource.
	ForegroundDeletionFinalizer = "foregroundDeletion"

	// DBBackupCleanupFinalizer is the finalizer for cleaning up DatabaseClusterBackup.
	// Deprecated: We keep this for backward compatibility.
	DBBackupCleanupFinalizer = EverestFinalizerPrefix + "dbb-cleanup"

	// UpstreamClusterCleanupFinalizer is the finalizer for cleaning up the upstream cluster.
	UpstreamClusterCleanupFinalizer = EverestFinalizerPrefix + "upstream-cluster-cleanup"

	// DataImportJobRBACCleanupFinalizer is a finalizer that is used to clean up RBAC resources.
	// This finalizer ensures that the Kubernetes Job is deleted only after the RBAC resources are cleaned up.
	// If deletion of resources does not happen in this order, the Job may not have sufficient permissions to perform any cleanup.
	DataImportJobRBACCleanupFinalizer = EverestFinalizerPrefix + "rbac-cleanup"

	// MonitoringConfigSecretCleanupFinalizer is a finalizer that is used to clean up secrets created by the monitoring configuration.
	MonitoringConfigSecretCleanupFinalizer = EverestFinalizerPrefix + "cleanup-secrets"
	// MonitoringConfigVMAgentFinalizer is a finalizer that is used to clean up VM Agent resources created by the monitoring configuration.
	MonitoringConfigVMAgentFinalizer = EverestFinalizerPrefix + "vmagent"

	// EngineFeaturesSplitHorizonDNSConfigSecretCleanupFinalizer is a finalizer that is used to clean up
	// secrets created by the SplitHorizonDNSConfig configuration.
	EngineFeaturesSplitHorizonDNSConfigSecretCleanupFinalizer = EverestFinalizerPrefix + "cleanup-secrets"
)
