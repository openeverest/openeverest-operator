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
	pgv2 "github.com/percona/percona-postgresql-operator/pkg/apis/pgv2.percona.com/v2"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// DBBackupStorageProtectionFinalizer must be set on DatabaseClusterBackup to ensure that storage is NOT cleaned up.
	// By default, this finalizer is not set, and the storage is cleaned up when the backup is deleted.
	DBBackupStorageProtectionFinalizer = "everest.percona.com/dbb-storage-protection"
)

// BackupState is used to represent the backup's state.
type BackupState string

// Known Backup states.
const (
	BackupNew       BackupState = ""
	BackupStarting  BackupState = "Starting"
	BackupRunning   BackupState = "Running"
	BackupFailed    BackupState = "Failed"
	BackupSucceeded BackupState = "Succeeded"
	BackupDeleting  BackupState = "Deleting"
)

// DatabaseClusterBackupSpec defines the desired state of DatabaseClusterBackup.
type DatabaseClusterBackupSpec struct {
	// DBClusterName is the original database cluster name.
	DBClusterName string `json:"dbClusterName"`
	// BackupStorageName is the name of the BackupStorage used for backups.
	// The BackupStorage must be created in the same namespace as the DatabaseCluster.
	BackupStorageName string `json:"backupStorageName"`
}

// DatabaseClusterBackupStatus defines the observed state of DatabaseClusterBackup.
type DatabaseClusterBackupStatus struct {
	// Created is the timestamp of the upstream backup's creation.
	CreatedAt *metav1.Time `json:"created,omitempty"`
	// Completed is the time when the job was completed.
	CompletedAt *metav1.Time `json:"completed,omitempty"`
	// State is the DatabaseBackup state.
	State BackupState `json:"state,omitempty"`
	// Destination is the full path to the backup.
	Destination *string `json:"destination,omitempty"`
	// Gaps identifies if there are gaps detected in the PITR logs
	Gaps bool `json:"gaps"`
	// LatestRestorableTime is the latest time that can be used for PITR restore
	LatestRestorableTime *metav1.Time `json:"latestRestorableTime,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dbbackup;dbb
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".spec.dbClusterName",description="The original database cluster name"
// +kubebuilder:printcolumn:name="Destination",type="string",JSONPath=".status.destination",description="Backup destination"
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.state",description="Job status"
// +kubebuilder:printcolumn:name="Completed",type="date",JSONPath=".status.completed",description="Time the job was completed"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".status.created",description="Age of the resource"

// DatabaseClusterBackup is the Schema for the databaseclusterbackups API.
type DatabaseClusterBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatabaseClusterBackupSpec   `json:"spec,omitempty"`
	Status DatabaseClusterBackupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DatabaseClusterBackupList contains a list of DatabaseClusterBackup.
type DatabaseClusterBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabaseClusterBackup `json:"items"`
}

// HasSucceeded returns true if the backup has succeeded.
func (b *DatabaseClusterBackup) HasSucceeded() bool {
	return b.Status.State == BackupState(pxcv1.BackupSucceeded) ||
		b.Status.State == BackupState(pgv2.BackupSucceeded) ||
		b.Status.State == BackupState(psmdbv1.BackupStateReady)
}

// HasFailed returns true if the backup has failed.
func (b *DatabaseClusterBackup) HasFailed() bool {
	return b.Status.State == BackupState(pxcv1.BackupFailed) ||
		b.Status.State == BackupState(pgv2.BackupFailed) ||
		b.Status.State == BackupState(psmdbv1.BackupStateError)
}

// HasCompleted returns true if the backup has completed.
func (b *DatabaseClusterBackup) HasCompleted() bool {
	return (b.HasSucceeded() || b.HasFailed()) && b.GetDeletionTimestamp().IsZero()
}

// GetDBBackupState returns the backup state from the upstream backup object.
func GetDBBackupState(upstreamBkp client.Object) BackupState {
	switch bkp := upstreamBkp.(type) {
	case *pxcv1.PerconaXtraDBClusterBackup:
		return dbbackupStateFromPXC(bkp)
	case *pgv2.PerconaPGBackup:
		return dbbackupStateFromPG(bkp)
	case *psmdbv1.PerconaServerMongoDBBackup:
		return dbbackupStateFromPSMDB(bkp)
	default:
		return ""
	}
}

func dbbackupStateFromPXC(bkp *pxcv1.PerconaXtraDBClusterBackup) BackupState {
	switch bkp.Status.State {
	case pxcv1.BackupSucceeded:
		return BackupSucceeded
	case pxcv1.BackupFailed:
		return BackupFailed
	case pxcv1.BackupRunning:
		return BackupRunning
	case pxcv1.BackupStarting:
		return BackupStarting
	default:
		return BackupNew
	}
}

func dbbackupStateFromPG(bkp *pgv2.PerconaPGBackup) BackupState {
	switch bkp.Status.State {
	case pgv2.BackupSucceeded:
		return BackupSucceeded
	case pgv2.BackupFailed:
		return BackupFailed
	case pgv2.BackupRunning:
		return BackupRunning
	case pgv2.BackupStarting:
		return BackupStarting
	default:
		return BackupNew
	}
}

func dbbackupStateFromPSMDB(bkp *psmdbv1.PerconaServerMongoDBBackup) BackupState {
	switch bkp.Status.State {
	case psmdbv1.BackupStateReady:
		return BackupSucceeded
	case psmdbv1.BackupStateError:
		return BackupFailed
	case psmdbv1.BackupStateRunning:
		return BackupRunning
	case psmdbv1.BackupStateWaiting, psmdbv1.BackupStateRequested:
		return BackupStarting
	default:
		return BackupNew
	}
}

func init() {
	SchemeBuilder.Register(&DatabaseClusterBackup{}, &DatabaseClusterBackupList{})
}
