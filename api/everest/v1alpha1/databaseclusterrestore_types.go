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
	"encoding/json"
	"time"

	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RestoreState represents state of restoration.
type RestoreState string

// Known Restore states.
const (
	RestoreNew       RestoreState = ""
	RestoreStarting  RestoreState = "Starting"
	RestoreRunning   RestoreState = "Restoring"
	RestoreFailed    RestoreState = "Failed"
	RestoreSucceeded RestoreState = "Succeeded"
)

// PITRType represents type of Point-in-time recovery.
type PITRType string

// DateFormat is the date format used in the user input.
const (
	DateFormat      = "2006-01-02T15:04:05Z"
	DateFormatSpace = "2006-01-02 15:04:05"
)

const (
	// PITRTypeDate is Point-in-time recovery type based on the specific date.
	PITRTypeDate PITRType = "date"
	// PITRTypeLatest is Point-in-time recovery type based on the latest date.
	PITRTypeLatest PITRType = "latest"
)

// DatabaseClusterRestoreSpec defines the desired state of DatabaseClusterRestore.
type DatabaseClusterRestoreSpec struct {
	// DBClusterName defines the cluster name to restore.
	DBClusterName string `json:"dbClusterName"`
	// DataSource defines a data source for restoration.
	DataSource DatabaseClusterRestoreDataSource `json:"dataSource"`
}

// DatabaseClusterRestoreDataSource defines a data source for restoration.
type DatabaseClusterRestoreDataSource struct {
	// DBClusterBackupName is the name of the DB cluster backup to restore from
	DBClusterBackupName string `json:"dbClusterBackupName,omitempty"`
	// BackupSource is the backup source to restore from
	BackupSource *BackupSource `json:"backupSource,omitempty"`
	// PITR is the point-in-time recovery configuration
	PITR *PITR `json:"pitr,omitempty"`
}

// DatabaseClusterRestoreStatus defines the observed state of DatabaseClusterRestore.
type DatabaseClusterRestoreStatus struct {
	State       RestoreState `json:"state,omitempty"`
	CompletedAt *metav1.Time `json:"completed,omitempty"`
	Message     string       `json:"message,omitempty"`
}

// PITR represents a specification to configure point in time recovery for a database backup/restore.
type PITR struct {
	// Type is the type of recovery.
	// +kubebuilder:validation:Enum:=date;latest
	// +kubebuilder:default:=date
	Type PITRType `json:"type,omitempty"`
	// Date is the UTC date to recover to. The accepted format: "2006-01-02T15:04:05Z".
	Date *RestoreDate `json:"date,omitempty"`
}

// RestoreDate is a data type for better time.Time support, the same approach as used in psmdb.
// +kubebuilder:validation:Type=string
type RestoreDate struct {
	metav1.Time `json:",inline"`
}

// OpenAPISchemaType returns a schema type for OperAPI specification.
func (*RestoreDate) OpenAPISchemaType() []string { return []string{"string"} }

// OpenAPISchemaFormat returns a format for OperAPI specification.
func (*RestoreDate) OpenAPISchemaFormat() string { return "" }

// MarshalJSON marshals JSON.
func (t *RestoreDate) MarshalJSON() ([]byte, error) {
	if t.IsZero() {
		return []byte("null"), nil
	}

	timeStr := t.Time.Format(DateFormat)
	return json.Marshal(timeStr)
}

// UnmarshalJSON unmarshals JSON.
func (t *RestoreDate) UnmarshalJSON(b []byte) error {
	if len(b) == 4 && string(b) == "null" {
		t.Time = metav1.NewTime(time.Time{})
		return nil
	}

	var str string

	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}

	pt, err := time.Parse(DateFormat, str)
	if err != nil {
		return err
	}

	t.Time = metav1.NewTime(pt)

	return nil
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dbrestore;dbr
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".spec.dbClusterName",description="Cluster name"
// +kubebuilder:printcolumn:name="Backup",type="string",JSONPath=".spec.dataSource.dbClusterBackupName",description="DBClusterBackup name"
// +kubebuilder:printcolumn:name="Path",type="string",JSONPath=".spec.dataSource.backupSource.path",description="Backup path"
// +kubebuilder:printcolumn:name="Storage",type="string",JSONPath=".spec.dataSource.backupSource.backupStorageName",description="Storage name"
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.state",description="Job status"
// +kubebuilder:printcolumn:name="Completed",type="date",JSONPath=".status.completed",description="Completed time"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// DatabaseClusterRestore is the Schema for the databaseclusterrestores API.
type DatabaseClusterRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatabaseClusterRestoreSpec   `json:"spec,omitempty"`
	Status DatabaseClusterRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DatabaseClusterRestoreList contains a list of DatabaseClusterRestore.
type DatabaseClusterRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabaseClusterRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DatabaseClusterRestore{}, &DatabaseClusterRestoreList{})
}

// IsInProgress indicates if the restoration process is in progress.
func (r *DatabaseClusterRestore) IsInProgress() bool {
	return !r.IsComplete()
}

// IsComplete indicates if the restoration process is complete (regardless successful or not).
func (r *DatabaseClusterRestore) IsComplete() bool {
	switch r.Status.State {
	case RestoreNew, RestoreStarting, RestoreRunning:
		return false
	case RestoreFailed, RestoreSucceeded:
		return true
	}
	return true
}

// GetDBRestoreState returns the restore state from the upstream restore object.
func GetDBRestoreState(upstream client.Object) RestoreState {
	switch bkp := upstream.(type) {
	case *pxcv1.PerconaXtraDBClusterRestore:
		return dbRestoreStateFromPXC(bkp)
	case *pgv2.PerconaPGRestore:
		return dbRestoreStateFromPG(bkp)
	case *psmdbv1.PerconaServerMongoDBRestore:
		return dbRestoreStateFromPSMDB(bkp)
	}
	return RestoreNew
}

func dbRestoreStateFromPXC(bkp *pxcv1.PerconaXtraDBClusterRestore) RestoreState {
	switch bkp.Status.State {
	case pxcv1.RestoreSucceeded:
		return RestoreSucceeded
	case pxcv1.RestoreFailed:
		return RestoreFailed
	case pxcv1.RestoreRestore, pxcv1.RestorePITR, pxcv1.RestoreStartCluster:
		return RestoreRunning
	case pxcv1.RestoreStarting, pxcv1.RestoreStopCluster:
		return RestoreStarting
	default:
		return RestoreNew
	}
}

func dbRestoreStateFromPG(bkp *pgv2.PerconaPGRestore) RestoreState {
	switch bkp.Status.State {
	case pgv2.RestoreSucceeded:
		return RestoreSucceeded
	case pgv2.RestoreFailed:
		return RestoreFailed
	case pgv2.RestoreRunning:
		return RestoreRunning
	case pgv2.RestoreStarting:
		return RestoreStarting
	default:
		return RestoreNew
	}
}

func dbRestoreStateFromPSMDB(bkp *psmdbv1.PerconaServerMongoDBRestore) RestoreState {
	switch bkp.Status.State {
	case psmdbv1.RestoreStateReady:
		return RestoreSucceeded
	case psmdbv1.RestoreStateError, psmdbv1.RestoreStateRejected:
		return RestoreFailed
	case psmdbv1.RestoreStateRunning:
		return RestoreRunning
	case psmdbv1.RestoreStateWaiting, psmdbv1.RestoreStateRequested:
		return RestoreStarting
	default:
		return RestoreNew
	}
}
