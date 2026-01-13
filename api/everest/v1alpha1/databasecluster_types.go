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
	"fmt"
	"net"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	enginefeatureseverestv1alpha1 "github.com/percona/everest-operator/api/enginefeatures.everest/v1alpha1"
)

// Prefefined database engine sizes based on memory.
var (
	MemorySmallSize  = resource.MustParse("2G")
	MemoryMediumSize = resource.MustParse("8G")
	MemoryLargeSize  = resource.MustParse("32G")
)

const (
	// AppStateUnknown is an unknown state.
	AppStateUnknown AppState = "unknown"
	// AppStateCreating is used when the db did not start to initialize yet.
	AppStateCreating AppState = "creating"
	// AppStateInit is a initializing state.
	AppStateInit AppState = "initializing"
	// AppStatePaused is a paused state.
	AppStatePaused AppState = "paused"
	// AppStateReady is a ready state.
	AppStateReady AppState = "ready"
	// AppStateError is an error state.
	AppStateError AppState = "error"
	// AppStateRestoring is a restoring state.
	AppStateRestoring AppState = "restoring"
	// AppStateDeleting is a deleting state.
	AppStateDeleting AppState = "deleting"
	// AppStateUpgrading is an upgrading state.
	AppStateUpgrading AppState = "upgrading"
	// AppStateResizingVolumes is the state when PVCs are being resized.
	AppStateResizingVolumes = "resizingVolumes"
	// AppStateImporting is the state when a data import job is being executed for the cluster.
	AppStateImporting AppState = "importing"
	// AppStateNew represents a newly created cluster that has not yet been reconciled.
	AppStateNew AppState = ""

	// Deprecated: ExposeTypeInternal is an internal expose type. Please use ExposeTypeClusterIP instead
	ExposeTypeInternal ExposeType = "internal"
	// Deprecated: ExposeTypeExternal is an external expose type. Please use ExposeTypeLoadBalancer instead
	ExposeTypeExternal ExposeType = "external"

	// ExposeTypeClusterIP is the ClusterIP expose type.
	ExposeTypeClusterIP ExposeType = "ClusterIP"
	// ExposeTypeLoadBalancer is the LoadBalancer expose type.
	ExposeTypeLoadBalancer ExposeType = "LoadBalancer"
	// ExposeTypeNodePort is the NodePort expose type.
	ExposeTypeNodePort ExposeType = "NodePort"

	// ProxyTypeMongos is a mongos proxy type.
	ProxyTypeMongos ProxyType = "mongos"
	// ProxyTypeHAProxy is a HAProxy proxy type.
	ProxyTypeHAProxy ProxyType = "haproxy"
	// ProxyTypeProxySQL is a ProxySQL proxy type.
	ProxyTypeProxySQL ProxyType = "proxysql"
	// ProxyTypePGBouncer is a PGBouncer proxy type.
	ProxyTypePGBouncer ProxyType = "pgbouncer"
)

func (t ExposeType) Normalize() ExposeType {
	switch t {
	case ExposeTypeInternal:
		return ExposeTypeClusterIP
	case ExposeTypeExternal:
		return ExposeTypeLoadBalancer
	}
	return t
}

func (t ExposeType) DeprecationWarning() string {
	if t != t.Normalize() {
		return fmt.Sprintf("expose type '%s' is deprecated, please use '%s' instead", t, t.Normalize())
	}
	return ""
}

type (
	// LoadBalancerType contains supported loadbalancers. It can be proxysql or haproxy
	// for PXC clusters, mongos for PSMDB clusters or pgbouncer for Postgresql clusters.
	LoadBalancerType string
	// AppState is used to represent cluster's state.
	AppState string
	// BackupStorageProviderSpec represents set of settings to configure cloud provider.
	BackupStorageProviderSpec struct {
		// A container name is a valid DNS name that conforms to the Azure naming rules.
		ContainerName string `json:"containerName,omitempty"`

		Bucket            string `json:"bucket,omitempty"`
		Prefix            string `json:"prefix,omitempty"`
		CredentialsSecret string `json:"credentialsSecret"`
		Region            string `json:"region,omitempty"`
		EndpointURL       string `json:"endpointUrl,omitempty"`

		// STANDARD, NEARLINE, COLDLINE, ARCHIVE for GCP
		// Hot (Frequently accessed or modified data), Cool (Infrequently accessed or modified data), Archive (Rarely accessed or modified data) for Azure.
		StorageClass string `json:"storageClass,omitempty"`
	}
	// EngineSize is used to represent the size of a database engine based on memory.
	EngineSize string
)

const (
	// EngineSizeSmall represents a small engine size.
	EngineSizeSmall EngineSize = "small"
	// EngineSizeMedium represents a medium engine size.
	EngineSizeMedium EngineSize = "medium"
	// EngineSizeLarge represents a large engine size.
	EngineSizeLarge EngineSize = "large"
)

// WithCreatingState transforms empty and unknown states to a single AppStateCreating.
// The upstream operators have the different statuses when a cluster is being created -
// pxc - "unknown", psmdb - "", pg does not have any empty status.
// Everest maps the DB status 1:1, and there is no point so far to create a separate mapping
// for each upstream operator separately only because we want to unify AppStateCreating.
func (s AppState) WithCreatingState() AppState {
	if s == AppStateUnknown || s == AppStateNew {
		return AppStateCreating
	}
	return s
}

// Applier provides methods for specifying how to apply a DatabaseCluster CR
// onto the CR(s) provided by the underlying DB operators (e.g. PerconaXtraDBCluster, PerconaServerMongoDB, PerconaPGCluster, etc.)
//
// +kubebuilder:object:generate=false
type Applier interface {
	Paused(paused bool)
	AllowUnsafeConfig()
	Engine() error
	EngineFeatures() error
	Proxy() error
	DataSource() error
	DataImport() error
	Monitoring() error
	PodSchedulingPolicy() error
	Backup() error
	Metadata() error
	ResetDefaults() error
}

// Storage is the storage configuration.
type Storage struct {
	// Size is the size of the persistent volume claim
	Size resource.Quantity `json:"size"`
	// Class is the storage class to use for the persistent volume claim
	Class *string `json:"class,omitempty"`
}

// Resources are the resource requirements.
type Resources struct {
	// CPU is the CPU resource requirements
	CPU resource.Quantity `json:"cpu,omitempty"`
	// Memory is the memory resource requirements
	Memory resource.Quantity `json:"memory,omitempty"`
}

// Engine is the engine configuration.
type Engine struct {
	// Type is the engine type
	// +kubebuilder:validation:Enum:=pxc;postgresql;psmdb
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message=".spec.engine.type cannot be changed"
	Type EngineType `json:"type"`
	// Version is the engine version
	Version string `json:"version,omitempty"`
	// Replicas is the number of engine replicas
	// +kubebuilder:validation:Minimum:=1
	Replicas int32 `json:"replicas,omitempty"`
	// Storage is the engine storage configuration
	Storage Storage `json:"storage"`
	// Resources are the resource limits for each engine replica.
	// If not set, resource limits are not imposed
	Resources Resources `json:"resources,omitempty"`
	// Config is the engine configuration
	Config string `json:"config,omitempty"`
	// UserSecretsName is the name of the secret containing the user secrets
	UserSecretsName string `json:"userSecretsName,omitempty"`
	// CRVersion is the desired version of the CR to use with the
	// underlying operator.
	// If unspecified, everest-operator will use the same version as the operator.
	//
	// NOTE: Updating this property post installation may lead to a restart of the cluster.
	// +optional
	CRVersion *string `json:"crVersion,omitempty"`
}

// Size returns the size of the engine.
func (e *Engine) Size() EngineSize {
	m := e.Resources.Memory
	// mem >= Large
	if m.Cmp(MemoryLargeSize) >= 0 {
		return EngineSizeLarge
	}
	// mem >= Medium
	if m.Cmp(MemoryMediumSize) >= 0 {
		return EngineSizeMedium
	}
	return EngineSizeSmall
}

// ExposeType is the expose type.
type ExposeType string

// IPSourceRange represents IP addresses in CIDR notation or without a netmask.
type IPSourceRange string

// Expose is the expose configuration.
type Expose struct {
	// Type is the expose type, can be ClusterIP, LoadBalancer, NodePort. The types internal and external are deprecated.
	// +kubebuilder:validation:Enum:=internal;external;ClusterIP;LoadBalancer;NodePort
	// +kubebuilder:default:=ClusterIP
	Type ExposeType `json:"type,omitempty"`
	// IPSourceRanges is the list of IP source ranges (CIDR notation)
	// to allow access from. If not set, there is no limitations
	IPSourceRanges []IPSourceRange `json:"ipSourceRanges,omitempty"`
	// LoadBalancerConfigName is the name of load balancer config if applied
	LoadBalancerConfigName string `json:"loadBalancerConfigName,omitempty"`
}

func (e *Expose) toCIDR(ranges []IPSourceRange) []IPSourceRange {
	ret := make([]IPSourceRange, 0, len(ranges))
	ret = append(ret, ranges...)
	for k, v := range ret {
		if _, _, err := net.ParseCIDR(string(v)); err == nil {
			continue
		}

		ip := net.ParseIP(string(v))
		if ip == nil {
			continue
		}

		if ip.To4() != nil {
			// IPv4 without a subnet. Add /32 subnet by default.
			ret[k] = v + "/32"
		} else {
			// IPv6 without a subnet. Add /128 subnet by default.
			ret[k] = v + "/128"
		}
	}

	return ret
}

// IPSourceRangesStringArray returns []string of IPSource ranges. It also calls toCIDR function to convert IP addresses to the correct CIDR notation.
func (e *Expose) IPSourceRangesStringArray() []string {
	sourceRanges := make([]string, len(e.IPSourceRanges))
	ranges := e.toCIDR(e.IPSourceRanges)
	for i, r := range ranges {
		sourceRanges[i] = string(r)
	}
	return sourceRanges
}

// ProxyType is the proxy type.
type ProxyType string

// Proxy is the proxy configuration.
type Proxy struct {
	// Type is the proxy type
	// +kubebuilder:validation:Enum:=mongos;haproxy;proxysql;pgbouncer
	Type ProxyType `json:"type,omitempty"`
	// Replicas is the number of proxy replicas
	// +kubebuilder:validation:Minimum:=1
	Replicas *int32 `json:"replicas,omitempty"`
	// Config is the proxy configuration
	Config string `json:"config,omitempty"`
	// Expose is the proxy expose configuration
	// +kubebuilder:validation:XValidation:rule="self.type == 'internal' || !has(oldSelf.loadBalancerConfigName) || oldSelf.loadBalancerConfigName == '' || (has(self.loadBalancerConfigName) && self.loadBalancerConfigName != '')",message=".spec.proxy.expose.loadBalancerConfigName cannot be cleared once set"
	Expose Expose `json:"expose,omitempty"`
	// Resources are the resource limits for each proxy replica.
	// If not set, resource limits are not imposed
	Resources Resources `json:"resources,omitempty"`
}

// BackupSource represents settings of a source where to get a backup to run restoration.
type BackupSource struct {
	// Path is the path to the backup file/directory.
	// +kubebuilder:validation:Required
	Path string `json:"path"`
	// BackupStorageName is the name of the BackupStorage used for storing backups.
	// The BackupStorage must be created in the same namespace as the DatabaseCluster.
	// +kubebuilder:validation:Required
	BackupStorageName string `json:"backupStorageName"`
}

// DataSource is the data source configuration.
type DataSource struct {
	// DBClusterBackupName is the name of the DB cluster backup to restore from
	DBClusterBackupName string `json:"dbClusterBackupName,omitempty"`
	// BackupSource is the backup source to restore from
	BackupSource *BackupSource `json:"backupSource,omitempty"`
	// PITR is the point-in-time recovery configuration
	PITR *PITR `json:"pitr,omitempty"`
	// DataImport allows importing data from an external backup source.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message=".spec.dataSource.dataImport is immutable"
	DataImport *DataImportJobTemplate `json:"dataImport,omitempty"`
}

// BackupSchedule is the backup schedule configuration.
type BackupSchedule struct {
	// Enabled is a flag to enable the schedule
	Enabled bool `json:"enabled"`
	// Name is the name of the schedule
	Name string `json:"name"`
	// RetentionCopies is the number of backup copies to retain
	RetentionCopies int32 `json:"retentionCopies,omitempty"`
	// Schedule is the cron schedule
	Schedule string `json:"schedule"`
	// BackupStorageName is the name of the BackupStorage CR that defines the
	// storage location.
	// The BackupStorage must be created in the same namespace as the DatabaseCluster.
	BackupStorageName string `json:"backupStorageName"`
}

// Backup is the backup configuration.
type Backup struct {
	// Enabled is a flag to enable backups
	// Deprecated. Please use db.spec.backup.schedules[].enabled to control each schedule separately and db.spec.backup.pitr.enabled to control PITR.
	Enabled bool `json:"enabled,omitempty"`
	// Schedules is a list of backup schedules
	Schedules []BackupSchedule `json:"schedules,omitempty"`
	// PITR is the configuration of the point in time recovery
	PITR PITRSpec `json:"pitr,omitempty"`
}

// PITRSpec represents a specification to configure point in time recovery for a database backup/restore.
type PITRSpec struct {
	// Enabled is a flag to enable PITR
	Enabled bool `json:"enabled"`
	// BackupStorageName is the name of the BackupStorage where the PITR is enabled
	// The BackupStorage must be created in the same namespace as the DatabaseCluster.
	BackupStorageName *string `json:"backupStorageName,omitempty"`
	// UploadIntervalSec number of seconds between the binlogs uploads
	UploadIntervalSec *int `json:"uploadIntervalSec,omitempty"`
}

// Monitoring is the monitoring configuration.
type Monitoring struct {
	// MonitoringConfigName is the name of a monitoringConfig CR.
	// The MonitoringConfig must be created in the same namespace as the DatabaseCluster.
	MonitoringConfigName string `json:"monitoringConfigName,omitempty"`
	// Resources defines resource limitations for the monitoring.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// ConfigServer represents the sharding configuration server settings.
type ConfigServer struct {
	// Replicas is the amount of configServers
	// +kubebuilder:validation:Minimum:=1
	Replicas int32 `json:"replicas"`
}

// Sharding are the sharding options. Available only for psmdb.
type Sharding struct {
	// Enabled defines if the sharding is enabled
	Enabled bool `json:"enabled"`
	// Shards defines the number of shards
	// +kubebuilder:validation:Minimum:=1
	Shards int32 `json:"shards"`
	// ConfigServer represents the sharding configuration server settings
	ConfigServer ConfigServer `json:"configServer"`
}

// PSMDBEngineFeatures represents additional features for the PSMDB engine.
type PSMDBEngineFeatures struct {
	// SplitHorizonDNSConfigName is the name of a SplitHorizonDNSConfig CR.
	// The SplitHorizonDNSConfig must be created in the same namespace as the DatabaseCluster.
	SplitHorizonDNSConfigName string `json:"splitHorizonDnsConfigName,omitempty"`

	// NOTE: Features for PSMDB shall be added in the future, like:
	// AdvancedSecurity *PSMDBAdvancedSecurity `json:"advancedSecurity,omitempty"`
}

// PSMDBEngineFeaturesStatus represents additional features statuses for the PSMDB engine.
type PSMDBEngineFeaturesStatus struct {
	// SplitHorizon status of SplitHorizon feature.
	SplitHorizon *enginefeatureseverestv1alpha1.SplitHorizonStatus `json:"splitHorizon,omitempty"`

	// NOTE: Features for PSMDB shall be added in the future, like:
	// AdvancedSecurity *PSMDBAdvancedSecurityStatus `json:"advancedSecurity,omitempty"`
}

// EngineFeatures represents configuration of additional features for the database engine.
type EngineFeatures struct {
	// PSMDB represents additional features for the PSMDB engine.
	PSMDB *PSMDBEngineFeatures `json:"psmdb,omitempty"`
	// NOTE: Features for PXC and PostgreSQL shall be added in the future, like:
	// PXC *PXCEngineFeatures `json:"pxc,omitempty"`
	// PostgreSQL *PostgreSQLEngineFeatures `json:"postgresql,omitempty"`
}

// EngineFeaturesStatus represents additional features statuses for the database engine.
type EngineFeaturesStatus struct {
	// PSMDB represents additional features statuses for the PSMDB engine.
	PSMDB *PSMDBEngineFeaturesStatus `json:"psmdb,omitempty"`
	// NOTE: Features statuses for PXC and PostgreSQL shall be added in the future, like:
	// PXC *PXCEngineFeaturesStatus `json:"pxc,omitempty"`
	// PostgreSQL *PostgreSQLEngineFeaturesStatus `json:"postgresql,omitempty"`
}

// DatabaseClusterSpec defines the desired state of DatabaseCluster.
type DatabaseClusterSpec struct {
	// Paused is a flag to stop the cluster
	Paused bool `json:"paused,omitempty"`
	// GroupName specifies the DatabaseGroup this cluster belongs to.
	// When set, the cluster becomes part of the group and can be managed collectively.
	// The cluster will be automatically labeled with "everest.percona.com/group" for group operations.
	// +optional
	GroupName string `json:"groupName,omitempty"`
	// AllowUnsafeConfiguration field used to ensure that the user can create configurations unfit for production use.
	//
	// Deprecated: AllowUnsafeConfiguration will not be supported in the future releases.
	AllowUnsafeConfiguration bool `json:"allowUnsafeConfiguration,omitempty"`
	// Engine is the database engine specification
	Engine Engine `json:"engine"`
	// Proxy is the proxy specification. If not set, an appropriate
	// proxy specification will be applied for the given engine. A
	// common use case for setting this field is to control the
	// external access to the database cluster.
	Proxy Proxy `json:"proxy,omitempty"`
	// DataSource defines a data source for bootstraping a new cluster
	DataSource *DataSource `json:"dataSource,omitempty"`
	// Backup is the backup specification
	Backup Backup `json:"backup,omitempty"`
	// Monitoring is the monitoring configuration
	Monitoring *Monitoring `json:"monitoring,omitempty"`
	// Sharding is the sharding configuration. PSMDB-only
	Sharding *Sharding `json:"sharding,omitempty"`
	// PodSchedulingPolicyName is the name of the PodSchedulingPolicy CR that defines rules for DB cluster pods allocation across the cluster.
	PodSchedulingPolicyName string `json:"podSchedulingPolicyName,omitempty"`
	// EngineFeatures represents configuration of additional features for the database engine.
	EngineFeatures *EngineFeatures `json:"engineFeatures,omitempty"`
}

// IntoDBRestoreDataSource converts the DataSource into a DatabaseClusterRestoreDataSource.
func (in *DataSource) IntoDBRestoreDataSource() DatabaseClusterRestoreDataSource {
	return DatabaseClusterRestoreDataSource{
		DBClusterBackupName: in.DBClusterBackupName,
		BackupSource:        in.BackupSource,
		PITR:                in.PITR,
	}
}

// IntoDataSource converts the DatabaseClusterRestoreDataSource into a DataSource.
func (in *DatabaseClusterRestoreDataSource) IntoDataSource() *DataSource {
	if in == nil {
		return nil
	}
	return &DataSource{
		DBClusterBackupName: in.DBClusterBackupName,
		BackupSource:        in.BackupSource,
		PITR:                in.PITR,
	}
}

const (
	// ConditionTypeCannotResizeVolume is a condition type that indicates that the volume cannot be resized.
	ConditionTypeCannotResizeVolume = "CannotResizeVolume"
	// ConditionTypeVolumeResizeFailed is a condition type that indicates that the volume resize failed.
	ConditionTypeVolumeResizeFailed = "VolumeResizeFailed"
	// ConditionTypeImportFailed is a condition type that indicates that the data import failed.
	ConditionTypeImportFailed = "ImportFailed"
)

const (
	// ReasonStorageClassDoesNotSupportExpansion is a reason for condition ConditionTypeCannotExpandStorage
	// when the storage class does not support volume expansion.
	ReasonStorageClassDoesNotSupportExpansion = "StorageClassDoesNotSupportExpansion"
	// ReasonCannotShrinkVolume is a reason for condition ConditionTypeCannotResizeVolume
	// when the volume cannot be shrunk.
	ReasonCannotShrinkVolume = "CannotShrinkVolume"
	// ReasonVolumeResizeFailed is a reason for condition ConditionTypeVolumeResizeFailed
	// when the volume resize failed.
	ReasonVolumeResizeFailed = "VolumeResizeFailed"
	// ReasonDataImportJobFailed is a reason for condition ConditionTypeImportFailed.
	ReasonDataImportJobFailed = "DataImportJobFailed"
)

// DatabaseClusterStatus defines the observed state of DatabaseCluster.
type DatabaseClusterStatus struct {
	// ObservedGeneration is the most recent generation observed for this DatabaseCluster.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Status is the status of the cluster
	Status AppState `json:"status,omitempty"`
	// Hostname is the hostname where the cluster can be reached
	Hostname string `json:"hostname,omitempty"`
	// Port is the port where the cluster can be reached
	Port int32 `json:"port,omitempty"`
	// Ready is the number of ready pods
	Ready int32 `json:"ready,omitempty"`
	// Size is the total number of pods
	Size int32 `json:"size,omitempty"`
	// Message is extra information about the cluster
	Message string `json:"message,omitempty"`
	// ActiveStorage is the storage used in cluster (psmdb only)
	ActiveStorage string `json:"activeStorage,omitempty"`
	// CRVersion is the observed version of the CR used with the underlying operator.
	CRVersion string `json:"crVersion,omitempty"`
	// RecommendedCRVersion indicates the target version that the underlying CR should be updated to.
	// When this field is set, it means the CR is running an outdated version and requires an update.
	// The following restrictions apply until the CR is updated to the recommended version:
	// - The operator cannot be upgraded
	// - The database engine version (.spec.engine.version) cannot be modified
	// This field is unset when the CR is already running at the latest recommended version.
	RecommendedCRVersion *string `json:"recommendedCRVersion,omitempty"`
	// Details provides full status of the upstream cluster as a plain text.
	Details string `json:"details,omitempty"`
	// DataImportJobName refers to the DataImportJob that is used to import data into the cluster.
	// This is set only when .spec.dataSource.dataImport is set.
	// +optional
	DataImportJobName *string `json:"dataImportJobName,omitempty"`
	// Conditions contains the observed conditions of the DatabaseCluster.
	Conditions     []metav1.Condition    `json:"conditions,omitempty"`
	EngineFeatures *EngineFeaturesStatus `json:"engineFeatures,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=db;dbc;dbcluster
// +kubebuilder:printcolumn:name="Size",type="string",JSONPath=".status.size"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.ready"
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.status"
// +kubebuilder:printcolumn:name="Hostname",type="string",JSONPath=".status.hostname"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:validation:XValidation:rule="!has(self.?status.recommendedCRVersion) || self.spec.engine.version == oldSelf.spec.engine.version",message="engine version may not be updated when a CRVersion update is pending"
//
// DatabaseCluster is the Schema for the databaseclusters API.
type DatabaseCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatabaseClusterSpec   `json:"spec,omitempty"`
	Status DatabaseClusterStatus `json:"status,omitempty"`
}

// CanRestoreFromBackup returns true if the cluster can be restored from a backup in the current state.
func (dbc *DatabaseCluster) CanRestoreFromBackup() bool {
	// Handle the special case for PG bootstrap restore which appears when the new cluster is being created from a backup.
	// The Everest dbr resource needs to be created to use its backup storage in the PG repos list during restoration,
	// so regardless the dbc status we allow the dbr creation in case of creating a new pg db from DataSource.
	if dbc.Spec.Engine.Type == DatabaseEnginePostgresql && dbc.Spec.DataSource != nil {
		return true
	}
	switch dbc.Status.Status.WithCreatingState() {
	case AppStateCreating, AppStatePaused,
		AppStateRestoring, AppStateDeleting, AppStateUpgrading,
		AppStateResizingVolumes, AppStateImporting:
		return false
	default:
		return true
	}
}

// +kubebuilder:object:root=true

// DatabaseClusterList contains a list of DatabaseCluster.
type DatabaseClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabaseCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DatabaseCluster{}, &DatabaseClusterList{})
}
