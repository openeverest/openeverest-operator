# Everest operator CRD usage guide

The Everest operator is a crucial component of Everest, providing a set of CRDs and easy-to-use abstractions to manage your databases declaratively. This guide will walk you through the CRDs provided by Everest operator and how to use them effectively.

> **Note**: While this guide focuses on using Everest through CRDs, we strongly recommend using the Everest UI or API for most operations. The UI/API provides the following benefits:   
> - Built-in validation and guardrails
> - A complete DBaaS-like experience
> - Simplified management of complex operations
> - Enhanced security and access control
> 
> Use CRDs directly only when you need fine-grained control or are integrating with other Kubernetes tools.

## Table of Contents
- [Database Cluster Management](#database-cluster-management)
  - [Basic Database Cluster](#basic-database-cluster)
  - [Engine Types](#engine-types)
    - [DatabaseEngine CRD](#databaseengine-crd)
  - [Exposing Your Database](#exposing-your-database)
- [Backup Storage Configuration](#backup-storage-configuration)
  - [Manual Backups and Restores](#manual-backups-and-restores)
    - [Creating a Manual Backup](#creating-a-manual-backup)
    - [Restoring from a Backup](#restoring-from-a-backup)
    - [Point-in-Time Recovery (PITR)](#point-in-time-recovery-pitr)
- [Monitoring Configuration](#monitoring-configuration)
- [Examples](#examples)
  - [Complete PostgreSQL Cluster Example](#complete-postgresql-cluster-example)
  - [MongoDB Sharded Cluster Example](#mongodb-sharded-cluster-example)
  - [Percona XtraDB Cluster Example](#percona-xtradb-cluster-example)
- [Status Monitoring](#status-monitoring)
- [Troubleshooting](#troubleshooting)

## Database Cluster Management

The core of Everest is the `DatabaseCluster` CRD, which allows you to define and manage database clusters. Here's how to use it:

### Basic Database Cluster

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: DatabaseCluster
metadata:
  labels:
    clusterName: my-database-cluster
  name: my-database-cluster
spec:
  backup:
    pitr:
      enabled: false
  engine:
    replicas: 1
    resources:
      cpu: "1"
      memory: 2G
    storage:
      class: standard-rwo
      size: 25Gi
    type: postgresql # Can be pxc, psmdb, postgresql
    userSecretsName: everest-secrets-my-database-cluster
    version: "17.4"
  monitoring:
    resources: {}
  proxy:
    expose:
      type: internal
    replicas: 1
    resources:
      cpu: "1"
      memory: 30M
    type: pgbouncer
```

### Engine Types

Everest supports three database engines:
- `postgresql`: Percona Distribution for PostgreSQL
- `pxc`: Percona XtraDB Cluster (MySQL)
- `psmdb`: Percona Server for MongoDB

#### DatabaseEngine CRD

The `DatabaseEngine` CRD manages the lifecycle of database engines in your Kubernetes cluster. It handles operator installation, upgrades, and version management.

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: DatabaseEngine
metadata:
  name: percona-postgresql-operator
spec:
  type: postgresql
```

The `DatabaseEngine` status tracks:
- Operator installation state (`not installed`, `installing`, `installed`, `upgrading`)
- Current operator version
- Available database versions and their components
- Pending operator upgrades

Each version can have one of these statuses:
- `recommended`: The preferred version for production use
- `available`: Supported but not the recommended version
- `unavailable`: Version exists but currently can't be used
- `unsupported`: Version that is no longer supported

To check available versions and their status:

```bash
kubectl get dbengine percona-postsgresql-operator -n <your namespace> -o jsonpath='{.status.availableVersions}'
```

> NOTE: Upgrading the operator should be done from the UI or API only.

### Exposing Your Database

You can expose your database service either internally or externally:

```yaml
spec:
  proxy:
    type: haproxy  # or: pgbouncer, proxysql, mongos
    expose:
      type: external
      ipSourceRanges:  # Optional IP whitelist
        - "10.0.0.0/24"
    replicas: 2
```

## Backup Storage Configuration

Before configuring backups, you need to set up a backup storage location. First, create a Kubernetes secret with your cloud storage credentials:

```yaml
apiVersion: v1
data:
  AWS_ACCESS_KEY_ID: YOUR_ACCESS_KEY_ID_BASE64_ENCODED
  AWS_SECRET_ACCESS_KEY: YOUR_SECRET_ACCESS_KEY_BASE64_ENCODED
kind: Secret
metadata:
  name: my-s3-backup-storage
type: Opaque
```

Then, create a `BackupStorage` CRD that references this secret:

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: BackupStorage
metadata:
  name: my-s3-backup-storage
spec:
  bucket: my-s3-bucket
  credentialsSecretName: my-s3-backup-storage
  description: My S3 backup storage
  endpointURL: https://my-s3-endpoint.com
  forcePathStyle: false
  region: us-west-2
  type: s3
  verifyTLS: true
```

Then, configure backup schedules in your `DatabaseCluster`:

```yaml
spec:
  backup:
    schedules:
      - name: "daily-backup"
        enabled: true
        schedule: "0 0 * * *"  # Daily at midnight
        retentionCopies: 7
        backupStorageName: "my-s3-backup-storage"
    pitr:  # Point-in-Time Recovery
      enabled: true
      backupStorageName: "my-s3-backup-storage"
      uploadIntervalSec: 300  # 5 minutes
```

### Manual Backups and Restores

In addition to scheduled backups, you can create manual backups and perform restores using the `DatabaseClusterBackup` and `DatabaseClusterRestore` CRDs.

#### Creating a Manual Backup

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: DatabaseClusterBackup
metadata:
  labels:
    clusterName: my-database-cluster
  name: my-database-cluster-backup
spec:
  backupStorageName: my-s3-backup-storage
  dbClusterName: my-database-cluster
```

Monitor the backup status:
```bash
kubectl get dbbackup manual-backup-2024-04-11 -o jsonpath='{.status}'
```

The backup will go through these states:
- `Starting`: Initial backup preparation
- `Running`: Backup in progress
- `Succeeded`: Backup completed successfully
- `Failed`: Backup failed

#### Restoring from a Backup

You can restore from a backup in two ways:

1. **Restore from a DatabaseClusterBackup**:
```yaml
apiVersion: everest.percona.com/v1alpha1
kind: DatabaseClusterRestore
metadata:
  name: restore-from-backup
spec:
  dbClusterName: my-database-cluster
  dataSource:
    dbClusterBackupName: my-database-cluster-backup
```

2. **Restore to a New Database Cluster**:

To restore a backup to a new database cluster, create a new `DatabaseCluster` with the `dataSource` field that references the backup:

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: DatabaseCluster
metadata:
  name: restored-database
  namespace: default
spec:
  # .. hidden
  dataSource:
    dbClusterBackupName: backup-name-here  # Name of the backup to restore from
```

Important considerations when restoring to a new cluster:
- The new cluster must use the same database engine type and version as the backup
- The new cluster's storage size should be at least as large as the original cluster
- The new cluster will be created in the same namespace as the backup
- Other configurations (like proxy settings, monitoring, etc.) may be different from the original cluster

#### Point-in-Time Recovery (PITR)

You can perform point-in-time recovery to restore your database to a specific moment:

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: DatabaseClusterRestore
metadata:
  name: pitr-restore
spec:
  dbClusterName: my-database
  dataSource:
    dbClusterBackupName: base-backup
    pitr:
      type: date
      date: "2024-04-11T15:30:00Z"  # UTC timestamp
```

Monitor the restore status:
```bash
kubectl get dbrestore restore-from-backup -o jsonpath='{.status}'
```

The restore will go through these states:
- `Starting`: Initial restore preparation
- `Restoring`: Restore in progress
- `Succeeded`: Restore completed successfully
- `Failed`: Restore failed

## Monitoring Configuration

Before enabling monitoring for your database clusters, you need to create a `MonitoringConfig` that defines your monitoring setup. Currently, Everest supports PMM (Percona Monitoring and Management) as the monitoring solution.

First, create a secret with your PMM credentials:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: pmm-credentials
type: Opaque
data:
  username: <YOUR BASE64 ENCODED USERNAME>
  apiKey: <YOUR BASE64 ENCODED API KEY>
```

Then create a `MonitoringConfig`:

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: MonitoringConfig
metadata:
  name: my-monitoring-config
spec:
  type: pmm
  credentialsSecretName: pmm-credentials
  pmm:
    url: "https://pmm.example.com"
    image: "percona/pmm-client:3.4.1"  # Optional: specify PMM client version
  verifyTLS: true  # Optional: verify TLS certificates
```

Now you can enable monitoring for your database cluster:

```yaml
spec:
  monitoring:
    monitoringConfigName: "my-monitoring-config"
    resources:
      limits:
        cpu: "200m"
        memory: "200Mi"
      requests:
        cpu: "100m"
        memory: "100Mi"
```

## Examples

### Complete PostgreSQL Cluster Example

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: DatabaseCluster
metadata:
  labels:
    clusterName: my-pg-cluster
  name: my-pg-cluster
spec:
  backup:
    pitr:
      backupStorageName: my-s3-backup-storage
      enabled: true
    schedules:
    - backupStorageName: my-s3-backup-storage
      enabled: true
      name: my-pg-backup-schedule
      schedule: 30 19 * * *
  engine:
    replicas: 3
    resources:
      cpu: "4"
      memory: 8G
    storage:
      class: standard-rwo
      size: 100Gi
    type: postgresql
    userSecretsName: everest-secrets-my-pg-cluster
    version: "17.4"
  monitoring:
    resources: {}
  proxy:
    expose:
      type: internal
    replicas: 3
    resources:
      cpu: "1"
      memory: 30M
    type: pgbouncer
```

### MongoDB Sharded Cluster Example

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: DatabaseCluster
metadata:
  labels:
    clusterName: mongodb-sharded
  name: mongodb-sharded
spec:
  backup:
    pitr:
      enabled: false
    schedules:
    - backupStorageName: my-s3-backup-storage
      enabled: true
      name: my-schedule
      schedule: 30 19 * * *
  engine:
    replicas: 3
    resources:
      cpu: "1"
      memory: 4G
    storage:
      class: standard-rwo
      size: 25Gi
    type: psmdb
    userSecretsName: everest-secrets-mongodb-sharded
    version: 8.0.12-4
  monitoring:
    resources: {}
  proxy:
    expose:
      type: internal
    replicas: 3
    resources:
      cpu: "1"
      memory: 2G
    type: mongos
  sharding:
    configServer:
      replicas: 3
    enabled: true
    shards: 2
```

### Percona XtraDB Cluster Example

```yaml
apiVersion: everest.percona.com/v1alpha1
kind: DatabaseCluster
metadata:
  labels:
    clusterName: my-pxc-cluster
  name: my-pxc-cluster
spec:
  backup:
    pitr:
      enabled: false
    schedules:
    - backupStorageName: s3
      enabled: true
      name: backup-wbj
      schedule: 30 19 * * *
  engine:
    replicas: 3
    resources:
      cpu: "4"
      memory: 8G
    storage:
      class: standard-rwo
      size: 100Gi
    type: pxc
    userSecretsName: everest-secrets-my-pxc-cluster
    version: 8.0.39-30.1
  monitoring:
    resources: {}
  proxy:
    expose:
      type: internal
    replicas: 3
    resources:
      cpu: 200m
      memory: 200M
    type: haproxy
```

> You can find all the above examples in the [examples](../examples/) directory.

## Status Monitoring

You can monitor the status of your database cluster using:

```bash
kubectl get databasecluster <cluster-name> -o jsonpath='{.status}'
```

The status field will show one of these states:
- `creating`: Initial creation in progress
- `initializing`: Database is being initialized
- `ready`: Database is running and ready
- `error`: An error has occurred
- `paused`: Database is paused
- `upgrading`: Database is being upgraded
- `restoring`: Database is being restored from backup

## Troubleshooting

If your database cluster isn't behaving as expected:

1. Check the cluster status:
   ```bash
   kubectl describe databasecluster <cluster-name> -n <your namespace>
   ```

2. Check the operator logs:
   ```bash
   kubectl logs -n <operator-namespace> deployment/everest-operator-controller-manager -n everest-system
   ```
