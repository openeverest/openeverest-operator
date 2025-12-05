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

// Package psmdb ...
package psmdb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	pbmdefs "github.com/percona/percona-backup-mongodb/pbm/defs"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/api/everest/v1alpha1/dataimporterspec"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/data-importer/utils"
)

// Cmd is the command for running psmdb import.
var Cmd = &cobra.Command{
	Use:  "psmdb",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configPath := args[0]
		if err := runPSMDBImport(cmd.Context(), configPath); err != nil {
			log.Error().Err(err).Msg("Failed to run psmdb import")
			os.Exit(1)
		}
	},
}

func runPSMDBImport(ctx context.Context, configPath string) error {
	cfg := &dataimporterspec.Spec{}
	if err := cfg.ReadFromFilepath(configPath); err != nil {
		return err
	}

	// prepare API scheme.
	scheme := runtime.NewScheme()
	if err := everestv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add everestv1alpha1 to scheme: %w", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add corev1 to scheme: %w", err)
	}
	if err := psmdbv1.SchemeBuilder.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add psmdbv1 to scheme: %w", err)
	}

	var (
		dbName          = cfg.Target.DatabaseClusterRef.Name
		namespace       = cfg.Target.DatabaseClusterRef.Namespace
		accessKeyID     = cfg.Source.S3.AccessKeyID
		secretAccessKey = cfg.Source.S3.SecretKey
		endpoint        = cfg.Source.S3.EndpointURL
		region          = cfg.Source.S3.Region
		bucket          = cfg.Source.S3.Bucket
		backupPath      = cfg.Source.Path
		verifyTLS       = cfg.Source.S3.VerifyTLS
		forcePathStyle  = cfg.Source.S3.ForcePathStyle
	)

	// prepare k8s client.
	k8sClient, err := client.New(config.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	psmdbRestoreName := utils.GetMd5HashedName("data-import-" + dbName)

	defer func() { //nolint:contextcheck
		// We use a new context for cleanup since the original context may be canceled or timed out,
		// for e.g., if the DB is deleted before the import can complete.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second*30) //nolint:mnd
		defer cancel()

		if err := cleanup(cleanupCtx, k8sClient, namespace, psmdbRestoreName); err != nil {
			log.Error().Err(err).Msgf("Failed to clean up after PSMDB import for database %s", dbName)
		}
	}()

	log.Info().Msgf("Starting PSMDB import for database %s in namespace %s", dbName, namespace)

	// Prepare S3 credentials secret.
	if err := prepareS3CredentialSecret(ctx, k8sClient, psmdbRestoreName, namespace, accessKeyID, secretAccessKey); err != nil {
		return err
	}
	log.Info().Msgf("S3 credentials secret %s created in namespace %s", psmdbRestoreName, namespace)

	// Run PSMDB restore and wait for it to complete.
	log.Info().Msgf("Starting PSMDB restore for database %s from backup path %s", dbName, backupPath)
	if err := runPSMDBRestoreAndWait(ctx, k8sClient, namespace, dbName, psmdbRestoreName, backupPath, bucket,
		endpoint, region, forcePathStyle, verifyTLS); err != nil {
		return fmt.Errorf("failed to run PSMDB restore: %w", err)
	}
	log.Info().Msgf("PSMDB restore %s completed successfully for database %s", psmdbRestoreName, dbName)
	return nil
}

func prepareS3CredentialSecret(
	ctx context.Context,
	c client.Client,
	psmdbRestoreName, namespace string,
	accessKeyID, secretAccessKey string,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      psmdbRestoreName,
			Namespace: namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, secret, func() error {
		secret.Type = corev1.SecretTypeOpaque
		secret.StringData = map[string]string{
			"AWS_ACCESS_KEY_ID":     accessKeyID,
			"AWS_SECRET_ACCESS_KEY": secretAccessKey,
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func runPSMDBRestoreAndWait(
	ctx context.Context,
	c client.Client,
	namespace string,
	dbName string,
	psmdbRestoreName string,
	backupPath string,
	bucket, endpoint, region string,
	forcePathstyle, verifyTLS bool,
) error {
	// parse the backup path to extract prefix and destination.
	backupPath = strings.Trim(backupPath, "/")
	split := strings.Split(backupPath, "/")
	prefix := strings.Join(split[:len(split)-1], "/")
	destination := fmt.Sprintf("s3://%s/%s", bucket, backupPath)

	psmdbRestore := &psmdbv1.PerconaServerMongoDBRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      psmdbRestoreName,
			Namespace: namespace,
		},
	}

	db := &everestv1alpha1.DatabaseCluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: dbName}, db); err != nil {
		return fmt.Errorf("failed to get database cluster %s/%s: %w", namespace, dbName, err)
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, c, psmdbRestore, func() error {
		// set this annotation so that Everest operator does not create a DatabaseBackupRestore (DBR) for this restore.
		psmdbRestore.SetAnnotations(map[string]string{
			consts.ManagedByDataImportAnnotation: consts.ManagedByDataImportAnnotationValueTrue,
		})
		// Additional labels to help identify the object.
		psmdbRestore.SetLabels(map[string]string{
			consts.EverestLabelPrefix + consts.DatabaseClusterNameLabel: dbName,
		})
		// set owner reference to the database cluster, so that it will be deleted when the DB is deleted.
		if err := controllerutil.SetOwnerReference(db, psmdbRestore, c.Scheme()); err != nil {
			return fmt.Errorf("failed to set owner reference for PerconaPGRestore %s/%s: %w", namespace, psmdbRestoreName, err)
		}
		psmdbRestore.Spec = psmdbv1.PerconaServerMongoDBRestoreSpec{
			ClusterName: dbName,
			BackupSource: &psmdbv1.PerconaServerMongoDBBackupStatus{
				Type:        pbmdefs.LogicalBackup,
				Destination: destination,
				S3: &psmdbv1.BackupStorageS3Spec{
					Bucket:                bucket,
					Region:                region,
					EndpointURL:           endpoint,
					CredentialsSecret:     psmdbRestoreName,
					Prefix:                prefix,
					InsecureSkipTLSVerify: !verifyTLS,
					ForcePathStyle:        &forcePathstyle,
				},
			},
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to create or update PSMDB restore: %w", err)
	}

	retryInterval := 5 * time.Second //nolint:mnd
	// wait for it to be completed.
	return wait.PollUntilContextCancel(ctx, retryInterval, true, func(ctx context.Context) (bool, error) {
		psmdbRestore := &psmdbv1.PerconaServerMongoDBRestore{}
		if err := c.Get(ctx, types.NamespacedName{
			Name:      psmdbRestoreName,
			Namespace: namespace,
		}, psmdbRestore); err != nil {
			return false, fmt.Errorf("failed to get PSMDB restore %s: %w", psmdbRestoreName, err)
		}
		// we cannot recover from this state, so no point waiting.
		if psmdbRestore.Status.State == psmdbv1.RestoreStateError {
			return false, fmt.Errorf("PSMDB restore failed with message: %s", psmdbRestore.Status.Error)
		}
		return psmdbRestore.Status.State == psmdbv1.RestoreStateReady, nil
	})
}

func cleanup(
	ctx context.Context,
	c client.Client,
	namespace string,
	psmdbRestoreName string,
) error {
	// delete S3 secret.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      psmdbRestoreName,
			Namespace: namespace,
		},
	}
	if err := c.Delete(ctx, secret); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete S3 credentials secret %s: %w", psmdbRestoreName, err)
	}
	return nil
}
