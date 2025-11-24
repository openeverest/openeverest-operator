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

// Package pxc ...
package pxc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
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

// Cmd is the command for running PXC import.
var Cmd = &cobra.Command{
	Use:  "pxc",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configPath := args[0]

		if err := runPXCImport(cmd.Context(), configPath); err != nil {
			log.Error().Err(err).Msg("Failed to run pxc import")
			os.Exit(1)
		}
	},
}

func runPXCImport(ctx context.Context, configPath string) error {
	cfg := &dataimporterspec.Spec{}
	if err := cfg.ReadFromFilepath(configPath); err != nil {
		return err
	}

	// prepare API scheme.
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add corev1 to scheme: %w", err)
	}
	if err := everestv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add everestv1alpha1 to scheme: %w", err)
	}
	if err := pxcv1.SchemeBuilder.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add pxcv1 to scheme: %w", err)
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
	)

	// prepare k8s client.
	k8sClient, err := client.New(config.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return err
	}

	// First we shall pause the DB reconciliation because the PXC operator will
	// pause/unpause the cluster during the restore operation and we don't want
	// the Everest operator to interfere with that.
	if err := pauseDBReconciliation(ctx, k8sClient, dbName, namespace); err != nil {
		return fmt.Errorf("failed to pause DB reconciliation: %w", err)
	}

	defer func() { //nolint:contextcheck
		// We use a new context for cleanup since the original context may be canceled or timed out,
		// for e.g., if the DB is deleted before the import can complete.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second*30) //nolint:mnd
		defer cancel()

		if unpauseErr := unpauseDBReconciliation(cleanupCtx, k8sClient, dbName, namespace); unpauseErr != nil {
			log.Error().Err(unpauseErr).Msg("Failed to unpause DB reconciliation")
			err = errors.Join(err, unpauseErr)
		}
	}()

	pxcRestoreName := utils.GetMd5HashedName("data-import-" + dbName)

	defer func() { //nolint:contextcheck
		// We use a new context for cleanup since the original context may be canceled or timed out,
		// for e.g., if the DB is deleted before the import can complete.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second*30) //nolint:mnd
		defer cancel()

		if err := cleanup(cleanupCtx, k8sClient, namespace, pxcRestoreName); err != nil {
			log.Error().Err(err).Msgf("Failed to clean up after PXC import for database %s", dbName)
		}
	}()

	log.Info().Msgf("Starting PXC import for database %s in namespace %s", dbName, namespace)

	// Prepare S3 credentials secret.
	if err := prepareS3CredentialSecret(ctx, k8sClient, pxcRestoreName, namespace, accessKeyID, secretAccessKey); err != nil {
		return err
	}
	log.Info().Msgf("S3 credentials secret %s created in namespace %s", pxcRestoreName, namespace)

	// Run PXC restore and wait for it to complete.
	log.Info().Msgf("Starting PXC restore for database %s from backup path %s", dbName, backupPath)
	if err := runPXCRestoreAndWait(ctx, k8sClient, namespace, dbName, pxcRestoreName, backupPath,
		bucket, endpoint, region, verifyTLS); err != nil {
		return fmt.Errorf("failed to run PXC restore: %w", err)
	}
	log.Info().Msgf("PXC restore %s completed successfully for database %s", pxcRestoreName, dbName)
	return nil
}

func setDBReconciliationPause(
	ctx context.Context,
	c client.Client,
	name, namespace string,
	paused bool,
) error {
	db := &everestv1alpha1.DatabaseCluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, db); err != nil {
		return fmt.Errorf("failed to get database cluster %s/%s: %w", namespace, name, err)
	}
	annots := db.GetAnnotations()
	delete(annots, consts.PauseReconcileAnnotation)

	if paused {
		if annots == nil {
			annots = make(map[string]string)
		}
		annots[consts.PauseReconcileAnnotation] = consts.PauseReconcileAnnotationValueTrue
	}
	db.SetAnnotations(annots)
	if err := c.Update(ctx, db); err != nil {
		return fmt.Errorf("failed to set pause reconciliation for database cluster %s/%s: %w", namespace, name, err)
	}
	return nil
}

func pauseDBReconciliation(
	ctx context.Context,
	c client.Client,
	name, namespace string,
) error {
	return setDBReconciliationPause(ctx, c, name, namespace, true)
}

func unpauseDBReconciliation(
	ctx context.Context,
	c client.Client,
	name, namespace string,
) error {
	return setDBReconciliationPause(ctx, c, name, namespace, false)
}

func prepareS3CredentialSecret(
	ctx context.Context,
	c client.Client,
	pxcRestoreName, namespace string,
	accessKeyID, secretAccessKey string,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pxcRestoreName,
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

// Returns the destination and bucket for PXC restore operation.
func getDestinationAndBucket(backupPath, bucket string) (string, string) {
	backupPath = strings.Trim(backupPath, "/") // for consistency
	destination := fmt.Sprintf("s3://%s/%s", bucket, backupPath)

	backupPathParts := strings.Split(backupPath, "/")
	bucketSuffixParts := backupPathParts[:len(backupPathParts)-1]
	bucketSuffix := strings.Join(bucketSuffixParts, "/")
	bucket = strings.Join([]string{bucket, bucketSuffix}, "/")

	return destination, strings.Trim(bucket, "/")
}

func runPXCRestoreAndWait(
	ctx context.Context,
	c client.Client,
	namespace string,
	dbName string,
	pxcRestoreName string,
	backupPath string,
	bucket, endpoint, region string,
	verifyTLS bool,
) error {
	destination, bucket := getDestinationAndBucket(backupPath, bucket)

	pxcRestore := &pxcv1.PerconaXtraDBClusterRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pxcRestoreName,
			Namespace: namespace,
		},
	}

	db := &everestv1alpha1.DatabaseCluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: dbName}, db); err != nil {
		return fmt.Errorf("failed to get database cluster %s/%s: %w", namespace, dbName, err)
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, c, pxcRestore, func() error {
		// set this annotation so that Everest operator does not create a DatabaseBackupRestore (DBR) for this restore.
		pxcRestore.SetAnnotations(map[string]string{
			consts.ManagedByDataImportAnnotation: consts.ManagedByDataImportAnnotationValueTrue,
		})
		// Additional labels to help identify the object.
		pxcRestore.SetLabels(map[string]string{
			consts.EverestLabelPrefix + consts.DatabaseClusterNameLabel: dbName,
		})
		// set owner reference to the database cluster, so that it will be deleted when the DB is deleted.
		if err := controllerutil.SetOwnerReference(db, pxcRestore, c.Scheme()); err != nil {
			return fmt.Errorf("failed to set owner reference for PerconaXtraDBClusterRestore %s/%s: %w", namespace, pxcRestoreName, err)
		}
		pxcRestore.Spec = pxcv1.PerconaXtraDBClusterRestoreSpec{
			PXCCluster: dbName,
			BackupSource: &pxcv1.PXCBackupStatus{
				Destination: pxcv1.PXCBackupDestination(destination),
				VerifyTLS:   &verifyTLS,
				S3: &pxcv1.BackupStorageS3Spec{
					Bucket:            bucket,
					Region:            region,
					EndpointURL:       endpoint,
					CredentialsSecret: pxcRestoreName,
				},
			},
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to create or update PXC restore: %w", err)
	}

	retryInterval := 5 * time.Second //nolint:mnd
	// wait for it to be completed.
	return wait.PollUntilContextCancel(ctx, retryInterval, true, func(ctx context.Context) (bool, error) {
		pxcRestore := &pxcv1.PerconaXtraDBClusterRestore{}
		if err := c.Get(ctx, types.NamespacedName{
			Name:      pxcRestoreName,
			Namespace: namespace,
		}, pxcRestore); err != nil {
			return false, fmt.Errorf("failed to get PXC restore %s: %w", pxcRestoreName, err)
		}
		// we cannot recover from this state, so no point waiting.
		if pxcRestore.Status.State == pxcv1.RestoreFailed {
			return false, fmt.Errorf("PXC restore failed with message: %s", pxcRestore.Status.Comments)
		}
		return pxcRestore.Status.State == pxcv1.RestoreSucceeded, nil
	})
}

func cleanup(
	ctx context.Context,
	c client.Client,
	namespace string,
	pxcRestoreName string,
) error {
	// delete S3 secret.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pxcRestoreName,
			Namespace: namespace,
		},
	}
	if err := c.Delete(ctx, secret); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete S3 credentials secret %s: %w", pxcRestoreName, err)
	}
	return nil
}
