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

// Package pg ...
package pg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/postgres-operator.crunchydata.com/v1beta1"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gopkg.in/ini.v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/api/everest/v1alpha1/dataimporterspec"
	"github.com/percona/everest-operator/internal/consts"
)

// Cmd is the command for running PG import.
var Cmd = &cobra.Command{
	Use:  "pg",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configPath := args[0]
		if err := runPGImport(cmd.Context(), configPath); err != nil {
			log.Error().Err(err).Msg("Failed to run pg import")
			os.Exit(1)
		}
	},
}

// Unlike PXC and PSMDB operators, which allow you to restore external data on a running cluster,
// the PG operator works differently. It requires the .spec.dataSource field to be set on the PGCluster resource
// at creation time for a restore operation. If .spec.dataSource is added after the cluster is running,
// the bootstrap process will not re-execute which later causes the instances to not come up (even though the restore succeeds).
// However, by the time the Data importer runs, the PGCluster has already been bootstraped and is running.
// To ensure a successful restore, we need to 're-create' the PGCluster with dataSource configuration.
// This guarantees the bootstrap process is triggered and the restore happens correctly.
// Using PerconaPGRestore to restore on running cluster results in the same issue (instances not coming up).
func runPGImport(
	ctx context.Context,
	configPath string,
) (err error) {
	cfg := &dataimporterspec.Spec{}
	if err := cfg.ReadFromFilepath(configPath); err != nil {
		return err
	}

	var (
		dbName          = cfg.Target.DatabaseClusterRef.Name
		namespace       = cfg.Target.DatabaseClusterRef.Namespace
		accessKeyID     = cfg.Source.S3.AccessKeyID
		secretAccessKey = cfg.Source.S3.SecretKey
		endpoint        = cfg.Source.S3.EndpointURL
		region          = cfg.Source.S3.Region
		backupPath      = cfg.Source.Path
		bucket          = cfg.Source.S3.Bucket
		uriStyle        = "host"
		verifyTLS       = cfg.Source.S3.VerifyTLS
	)
	if cfg.Source.S3.ForcePathStyle {
		uriStyle = "path"
	}

	log.Info().Msgf("Importing PostgreSQL data from %s to %s/%s", backupPath, namespace, dbName)

	// prepare API scheme.
	scheme := runtime.NewScheme()
	if err := everestv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add everestv1alpha1 to scheme: %w", err)
	}
	if err := pgv2.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add pgv2 to scheme: %w", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add corev1 to scheme: %w", err)
	}

	// prepare k8s client.
	k8sClient, err := client.New(config.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return err
	}

	// first we will pause reconciliation of the database cluster because
	// we will be making changes to the PGCluster and we don't want Everest interfering.
	if err := pauseDBReconciliation(ctx, k8sClient, dbName, namespace); err != nil {
		return fmt.Errorf("failed to pause DB reconciliation: %w", err)
	}

	defer func() { //nolint:contextcheck
		// We use a new context for cleanup since the original context may be canceled or timed out,
		// for e.g., if the DB is deleted before the import can complete.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second*30) //nolint:mnd
		defer cancel()

		if unpauseErr := unpauseDBReconciliation(cleanupCtx, k8sClient, dbName, namespace); unpauseErr != nil {
			log.Error().Err(err).Msg("Failed to unpause DB reconciliation")
			err = errors.Join(err, unpauseErr)
		}
	}()

	repoName := "repo1"
	pgBackRestSecretName, err := createPGBackrestSecret(ctx, k8sClient, dbName, repoName, namespace,
		accessKeyID, secretAccessKey)
	if err != nil {
		return fmt.Errorf("failed to create PGBackrest secret: %w", err)
	}

	defer func() { //nolint:contextcheck
		// We use a new context for cleanup since the original context may be canceled or timed out,
		// for e.g., if the DB is deleted before the import can complete.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second*30) //nolint:mnd
		defer cancel()

		if err := cleanup(cleanupCtx, k8sClient, namespace, pgBackRestSecretName); err != nil {
			log.Error().Err(err).Msgf("Failed to clean up PGBackrest secret %s/%s", namespace, pgBackRestSecretName)
		}
	}()

	pgCopy, err := copyPGCluster(ctx, k8sClient, dbName, namespace)
	if err != nil {
		return fmt.Errorf("failed to copy PGCluster %s/%s: %w", namespace, dbName, err)
	}

	backupName, repoPath := parseBackupPath(backupPath)
	addPGDataSource(pgBackRestSecretName, repoPath, repoName, backupName, bucket, endpoint,
		region, uriStyle, verifyTLS, pgCopy)

	if err := restorePGCluster(ctx, k8sClient, pgCopy); err != nil {
		return fmt.Errorf("failed to restore PGCluster %s/%s: %w", namespace, dbName, err)
	}

	log.Info().Msgf("Successfully imported PostgreSQL data from %s to %s/%s", backupPath, namespace, dbName)
	return nil
}

// returns: [baseBackupName, DBDirectory]
func parseBackupPath(fullPath string) (string, string) {
	// for consistency
	fullPath = strings.TrimSuffix(fullPath, "/")
	if !strings.HasPrefix(fullPath, "/") {
		fullPath = "/" + fullPath
	}
	base := filepath.Base(fullPath)
	fullPath = strings.TrimSuffix(fullPath, "backup/db/"+base)
	return base, fullPath
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

// createPGBackrestSecret creates a Secret for PGBackRest.
// Returns the name of the created secret or an error if it fails.
func createPGBackrestSecret(
	ctx context.Context,
	c client.Client,
	dbName string,
	repoName,
	namespace,
	accessKeyID,
	secretAccessKey string,
) (string, error) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      dbName + "-s3-data-import-pgbackrest",
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, secret, func() error {
		cfg, err := ini.LoadSources(ini.LoadOptions{}, []byte{})
		if err != nil {
			return fmt.Errorf("failed to load PGBackRest S3 config: %w", err)
		}

		// update new keys
		cfg.Section("global").Key(repoName + "-s3-key").SetValue(accessKeyID)
		cfg.Section("global").Key(repoName + "-s3-key-secret").SetValue(secretAccessKey)

		// write back to the secret
		w := strings.Builder{}
		if _, err := cfg.WriteTo(&w); err != nil {
			return fmt.Errorf("failed to write PGBackRest S3 config: %w", err)
		}
		secret.StringData = map[string]string{
			"s3.conf": w.String(),
		}
		return nil
	}); err != nil {
		return "", fmt.Errorf("failed to create or update PGBackrest secret: %w", err)
	}
	return secret.GetName(), nil
}

func copyPGCluster(
	ctx context.Context,
	k8sClient client.Client,
	dbName, namespace string,
) (*pgv2.PerconaPGCluster, error) {
	pg := &pgv2.PerconaPGCluster{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: dbName}, pg); err != nil {
		return nil, fmt.Errorf("failed to get PerconaPGCluster %s/%s: %w", namespace, dbName, err)
	}

	copied := pg.DeepCopy()
	copied.ObjectMeta = metav1.ObjectMeta{
		Name:      pg.GetName(),
		Namespace: pg.GetNamespace(),
	}
	copied.Status = pgv2.PerconaPGClusterStatus{}
	return copied, nil
}

func addPGDataSource(
	secretName,
	repoPath,
	repoName,
	backupName,
	bucket,
	endpoint,
	region,
	uriStyle string,
	verifyTLS bool,
	pg *pgv2.PerconaPGCluster,
) {
	options := []string{
		"--type=immediate", // TODO: support PITR
		"--set=" + backupName,
	}
	if !verifyTLS {
		options = append(options, fmt.Sprintf("--no-%s-storage-verify-tls", repoName))
	}
	dataSource := &crunchyv1beta1.DataSource{
		PGBackRest: &crunchyv1beta1.PGBackRestDataSource{
			Configuration: []corev1.VolumeProjection{
				{
					Secret: &corev1.SecretProjection{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: secretName,
						},
					},
				},
			},
			Global: map[string]string{
				repoName + "-path":         repoPath,
				repoName + "-s3-uri-style": uriStyle,
			},
			Options: options,
			Repo: crunchyv1beta1.PGBackRestRepo{
				Name: repoName,
				S3: &crunchyv1beta1.RepoS3{
					Bucket:   bucket,
					Endpoint: endpoint,
					Region:   region,
				},
			},
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
			},
			Stanza: "db",
		},
	}
	pg.Spec.DataSource = dataSource
}

const defaultRetryInterval = time.Second * 30

// We need to re-create the upstream PGCluster with the newly added dataSource config.
// The reason for re-creating rather than updating is that the DB needs to be bootstrapped again
// otherwise the PG instances will not come up successfully.
func restorePGCluster(
	ctx context.Context,
	k8sClient client.Client,
	pg *pgv2.PerconaPGCluster,
) error {
	// Delete the existing PerconaPGCluster.
	if err := k8sClient.Delete(ctx, pg); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete PerconaPGCluster %s/%s: %w", pg.GetNamespace(), pg.GetName(), err)
	}
	if err := wait.PollUntilContextTimeout(ctx, defaultRetryInterval, time.Minute*5, //nolint:mnd
		false, func(ctx context.Context) (bool, error) {
			pgCheck := &pgv2.PerconaPGCluster{}
			if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: pg.GetNamespace(), Name: pg.GetName()}, pgCheck); err != nil {
				if k8serrors.IsNotFound(err) {
					return true, nil // successfully deleted
				}
				return false, fmt.Errorf("failed to get PerconaPGCluster %s/%s: %w", pg.GetNamespace(), pg.GetName(), err)
			}
			return false, nil // still exists, keep waiting
		}); err != nil {
		return fmt.Errorf("failed to wait for PerconaPGCluster %s/%s deletion: %w", pg.GetNamespace(), pg.GetName(), err)
	}

	if err := k8sClient.Create(ctx, pg); err != nil {
		return fmt.Errorf("failed to create PerconaPGCluster %s/%s: %w", pg.GetNamespace(), pg.GetName(), err)
	}

	// Wait for the PGCluster to be ready.
	if err := wait.PollUntilContextCancel(ctx, defaultRetryInterval, false, func(ctx context.Context) (bool, error) {
		pgCheck := &pgv2.PerconaPGCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: pg.GetNamespace(), Name: pg.GetName()}, pgCheck); err != nil {
			return false, fmt.Errorf("failed to get PerconaPGCluster %s/%s: %w", pg.GetNamespace(), pg.GetName(), err)
		}
		return pgCheck.Status.State == pgv2.AppStateReady, nil
	}); err != nil {
		return fmt.Errorf("failed to wait for PerconaPGCluster %s/%s to be ready: %w", pg.GetNamespace(), pg.GetName(), err)
	}
	return nil
}

func cleanup(ctx context.Context, k8sClient client.Client, namespace, secretName string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
	}
	if err := k8sClient.Delete(ctx, secret); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete secret %s/%s: %w", namespace, secretName, err)
	}
	return nil
}
