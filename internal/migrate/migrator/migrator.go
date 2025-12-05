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

// Package migrator contains binary to migrate resources between versions
package migrator

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/coordination/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

const (
	systemNamespaceEnvVar              = "SYSTEM_NAMESPACE"
	leaseName                          = "everest-migration-lease"
	lastMigrationVersionAnnotationName = "last-migration-version"
	defaultEKSLoadBalancerConfigName   = "eks-default"
)

// Migrator is the struct representing the migrator.
type Migrator struct {
	l               logr.Logger
	currentVersion  string
	systemNamespace string
	client          client.Client
}

// NewMigrator creates a new migrator.
func NewMigrator(logger logr.Logger) (*Migrator, error) {
	logger.Info("Initializing Migrator", "currentVersion", consts.Version)
	m := &Migrator{
		l:               logger,
		currentVersion:  consts.Version,
		systemNamespace: os.Getenv(systemNamespaceEnvVar),
	}

	cl, err := client.New(config.GetConfigOrDie(), client.Options{Scheme: m.BuildScheme()})
	if err != nil {
		return nil, fmt.Errorf("unable to create client: %w", err)
	}

	m.client = cl

	return m, nil
}

// Migrate function that performs CRs migration.
func (m *Migrator) Migrate(ctx context.Context) (info string, rerr error) { //nolint:nonamedreturns
	lease, err := m.createLeaseIfNotExists(ctx)
	if err != nil {
		return "", fmt.Errorf("unable to prepare lease: %w", err)
	}

	defer func() {
		if rerr != nil {
			return
		}

		lease.Annotations[lastMigrationVersionAnnotationName] = m.currentVersion

		err = m.client.Update(ctx, lease)
		if err != nil {
			rerr = fmt.Errorf("unable to update lease: %w", err)
		}
	}()

	if lease.Annotations[lastMigrationVersionAnnotationName] == m.currentVersion {
		return "migration is already performed", nil
	}

	info, err = m.migrateResources(ctx)
	if err != nil {
		return "", fmt.Errorf("unable to perform migration: %w", err)
	}

	return info, nil
}

// BuildScheme creates and returns the scheme for k8s client.
func (m *Migrator) BuildScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(storagev1.AddToScheme(scheme))
	utilruntime.Must(everestv1alpha1.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))

	return scheme
}

func (m *Migrator) createLeaseIfNotExists(ctx context.Context) (*v1.Lease, error) {
	meta := metav1.ObjectMeta{
		Name:      leaseName,
		Namespace: m.systemNamespace,
	}
	lease := &v1.Lease{
		ObjectMeta: meta,
	}

	err := m.client.Get(ctx, types.NamespacedName{Name: leaseName, Namespace: m.systemNamespace}, lease)
	if k8serrors.IsNotFound(err) {
		lease.Annotations = map[string]string{
			lastMigrationVersionAnnotationName: "",
		}

		err = m.client.Create(ctx, lease)
		if err != nil {
			return nil, err
		}

		err = m.client.Get(ctx, types.NamespacedName{Name: leaseName, Namespace: m.systemNamespace}, lease)
		if err != nil {
			return nil, err
		}

		return lease, nil
	}

	if err != nil {
		return nil, err
	}

	return lease, nil
}

// This is the list of actions to be performed in the operator init container during the migration to the new
// Everest version. Update this method for each next release.
func (m *Migrator) migrateResources(_ context.Context) (string, error) { //nolint:unparam
	return "no resources migration is needed for Everest " + m.currentVersion, nil
}
