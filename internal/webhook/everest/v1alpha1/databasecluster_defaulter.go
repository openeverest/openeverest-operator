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

// Package v1alpha1 ...
//
//nolint:lll
package v1alpha1

import (
	"context"
	"fmt"

	"github.com/AlekSi/pointer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// +kubebuilder:webhook:path=/mutate-everest-percona-com-v1alpha1-databasecluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=everest.percona.com,resources=databaseclusters,verbs=create;update,versions=v1alpha1,name=mdatabasecluster-v1alpha1.everest.percona.com,admissionReviewVersions=v1
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=create;update;get;list
// +kubebuilder:rbac:groups=everest.percona.com,resources=databaseclusters,verbs=get;list;watch;update

// DatabaseClusterDefaulter is a webhook that sets default values for DatabaseCluster resources.
type DatabaseClusterDefaulter struct {
	// Client is the Kubernetes client used to interact with the cluster.
	Client ctrlclient.Client
}

// Default implements a mutating webhook for DatabaseCluster resources.
func (d *DatabaseClusterDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	var allErrs field.ErrorList

	db, ok := obj.(*everestv1alpha1.DatabaseCluster)
	if !ok {
		return fmt.Errorf("expected a DatabaseCluster object but got %T", obj)
	}

	logger := log.FromContext(ctx).WithName("DatabaseClusterDefaulter").WithValues(
		"name", db.GetName(),
		"namespace", db.GetNamespace(),
	)

	logger.Info("Mutating DatabaseCluster")

	// validate some fields
	// validate .spec.engine.type is supported
	dbEngines := &everestv1alpha1.DatabaseEngineList{}
	if err := d.Client.List(ctx, dbEngines, ctrlclient.InNamespace(db.GetNamespace())); err != nil {
		return err
	}

	var dbEngine everestv1alpha1.DatabaseEngine
	var found bool
	if dbEngine, found = dbEngines.Get(db.Spec.Engine.Type); !found {
		return apierrors.NewInvalid(dbClusterGroupKind, db.GetName(), field.ErrorList{
			field.NotSupported(dbcEngineTypePath, db.Spec.Engine.Type, dbEngines.EngineTypes()),
		})
	}

	// Set the default engine version if not specified
	if db.Spec.Engine.Version == "" {
		db.Spec.Engine.Version = dbEngine.Status.AvailableVersions.Engine.BestVersion()
	}

	importTpl := pointer.Get(db.Spec.DataSource).DataImport
	err := handleS3CredentialsSecret(ctx, d.Client, db.GetNamespace(), importTpl)
	if err != nil {
		logger.Error(err, "handleS3CredentialsSecret failed")
		return err
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(dbClusterGroupKind, db.GetName(), allErrs)
}
