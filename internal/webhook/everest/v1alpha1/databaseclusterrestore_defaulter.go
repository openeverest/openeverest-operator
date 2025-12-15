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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

//nolint:lll
// +kubebuilder:webhook:path=/mutate-everest-percona-com-v1alpha1-databaseclusterrestore,mutating=true,failurePolicy=fail,sideEffects=None,groups=everest.percona.com,resources=databaseclusterrestores,verbs=create;update,versions=v1alpha1,name=mdatabaseclusterrestore-v1alpha1.everest.percona.com,admissionReviewVersions=v1

// DatabaseClusterRestoreCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind DatabaseClusterRestore when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type DatabaseClusterRestoreCustomDefaulter struct{}

var _ webhook.CustomDefaulter = &DatabaseClusterRestoreCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind DatabaseClusterRestore.
func (d *DatabaseClusterRestoreCustomDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	dbcr, ok := obj.(*everestv1alpha1.DatabaseClusterRestore)
	if !ok {
		return fmt.Errorf("expected an DatabaseClusterRestore object but got %T", obj)
	}

	logger := logf.FromContext(ctx).WithName("DatabaseClusterRestoreDefaulter").WithValues(
		"name", dbcr.GetName(),
		"namespace", dbcr.GetNamespace(),
	)

	logger.Info("Mutating DatabaseClusterRestore")

	if dbcr.Spec.DataSource.PITR != nil && dbcr.Spec.DataSource.PITR.Type == "" {
		dbcr.Spec.DataSource.PITR.Type = everestv1alpha1.PITRTypeDate
	}

	return nil
}
