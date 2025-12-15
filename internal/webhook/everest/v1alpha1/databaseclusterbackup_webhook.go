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

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/utils"
)

const (
	pgReposLimit = 3
)

var (
	// .spec.backupStorageName.
	dbcbBackupStorageNamePath = specPath.Child("backupStorageName")

	// .spec.dbClusterName.
	dbcbDBClusterNamePath = specPath.Child("dbClusterName")
)

// SetupDatabaseClusterBackupWebhookWithManager registers the webhook for DatabaseClusterBackup in the manager.
func SetupDatabaseClusterBackupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&everestv1alpha1.DatabaseClusterBackup{}).
		WithValidator(&DatabaseClusterBackupCustomValidator{
			Client: mgr.GetClient(),
		}).
		Complete()
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
//nolint:lll
// +kubebuilder:webhook:path=/validate-everest-percona-com-v1alpha1-databaseclusterbackup,mutating=false,failurePolicy=fail,sideEffects=None,groups=everest.percona.com,resources=databaseclusterbackups,verbs=create;update;delete,versions=v1alpha1,name=vdatabaseclusterbackup-v1alpha1.everest.percona.com,admissionReviewVersions=v1

// DatabaseClusterBackupCustomValidator struct is responsible for validating the DatabaseClusterBackup resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type DatabaseClusterBackupCustomValidator struct {
	Client client.Client
}

var (
	_                              webhook.CustomValidator = &DatabaseClusterBackupCustomValidator{}
	databaseClusterBackupGroupKind                         = everestv1alpha1.GroupVersion.WithKind("DatabaseClusterBackup").GroupKind()
)

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type DatabaseClusterBackup.
func (v *DatabaseClusterBackupCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	var allErrs field.ErrorList
	dbcb, ok := obj.(*everestv1alpha1.DatabaseClusterBackup)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseClusterBackup object but got %T", obj)
	}
	logger := logf.FromContext(ctx).WithName("DatabaseClusterBackupCustomValidator").WithValues(
		"name", dbcb.GetName(),
		"namespace", dbcb.GetNamespace(),
	)
	ctx = logf.IntoContext(ctx, logger)

	logger.Info("Validation for DatabaseClusterBackup upon creation")

	if dbcb.Spec.BackupStorageName == "" {
		allErrs = append(allErrs, errRequiredField(dbcbBackupStorageNamePath))
	} else {
		// check if the referenced BackupStorage exists
		bs := &everestv1alpha1.BackupStorage{}
		backupStorageName := types.NamespacedName{
			Name:      dbcb.Spec.BackupStorageName,
			Namespace: dbcb.Namespace,
		}
		if err := v.Client.Get(ctx, backupStorageName, bs); err != nil {
			msg := fmt.Sprintf("failed to fetch BackupStorage='%s'", backupStorageName)
			logger.Error(err, msg)
			allErrs = append(allErrs, field.NotFound(dbcbBackupStorageNamePath, msg))
		}

		if !bs.DeletionTimestamp.IsZero() {
			msg := fmt.Sprintf("BackupStorage='%s' is being deleted", backupStorageName)
			logger.Error(nil, msg)
			allErrs = append(allErrs, field.Forbidden(dbcbBackupStorageNamePath, msg))
		}
	}

	if dbcb.Spec.DBClusterName == "" {
		allErrs = append(allErrs, errRequiredField(dbcbDBClusterNamePath))
	} else {
		// Checking if the referenced DBCluster exists.
		targetDb := &everestv1alpha1.DatabaseCluster{}
		targetDbName := types.NamespacedName{
			Name:      dbcb.Spec.DBClusterName,
			Namespace: dbcb.Namespace,
		}
		if err := v.Client.Get(ctx, targetDbName, targetDb); err != nil {
			msg := fmt.Sprintf("failed to fetch DatabaseCluster='%s'", targetDbName)
			logger.Error(err, msg)
			allErrs = append(allErrs, field.NotFound(dbcbDBClusterNamePath, msg))
			// without DatabaseCluster object, we cannot continue further validation
			return nil, apierrors.NewInvalid(databaseClusterBackupGroupKind, dbcb.GetName(), allErrs)
		}

		// check that the target DatabaseCluster is ready for restoring from backup.
		if errs := v.validateTargetDBClusterForBackup(ctx, *dbcb, *targetDb); errs != nil {
			allErrs = append(allErrs, errs...)
		}
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(databaseClusterBackupGroupKind, dbcb.GetName(), allErrs)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type DatabaseClusterBackup.
func (v *DatabaseClusterBackupCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	var allErrs field.ErrorList
	oldDbcb, ok := oldObj.(*everestv1alpha1.DatabaseClusterBackup)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseClusterBackup object for the oldObj but got %T", oldObj)
	}

	newDbcb, ok := newObj.(*everestv1alpha1.DatabaseClusterBackup)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseClusterBackup object for the newObj but got %T", newObj)
	}

	logger := logf.FromContext(ctx).WithName("DatabaseClusterBackupCustomValidator").WithValues(
		"name", oldDbcb.GetName(),
		"namespace", oldDbcb.GetNamespace(),
	)
	logger.Info("Validation for DatabaseClusterBackup upon update")

	if !oldDbcb.DeletionTimestamp.IsZero() {
		return nil, nil
	}

	if !equality.Semantic.DeepEqual(oldDbcb.Spec, newDbcb.Spec) {
		allErrs = append(allErrs, errImmutableField(specPath))
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(databaseClusterBackupGroupKind, oldDbcb.GetName(), allErrs)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type DatabaseClusterBackup.
func (v *DatabaseClusterBackupCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	dbcb, ok := obj.(*everestv1alpha1.DatabaseClusterBackup)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseClusterBackup object but got %T", obj)
	}
	logger := logf.FromContext(ctx).WithName("DatabaseClusterBackupCustomValidator").WithValues(
		"name", dbcb.GetName(),
		"namespace", dbcb.GetNamespace(),
	)
	logger.Info("Validation for DatabaseClusterBackup upon deletion")

	if !dbcb.DeletionTimestamp.IsZero() {
		return nil, nil
	}

	// we should prevent deletion if it is currently in use.
	if utils.IsEverestObjectInUse(dbcb) {
		return nil, apierrors.NewForbidden(
			everestv1alpha1.GroupVersion.WithResource("databaseclusterbackup").GroupResource(),
			dbcb.GetName(),
			errDeleteInUse)
	}

	return nil, nil
}

func (v *DatabaseClusterBackupCustomValidator) validateTargetDBClusterForBackup(
	ctx context.Context,
	dbcb everestv1alpha1.DatabaseClusterBackup,
	db everestv1alpha1.DatabaseCluster,
) field.ErrorList {
	var allErrs field.ErrorList

	if !db.DeletionTimestamp.IsZero() {
		// nothing to validate further if the target DBCluster is being deleted
		return append(allErrs, field.Invalid(dbcbDBClusterNamePath, dbcb.Spec.DBClusterName, "the target DatabaseCluster is being deleted"))
	}

	switch db.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePSMDB:
		if db.Status.ActiveStorage != "" && db.Status.ActiveStorage != dbcb.Spec.BackupStorageName {
			return append(allErrs, field.Invalid(dbcbDBClusterNamePath, dbcb.Spec.DBClusterName, "can't change the active storage for PSMDB clusters"))
		}
	case everestv1alpha1.DatabaseEnginePostgresql:
		if errs := v.validatePGReposForBackup(ctx, db); errs != nil {
			allErrs = append(allErrs, errs...)
		}
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

func (v *DatabaseClusterBackupCustomValidator) validatePGReposForBackup(ctx context.Context, db everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	backupList, err := common.DatabaseClusterBackupsThatReferenceObject(
		ctx,
		v.Client,
		consts.DBClusterBackupDBClusterNameField,
		db.GetNamespace(),
		db.GetName())
	if err != nil {
		return append(allErrs, field.Invalid(dbcbDBClusterNamePath, db.GetName(), err.Error()))
	}

	bs := make(map[string]struct{})
	for _, shed := range db.Spec.Backup.Schedules {
		bs[shed.BackupStorageName] = struct{}{}
	}

	for _, backup := range backupList.Items {
		// repos count is increased only if there wasn't such a BS used
		if _, ok := bs[backup.Spec.BackupStorageName]; !ok {
			bs[backup.Spec.BackupStorageName] = struct{}{}
		}
	}

	// second check if there are too many repos used.
	if len(bs) > pgReposLimit {
		return append(allErrs, field.Forbidden(dbcbBackupStorageNamePath,
			fmt.Sprintf("only %d different storages are allowed in a PostgreSQL cluster", pgReposLimit)))
	}

	return nil
}
