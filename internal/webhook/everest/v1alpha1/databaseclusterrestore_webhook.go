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
	"github.com/percona/everest-operator/utils"
)

var (
	// .spec.dbClusterName.
	dbcrDbClusterNamePath = specPath.Child("dbClusterName")

	// .spec.dataSource.
	dbcrDataSourcePath = specPath.Child("dataSource")
	// .spec.dataSource.dbClusterBackupName.
	dbcrDbClusterBackupNamePath = dbcrDataSourcePath.Child("dbClusterBackupName")
	// .spec.dataSource.backupSource.
	dbcrBackupSourcePath = dbcrDataSourcePath.Child("backupSource")
	// .spec.dataSource.backupSource.path.
	dbcrBackupSourcePathPath = dbcrBackupSourcePath.Child("path")
	// .spec.dataSource.backupSource.dbcrBackupStorageName.
	dbcrBackupSourceStorageNamePath = dbcrBackupSourcePath.Child("dbcrBackupStorageName")
	// .spec.dataSource.pitr.type.
	dbcrDataSourcePitrTypePath = dbcrDataSourcePath.Child("type")
	// .spec.dataSource.pitr.date.
	dbcrDataSourcePitrDatePath = dbcrDataSourcePath.Child("date")
)

// SetupDatabaseClusterRestoreWebhookWithManager registers the webhook for DatabaseClusterRestore in the manager.
func SetupDatabaseClusterRestoreWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&everestv1alpha1.DatabaseClusterRestore{}).
		WithValidator(&DatabaseClusterRestoreCustomValidator{
			Client: mgr.GetClient(),
		}).
		WithDefaulter(&DatabaseClusterRestoreCustomDefaulter{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		}).
		Complete()
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
//nolint:lll
// +kubebuilder:webhook:path=/validate-everest-percona-com-v1alpha1-databaseclusterrestore,mutating=false,failurePolicy=fail,sideEffects=None,groups=everest.percona.com,resources=databaseclusterrestores,verbs=create;update;delete,versions=v1alpha1,name=vdatabaseclusterrestore-v1alpha1.everest.percona.com,admissionReviewVersions=v1

// DatabaseClusterRestoreCustomValidator struct is responsible for validating the DatabaseClusterRestore resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type DatabaseClusterRestoreCustomValidator struct {
	Client client.Client
}

var (
	_         webhook.CustomValidator = &DatabaseClusterRestoreCustomValidator{}
	groupKind                         = everestv1alpha1.GroupVersion.WithKind("DatabaseClusterRestore").GroupKind()
)

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type DatabaseClusterRestore.
func (v *DatabaseClusterRestoreCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	var allErrs field.ErrorList
	dbcr, ok := obj.(*everestv1alpha1.DatabaseClusterRestore)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseClusterRestore object but got %T", obj)
	}
	logger := logf.FromContext(ctx).WithName("DatabaseClusterRestoreCustomValidator").WithValues(
		"name", dbcr.GetName(),
		"namespace", dbcr.GetNamespace(),
	)
	ctx = logf.IntoContext(ctx, logger)

	logger.Info("Validation for DatabaseClusterRestore upon creation")

	targetDb := &everestv1alpha1.DatabaseCluster{}
	if dbcr.Spec.DBClusterName == "" {
		allErrs = append(allErrs, errRequiredField(dbcrDbClusterNamePath))
	} else {
		// check if the target DatabaseCluster for restoring from backup exists.
		targetDbName := types.NamespacedName{
			Name:      dbcr.Spec.DBClusterName,
			Namespace: dbcr.Namespace,
		}
		if err := v.Client.Get(ctx, targetDbName, targetDb); err != nil {
			msg := fmt.Sprintf("failed to fetch target DatabaseCluster='%s'", targetDbName)
			logger.Error(err, msg)
			allErrs = append(allErrs, field.NotFound(dbcrDbClusterNamePath, msg))
			// without DatabaseCluster object, we cannot continue further validation
			return nil, apierrors.NewInvalid(groupKind, dbcr.GetName(), allErrs)
		}

		// check that the target DatabaseCluster is ready for restoring from backup.
		if !targetDb.Status.Status.CanRestoreFromBackup() {
			allErrs = append(allErrs, field.Forbidden(dbcrDbClusterNamePath, "the target DatabaseCluster's state does not allow restoration from backup"))
		}
	}

	if errs := v.validateDataSourceSpec(ctx, *dbcr, *targetDb); errs != nil {
		allErrs = append(allErrs, errs...)
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(groupKind, dbcr.GetName(), allErrs)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type DatabaseClusterRestore.
func (v *DatabaseClusterRestoreCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	var allErrs field.ErrorList
	oldDbcr, ok := oldObj.(*everestv1alpha1.DatabaseClusterRestore)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseClusterRestore object for the oldObj but got %T", oldObj)
	}

	newDbcr, ok := newObj.(*everestv1alpha1.DatabaseClusterRestore)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseClusterRestore object for the newObj but got %T", newObj)
	}
	logger := logf.FromContext(ctx).WithName("DatabaseClusterRestoreCustomValidator").WithValues(
		"name", oldDbcr.GetName(),
		"namespace", oldDbcr.GetNamespace(),
	)

	logger.Info("Validation for DatabaseClusterRestore upon update", "name", oldDbcr.GetName())

	if !oldDbcr.DeletionTimestamp.IsZero() {
		return nil, nil
	}

	if !equality.Semantic.DeepEqual(newDbcr.Spec, oldDbcr.Spec) {
		allErrs = append(allErrs, errImmutableField(specPath))
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(groupKind, oldDbcr.GetName(), allErrs)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type DatabaseClusterRestore.
func (v *DatabaseClusterRestoreCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	dbcr, ok := obj.(*everestv1alpha1.DatabaseClusterRestore)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseClusterRestore object but got %T", obj)
	}
	logger := logf.FromContext(ctx).WithName("DatabaseClusterRestoreCustomValidator").WithValues(
		"name", dbcr.GetName(),
		"namespace", dbcr.GetNamespace(),
	)
	logger.Info("Validation for DatabaseClusterRestore upon deletion", "name", dbcr.GetName())

	if !dbcr.DeletionTimestamp.IsZero() {
		return nil, nil
	}

	// we should prevent deletion if it is currently in use.
	if utils.IsEverestObjectInUse(dbcr) {
		return nil, apierrors.NewForbidden(
			everestv1alpha1.GroupVersion.WithResource("databaseclusterrestore").GroupResource(),
			dbcr.GetName(),
			errDeleteInUse)
	}

	return nil, nil
}

// Helper functions

func (v *DatabaseClusterRestoreCustomValidator) validateDataSourceSpec(
	ctx context.Context,
	dbcr everestv1alpha1.DatabaseClusterRestore,
	targetDb everestv1alpha1.DatabaseCluster,
) field.ErrorList {
	var allErrs field.ErrorList
	dataSource := dbcr.Spec.DataSource

	if dataSource.DBClusterBackupName == "" && dataSource.BackupSource == nil {
		return append(allErrs, field.Invalid(dbcrDataSourcePath, "",
			fmt.Sprintf("either %s or %s must be specified", dbcrDbClusterBackupNamePath, dbcrBackupSourcePath)))
	}

	if dataSource.DBClusterBackupName != "" && dataSource.BackupSource != nil {
		return append(allErrs, field.Invalid(dbcrDataSourcePath, "",
			fmt.Sprintf("either %s or %s must be specified, but not both", dbcrDbClusterBackupNamePath, dbcrBackupSourcePath)))
	}

	// either dbcr.Spec.DataSource.DBClusterBackupName or dbcr.Spec.DataSource.BackupSource is used for restoration.
	if dataSource.DBClusterBackupName != "" {
		// validate the referenced DatabaseClusterBackup
		if errs := v.validateDBClusterBackup(ctx, dbcr.Spec.DataSource.DBClusterBackupName, targetDb); errs != nil {
			allErrs = append(allErrs, errs...)
		}
	} else {
		// validate the referenced BackupSource
		if errs := v.validateBackupSource(ctx, dbcr); errs != nil {
			allErrs = append(allErrs, errs...)
		}
	}

	// May be set in addition to dbcr.Spec.DataSource.DBClusterBackupName or dbcr.Spec.DataSource.BackupSource to perform PITR restore.
	if dataSource.PITR != nil {
		if errs := v.validatePitrRestoreSpec(*dataSource.PITR); errs != nil {
			allErrs = append(allErrs, errs...)
		}
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

func (v *DatabaseClusterRestoreCustomValidator) validateDBClusterBackup(
	ctx context.Context,
	dbBackupName string,
	targetDb everestv1alpha1.DatabaseCluster,
) field.ErrorList {
	var allErrs field.ErrorList
	logger := logf.FromContext(ctx)

	dbClusterBackupName := types.NamespacedName{
		Name:      dbBackupName,
		Namespace: targetDb.Namespace,
	}
	dbb := &everestv1alpha1.DatabaseClusterBackup{}
	if err := v.Client.Get(ctx, dbClusterBackupName, dbb); err != nil {
		// without DatabaseClusterBackup object, we cannot continue further validation
		msg := fmt.Sprintf("failed to fetch DatabaseClusterBackup='%s'", dbClusterBackupName)
		logger.Error(err, msg)
		return append(allErrs, field.NotFound(dbcrDbClusterBackupNamePath, msg))
	}

	if !dbb.GetDeletionTimestamp().IsZero() {
		// the DatabaseClusterBackup is being deleted and cannot be used for restoration.
		return append(allErrs, field.Forbidden(dbcrDbClusterBackupNamePath,
			fmt.Sprintf("DatabaseClusterBackup='%s' is being deleted and cannot be used for restoration", dbClusterBackupName)))
	}

	// check that the DatabaseClusterBackup is in Succeeded state, otherwise it cannot be used for restoration.
	if dbb.Status.State != everestv1alpha1.BackupSucceeded {
		// the DatabaseClusterBackup cannot be used for restoration.
		// no need to continue validation.
		allErrs = append(allErrs, field.Forbidden(dbcrDbClusterBackupNamePath, "DatabaseClusterBackup must be in Succeeded state"))
	}

	if dbb.Spec.DBClusterName != targetDb.GetName() {
		// attempt to restore from a backup taken from a different DatabaseCluster.
		// check that the DatabaseClusterBackup and target DatabaseCluster have the same engine type.
		// DatabaseClusterBackup doesn't have engine info, so we need to fetch the backup source DatabaseCluster.
		backupSourceDb := &everestv1alpha1.DatabaseCluster{}
		dbClusterName := types.NamespacedName{
			Name:      dbb.Spec.DBClusterName,
			Namespace: dbb.Namespace,
		}
		if err := v.Client.Get(ctx, dbClusterName, backupSourceDb); err != nil {
			// without backup source DatabaseCluster object, we cannot continue further validation
			msg := fmt.Sprintf("failed to fetch backup source DatabaseCluster='%s'", dbClusterName)
			logger.Error(err, msg)
			allErrs = append(allErrs, field.NotFound(dbcrDbClusterNamePath, msg))
		} else if backupSourceDb.Spec.Engine.Type != targetDb.Spec.Engine.Type {
			allErrs = append(allErrs, field.Invalid(dbcrDbClusterBackupNamePath, dbBackupName,
				fmt.Sprintf("the engine of the target DatabaseCluster %s (%s) and the engine of the backup source DatabaseCluster %s (%s) do not match",
					targetDb.GetName(), targetDb.Spec.Engine.Type, backupSourceDb.GetName(), backupSourceDb.Spec.Engine.Type)))
		}
	}

	// TODO: validate DatabaseClusterBackup and target DatabaseCluster versions compatibility
	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

func (v *DatabaseClusterRestoreCustomValidator) validateBackupSource(ctx context.Context, dbcr everestv1alpha1.DatabaseClusterRestore) field.ErrorList {
	var allErrs field.ErrorList
	backupSource := dbcr.Spec.DataSource.BackupSource
	// if .spec.dataSource.backupSource is not empty, check if all params are set
	if backupSource.Path == "" {
		allErrs = append(allErrs, errRequiredField(dbcrBackupSourcePathPath))
	}

	if backupSource.BackupStorageName == "" {
		allErrs = append(allErrs, errRequiredField(dbcrBackupSourceStorageNamePath))
	} else {
		// check if the referenced BackupStorage exists
		bs := &everestv1alpha1.BackupStorage{}
		backupStorageName := types.NamespacedName{
			Name:      backupSource.BackupStorageName,
			Namespace: dbcr.Namespace,
		}
		if err := v.Client.Get(ctx, backupStorageName, bs); err != nil {
			msg := fmt.Sprintf("failed to fetch BackupStorage='%s'", backupStorageName)
			logf.FromContext(ctx).Error(err, msg)
			allErrs = append(allErrs, field.NotFound(dbcrBackupSourceStorageNamePath, msg))
		}
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

func (v *DatabaseClusterRestoreCustomValidator) validatePitrRestoreSpec(pitr everestv1alpha1.PITR) field.ErrorList {
	var allErrs field.ErrorList
	switch pitr.Type {
	case everestv1alpha1.PITRTypeDate:
		if pitr.Date == nil {
			allErrs = append(allErrs, errRequiredField(dbcrDataSourcePitrDatePath))
		}
	default:
		// TODO: figure out why PITRTypeLatest doesn't work currently for Everest
		allErrs = append(allErrs, field.NotSupported(dbcrDataSourcePitrTypePath, pitr.Type, []everestv1alpha1.PITRType{everestv1alpha1.PITRTypeDate}))
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}
