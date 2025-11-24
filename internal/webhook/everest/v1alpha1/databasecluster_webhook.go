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

//nolint:lll
package v1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/AlekSi/pointer"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	enginefeatureseverestv1alpha1 "github.com/percona/everest-operator/api/enginefeatures.everest/v1alpha1"
	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

var (
	// .spec.
	specPath = field.NewPath("spec")

	// .spec.engine.
	enginePath          = specPath.Child("engine")
	engineTypePath      = enginePath.Child("type")
	engineVersionPath   = enginePath.Child("version")
	userSecretsNamePath = enginePath.Child("userSecretsName")

	// .spec.engine.DataSource.
	dataSourcePath = specPath.Child("dataSource")
	dataImportPath = dataSourcePath.Child("dataImport")

	// .spec.proxy.
	proxyPath          = specPath.Child("proxy")
	proxyExposePath    = proxyPath.Child("expose")
	proxyExposeLbcPath = proxyExposePath.Child("loadBalancerConfigName")

	// .spec.engineFeatures.
	engineFeaturesPath = specPath.Child("engineFeatures")

	// .spec.engineFeatures.psmdb.
	psmdbEngineFeaturesPath    = engineFeaturesPath.Child("psmdb")
	psmdbShdcEngineFeaturePath = psmdbEngineFeaturesPath.Child("splitHorizonDnsConfigName")

	// Immutable field error generator.
	errImmutableField = func(fieldPath *field.Path) *field.Error {
		return field.Forbidden(fieldPath, "is immutable and cannot be changed")
	}
	// Required field error generator.
	errRequiredField = func(fieldPath *field.Path) *field.Error {
		return field.Required(fieldPath, "can not be empty")
	}

	// Invalid field value error generator.
	errInvalidField = func(fieldPath *field.Path, fieldValue, errMsg string) *field.Error {
		return field.Invalid(fieldPath, fieldValue, errMsg)
	}
)

var dbClusterGroupKind = everestv1alpha1.GroupVersion.WithKind(consts.DatabaseClusterKind).GroupKind()

// SetupDatabaseClusterWebhookWithManager sets up the webhook with the manager.
func SetupDatabaseClusterWebhookWithManager(mgr manager.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&everestv1alpha1.DatabaseCluster{}).
		WithValidator(&DatabaseClusterValidator{
			Client: mgr.GetClient(),
		}).
		WithDefaulter(&DatabaseClusterDefaulter{
			Client: mgr.GetClient(),
		}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-everest-percona-com-v1alpha1-databasecluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=everest.percona.com,resources=databaseclusters,verbs=create;update,versions=v1alpha1,name=vdatabasecluster-v1alpha1.everest.percona.com,admissionReviewVersions=v1

// DatabaseClusterValidator validates the DatabaseCluster resource.
type DatabaseClusterValidator struct {
	Client client.Client
}

// ValidateCreate validates the creation of a DatabaseCluster.
func (v *DatabaseClusterValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	var allErrs field.ErrorList

	db, ok := obj.(*everestv1alpha1.DatabaseCluster)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseCluster, got %T", obj)
	}

	logger := log.FromContext(ctx).WithName("DatabaseClusterValidator").WithValues(
		"name", db.GetName(),
		"namespace", db.GetNamespace(),
	)

	logger.Info("Validation for DatabaseCluster upon creation")

	// Validate the engine version
	if errs := v.validateEngineVersion(ctx, db); errs != nil {
		allErrs = append(allErrs, errs...)
	}

	// If a user secret is specified by the user, ensure that it exists.
	if userSecretsName := db.Spec.Engine.UserSecretsName; userSecretsName != "" {
		// ensure that this secret exists.
		secret := corev1.Secret{}
		if err := v.Client.Get(ctx, types.NamespacedName{
			Name:      userSecretsName,
			Namespace: db.GetNamespace(),
		}, &secret); err != nil {
			allErrs = append(allErrs, errInvalidField(userSecretsNamePath, userSecretsName, err.Error()))
		}
	}

	// If a data import source is specified, validate it.
	if di := pointer.Get(db.Spec.DataSource).DataImport; di != nil {
		if errs := v.validateDataImport(ctx, db); errs != nil {
			allErrs = append(allErrs, errs...)
		}
	}

	if errs := v.validateLoadBalancerConfig(ctx, db.Spec.Proxy.Expose.LoadBalancerConfigName); errs != nil {
		allErrs = append(allErrs, errs...)
	}

	if errs := v.validateEngineFeaturesOnCreate(ctx, db); errs != nil {
		allErrs = append(allErrs, errs...)
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(dbClusterGroupKind, db.GetName(), allErrs)
}

// ValidateUpdate validates the update of a DatabaseCluster.
func (v *DatabaseClusterValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	var allErrs field.ErrorList
	oldDb, ok := oldObj.(*everestv1alpha1.DatabaseCluster)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseCluster for oldDb, got %T", oldObj)
	}

	newDb, ok := newObj.(*everestv1alpha1.DatabaseCluster)
	if !ok {
		return nil, fmt.Errorf("expected a DatabaseCluster for newDb, got %T", newObj)
	}

	logger := log.FromContext(ctx).WithName("DatabaseClusterValidator").WithValues(
		"name", newDb.GetName(),
		"namespace", newDb.GetNamespace(),
	)

	logger.Info("Validation for DatabaseCluster upon update")

	// Validate the engine type immutability
	if oldDb.Spec.Engine.Type != newDb.Spec.Engine.Type {
		return nil, apierrors.NewInvalid(dbClusterGroupKind, oldDb.GetName(), field.ErrorList{
			errImmutableField(engineTypePath),
		})
	}

	// Validate the engine version
	if errs := v.validateEngineVersion(ctx, newDb); errs != nil {
		allErrs = append(allErrs, errs...)
	}

	// TODO: move remaining validations from Everest API
	// 1. Validate engine version change (upgrade/downgrade)
	// 2. Validate replica count change
	// 3. Validate storage size change
	// 3. Validate sharding constraints

	if errs := v.validateEngineFeaturesOnUpdate(ctx, oldDb, newDb); errs != nil {
		allErrs = append(allErrs, errs...)
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(dbClusterGroupKind, oldDb.GetName(), allErrs)
}

// ValidateDelete validates the deletion of a DatabaseCluster.
func (v *DatabaseClusterValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (v *DatabaseClusterValidator) validateDataImport(
	ctx context.Context,
	db *everestv1alpha1.DatabaseCluster,
) field.ErrorList {
	var allErrs field.ErrorList

	dataImport := pointer.Get(db.Spec.DataSource).DataImport
	if dataImport == nil {
		return nil
	}

	dataimporter := &everestv1alpha1.DataImporter{}
	if err := v.Client.Get(ctx, types.NamespacedName{
		Name: dataImport.DataImporterName,
	}, dataimporter); err != nil {
		return append(allErrs, errInvalidField(dataImportPath, dataImport.DataImporterName, err.Error()))
	}

	// Validate that the DataImporter supports the specified engine.
	engineType := db.Spec.Engine.Type
	if !dataimporter.Spec.SupportedEngines.Has(engineType) {
		return append(allErrs, errInvalidField(dataImportPath, dataImport.DataImporterName,
			fmt.Sprintf("data importer %s does not support engine type %s", dataImport.DataImporterName, engineType)))
	}

	if requiredFields := dataimporter.Spec.DatabaseClusterConstraints.RequiredFields; len(requiredFields) > 0 {
		if errs := validateDataImportRequiredFields(requiredFields, db); errs != nil {
			return append(allErrs, errs...)
		}
	}

	// Validate the params against the schema of the DataImporter
	if err := dataimporter.Spec.Config.Validate(dataImport.Config); err != nil {
		return append(allErrs, errInvalidField(dataImportPath, dataImport.DataImporterName, err.Error()))
	}

	return nil
}

func validateDataImportRequiredFields(requiredFields []string, db *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	for _, rField := range requiredFields {
		exists, err := checkJSONKeyExists(rField, db)
		if err != nil {
			allErrs = append(allErrs, errRequiredField(field.NewPath(rField)))
		}
		if !exists {
			allErrs = append(allErrs, errRequiredField(field.NewPath(rField)))
		}
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

// checkJSONKeyExists returns true if the specified key exists in the JSON object.
// The keyExpr is a dot-separated string representing the path to the key in the JSON object.
func checkJSONKeyExists(keyExpr string, obj any) (bool, error) {
	objB, err := json.Marshal(obj)
	if err != nil {
		return false, fmt.Errorf("failed to marshal object: %w", err)
	}
	objMap := make(map[string]any)
	if err := json.Unmarshal(objB, &objMap); err != nil {
		return false, fmt.Errorf("failed to unmarshal object: %w", err)
	}

	subKeys := slices.DeleteFunc(strings.Split(keyExpr, "."), func(s string) bool {
		return s == ""
	})
	currentSubObject := objMap
	for _, key := range subKeys {
		if value, exists := currentSubObject[key]; exists {
			switch v := value.(type) {
			case map[string]any:
				currentSubObject = v
				continue
			case int, int32, int64, float32, float64:
				return value != 0, nil
			case string:
				return value != "", nil
			}
			continue
		}
		return false, nil
	}
	return true, nil
}

func (v *DatabaseClusterValidator) validateEngineVersion(ctx context.Context, db *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// Get the DatabaseEngine
	if engine, err := common.GetDatabaseEngineForType(ctx, v.Client, db.Spec.Engine.Type, db.GetNamespace()); err != nil {
		allErrs = append(allErrs, errInvalidField(engineTypePath, string(db.Spec.Engine.Type), err.Error()))
	} else {
		// Check if the engine version is available
		if _, ok := engine.Status.AvailableVersions.Engine[db.Spec.Engine.Version]; !ok {
			allErrs = append(allErrs,
				field.NotSupported(engineVersionPath, db.Spec.Engine.Version, engine.Status.AvailableVersions.Engine.GetAllowedVersionsSorted()))
		}
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

func (v *DatabaseClusterValidator) validateLoadBalancerConfig(ctx context.Context, lbcName string) field.ErrorList {
	var allErrs field.ErrorList

	if lbcName == "" {
		return nil
	}

	lbc := everestv1alpha1.LoadBalancerConfig{}
	err := v.Client.Get(ctx, client.ObjectKey{
		Name: lbcName,
	}, &lbc)
	if err != nil {
		return append(allErrs, errInvalidField(proxyExposeLbcPath, lbcName, err.Error()))
	}

	return nil
}

func (v *DatabaseClusterValidator) validateEngineFeaturesOnCreate(ctx context.Context, dbObj *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	switch dbObj.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePXC:
		// validate PXC engine features
		if errs := v.validatePxcEngineFeaturesOnCreate(ctx, dbObj); errs != nil {
			return append(allErrs, errs...)
		}
	case everestv1alpha1.DatabaseEnginePostgresql:
		// validate Postgresql engine features
		if errs := v.validatePostgresqlEngineFeaturesOnCreate(ctx, dbObj); errs != nil {
			return append(allErrs, errs...)
		}
	case everestv1alpha1.DatabaseEnginePSMDB:
		// validate PSMDB engine features
		if errs := v.validatePsmdbEngineFeaturesOnCreate(ctx, dbObj); errs != nil {
			return append(allErrs, errs...)
		}
	default:
		// TODO: add validator calls for the rest of DB Engines.
		return nil
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePxcEngineFeaturesOnCreate(_ context.Context, dbObj *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// We do not support engine features for PXC yet.
	// Check that DB spec doesn't contain engine features related to the other db engines.
	if pointer.Get(dbObj.Spec.EngineFeatures).PSMDB != nil {
		return append(allErrs, errInvalidField(psmdbShdcEngineFeaturePath, "",
			fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePXC)))
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePostgresqlEngineFeaturesOnCreate(_ context.Context, dbObj *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// We do not support engine features for PXC yet.
	// Check that DB spec doesn't contain engine features related to the other db engines.
	if pointer.Get(dbObj.Spec.EngineFeatures).PSMDB != nil {
		return append(allErrs, errInvalidField(psmdbShdcEngineFeaturePath, "",
			fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePostgresql)))
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePsmdbEngineFeaturesOnCreate(ctx context.Context, dbObj *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	psmdbEF := pointer.Get(pointer.Get(dbObj.Spec.EngineFeatures).PSMDB)

	// SplitHorizonDNSConfig
	if psmdbEF.SplitHorizonDNSConfigName != "" {
		// For the time being SplitHorizonDNSConfig is not supported in sharded clusters.
		if pointer.Get(dbObj.Spec.Sharding).Enabled {
			return append(allErrs, field.Forbidden(psmdbShdcEngineFeaturePath, "SplitHorizonDNSConfig and Sharding configuration is not supported"))
		}

		shdc := enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{}
		err := v.Client.Get(ctx, client.ObjectKey{
			Name:      psmdbEF.SplitHorizonDNSConfigName,
			Namespace: dbObj.GetNamespace(),
		}, &shdc)
		if err != nil {
			return append(allErrs, errInvalidField(psmdbShdcEngineFeaturePath, psmdbEF.SplitHorizonDNSConfigName, err.Error()))
		}
	}

	// TODO: validate the rest of PSMDB engine features.
	return nil
}

func (v *DatabaseClusterValidator) validateEngineFeaturesOnUpdate(ctx context.Context, oldDb, newDb *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	if oldDb.Spec.EngineFeatures == nil && newDb.Spec.EngineFeatures == nil {
		// Engine features haven't been used before and are not requested in update request.
		// Nothing to do.
		return nil
	}

	switch oldDb.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePXC:
		// validate PXC engine features
		if errs := v.validatePxcEngineFeaturesOnUpdate(ctx, oldDb, newDb); errs != nil {
			return append(allErrs, errs...)
		}
	case everestv1alpha1.DatabaseEnginePostgresql:
		// validate Postgresql engine features
		if errs := v.validatePostgresqlEngineFeaturesOnUpdate(ctx, oldDb, newDb); errs != nil {
			return append(allErrs, errs...)
		}
	case everestv1alpha1.DatabaseEnginePSMDB:
		// validate PSMDB engine features
		if errs := v.validatePsmdbEngineFeaturesOnUpdate(ctx, oldDb, newDb); errs != nil {
			allErrs = append(allErrs, errs...)
		}
	default:
		// TODO: add validators for the rest of DB Engines later.
		return nil
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

func (v *DatabaseClusterValidator) validatePxcEngineFeaturesOnUpdate(_ context.Context, _, newDb *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// We do not support engine features for PXC yet.
	// Check that DB spec doesn't contain engine features related to the other db engines.
	if pointer.Get(newDb.Spec.EngineFeatures).PSMDB != nil {
		return append(allErrs, errInvalidField(psmdbShdcEngineFeaturePath, "",
			fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePXC)))
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePostgresqlEngineFeaturesOnUpdate(_ context.Context, _, newDb *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// We do not support engine features for PXC yet.
	// Check that DB spec doesn't contain engine features related to the other db engines.
	if pointer.Get(newDb.Spec.EngineFeatures).PSMDB != nil {
		return append(allErrs, errInvalidField(psmdbShdcEngineFeaturePath, "",
			fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePostgresql)))
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePsmdbEngineFeaturesOnUpdate(_ context.Context, oldDb, newDb *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	oldPsmdbEF := pointer.Get(pointer.Get(oldDb.Spec.EngineFeatures).PSMDB)
	newPsmdbEF := pointer.Get(pointer.Get(newDb.Spec.EngineFeatures).PSMDB)

	// SplitHorizonDNSConfig feature
	if oldPsmdbEF.SplitHorizonDNSConfigName != newPsmdbEF.SplitHorizonDNSConfigName {
		// NOTE: for the time being we do not allow to disable/enable/change SplitHorizonDNSConfig for already existing clusters.
		// This feature can be enabled during cluster creation only.
		allErrs = append(allErrs, errImmutableField(psmdbShdcEngineFeaturePath))
	}

	// TODO: validate the rest of PSMDB engine features later.

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}
