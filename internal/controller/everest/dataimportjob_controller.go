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

// Package everest contains the DataImportJobReconciler which manages DataImportJob resources.
package everest

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/AlekSi/pointer"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/api/everest/v1alpha1/dataimporterspec"
	"github.com/percona/everest-operator/internal/consts"
)

const (
	dataImporterRequestSecretNameSuffix = "-data-import-request" //nolint:gosec
	dataImportJSONSecretKey             = "request.json"
	payloadMountPath                    = "/payload"

	kindRole        = "Role"
	kindClusterRole = "ClusterRole"
)

// DataImportJobReconciler reconciles DataImportJob resources.
type DataImportJobReconciler struct {
	Client client.Client
	Scheme *runtime.Scheme
}

// SetupWithManager sets up the controller with the Manager.
func (r *DataImportJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("DataImportJob").
		For(&everestv1alpha1.DataImportJob{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&rbacv1.Role{}).
		Watches(&rbacv1.ClusterRoleBinding{}, clusterWideResourceHandler()).
		Watches(&rbacv1.ClusterRole{}, clusterWideResourceHandler()).
		Complete(r)
}

// clusterWideResourceHandler returns an event handler that enqueues requests for DataImportJob
// when cluster-wide resources like ClusterRole or ClusterRoleBinding are created, updated, or deleted.
// It uses the `dataImportJobOwnerLabel` to find the owner DataImportJob and enqueue a request for it.
func clusterWideResourceHandler() handler.EventHandler { //nolint:ireturn
	return handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []ctrl.Request {
		labels := o.GetLabels()
		name, ok := labels[consts.DataImportJobRefNameLabel]
		if !ok {
			return nil
		}
		namespace, ok := labels[consts.DataImportJobRefNamespaceLabel]
		if !ok {
			return nil
		}
		return []ctrl.Request{
			{
				NamespacedName: client.ObjectKey{
					Name:      name,
					Namespace: namespace,
				},
			},
		}
	})
}

//+kubebuilder:rbac:groups=everest.percona.com,resources=dataimportjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=everest.percona.com,resources=dataimportjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=everest.percona.com,resources=dataimportjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=create;get;list;watch;create;update;patch;delete;escalate;bind
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=create;get;list;watch;create;update;patch;delete;escalate;bind
//+kubebuilder:rbac:groups=everest.percona.com,resources=dataimporters,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *DataImportJobReconciler) Reconcile( //nolint:nonamedreturns
	ctx context.Context,
	req ctrl.Request,
) (rr ctrl.Result, rerr error) {
	logger := log.FromContext(ctx).
		WithName("DataImportJobReconciler").
		WithValues(
			"name", req.Name,
			"namespace", req.Namespace,
		)
	logger.Info("Reconciling")
	defer func() {
		logger.Info("Reconciled")
	}()

	diJob := &everestv1alpha1.DataImportJob{}
	if err := r.Client.Get(ctx, req.NamespacedName, diJob); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !diJob.GetDeletionTimestamp().IsZero() {
		ok, err := r.handleFinalizers(ctx, diJob)
		if err != nil {
			logger.Error(err, "Failed to handle finalizers")
			return ctrl.Result{}, err
		}

		result := ctrl.Result{}
		if !ok {
			result.RequeueAfter = 5 * time.Second //nolint:mnd
		}

		return result, nil
	}

	if diJob.Status.State == everestv1alpha1.DataImportJobStateSucceeded ||
		diJob.Status.State == everestv1alpha1.DataImportJobStateFailed {
		// Already complete, no need to reconcile again.
		return ctrl.Result{}, nil
	}

	// Reset the status, we will build a new one by observing the current state on each reconcile.
	startedAt := diJob.Status.StartedAt
	diJob.Status = everestv1alpha1.DataImportJobStatus{}
	diJob.Status.LastObservedGeneration = diJob.GetGeneration()
	if startedAt != nil && !startedAt.Time.IsZero() {
		diJob.Status.StartedAt = startedAt
	}

	// Sync status on finishing reconciliation.
	defer func() {
		if updErr := r.Client.Status().Update(ctx, diJob); updErr != nil {
			logger.Error(updErr, "Failed to update data import job status")
			rerr = errors.Join(rerr, updErr)
		}
	}()

	// Get the referenced data importer.
	di := &everestv1alpha1.DataImporter{}
	if err := r.Client.Get(ctx, client.ObjectKey{
		Name: diJob.Spec.DataImporterName,
	}, di); err != nil {
		diJob.Status.State = everestv1alpha1.DataImportJobStateError
		diJob.Status.Message = fmt.Errorf("failed to get data importer: %w", err).Error()
		return ctrl.Result{}, err
	}

	// Get the target database cluster.
	db := &everestv1alpha1.DatabaseCluster{}
	if err := r.Client.Get(ctx, client.ObjectKey{
		Name:      diJob.Spec.TargetClusterName,
		Namespace: diJob.GetNamespace(),
	}, db); err != nil {
		diJob.Status.State = everestv1alpha1.DataImportJobStateError
		diJob.Status.Message = fmt.Errorf("failed to get database cluster: %w", err).Error()
		return ctrl.Result{}, err
	}

	// Create payload secret.
	if err := r.ensureDataImportPayloadSecret(ctx, diJob, db); err != nil {
		diJob.Status.State = everestv1alpha1.DataImportJobStateError
		diJob.Status.Message = fmt.Errorf("failed to create data import payload secret: %w", err).Error()
		return ctrl.Result{}, err
	}

	// Create RBAC resources.
	requiresRbac := len(di.Spec.Permissions) > 0 || len(di.Spec.ClusterPermissions) > 0
	if requiresRbac { //nolint:nestif
		if err := r.ensureServiceAccount(ctx, diJob); err != nil {
			diJob.Status.State = everestv1alpha1.DataImportJobStateError
			diJob.Status.Message = fmt.Errorf("failed to ensure service account: %w", err).Error()
			return ctrl.Result{}, err
		}
		if err := r.ensureRBACResources(ctx, diJob, di.Spec.Permissions, di.Spec.ClusterPermissions); err != nil {
			diJob.Status.State = everestv1alpha1.DataImportJobStateError
			diJob.Status.Message = fmt.Errorf("failed to ensure RBAC resources: %w", err).Error()
			return ctrl.Result{}, err
		}

		if controllerutil.AddFinalizer(diJob, consts.DataImportJobRBACCleanupFinalizer) {
			if err := r.Client.Update(ctx, diJob); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to add finalizer to data import job: %w", err)
			}
		}
	}

	// Create import job.
	if err := r.ensureImportJob(ctx, requiresRbac, diJob, di); err != nil {
		diJob.Status.State = everestv1alpha1.DataImportJobStateError
		diJob.Status.Message = fmt.Errorf("failed to create import job: %w", err).Error()
		return ctrl.Result{}, err
	}

	// Observe import state.
	if err := r.observeImportState(ctx, diJob); err != nil {
		diJob.Status.State = everestv1alpha1.DataImportJobStateError
		diJob.Status.Message = fmt.Errorf("failed to observe state: %w", err).Error()
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *DataImportJobReconciler) observeImportState(ctx context.Context, diJob *everestv1alpha1.DataImportJob) error {
	jobName := diJob.Status.JobName
	if jobName == "" {
		diJob.Status.State = everestv1alpha1.DataImportJobStatePending
		return nil
	}

	job := &batchv1.Job{}
	if err := r.Client.Get(ctx, client.ObjectKey{
		Name:      jobName,
		Namespace: diJob.GetNamespace(),
	}, job); err != nil {
		return fmt.Errorf("failed to get import job: %w", err)
	}

	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			// Job is complete, delete the secret.
			if err := r.Client.Delete(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dataImporterRequestSecretName(diJob),
					Namespace: diJob.GetNamespace(),
				},
			}); err != nil {
				return fmt.Errorf("failed to delete data import request secret: %w", err)
			}
			diJob.Status.State = everestv1alpha1.DataImportJobStateSucceeded
			diJob.Status.CompletedAt = job.Status.CompletionTime
			return nil
		}

		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			diJob.Status.State = everestv1alpha1.DataImportJobStateFailed
			diJob.Status.Message = c.Message
			return nil
		}
	}
	diJob.Status.State = everestv1alpha1.DataImportJobStateRunning
	return nil
}

func (r *DataImportJobReconciler) ensureDataImportPayloadSecret(
	ctx context.Context,
	diJob *everestv1alpha1.DataImportJob,
	db *everestv1alpha1.DatabaseCluster,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataImporterRequestSecretName(diJob),
			Namespace: diJob.GetNamespace(),
		},
	}

	// If the Secret already exists with the desired key, we will not attempt create it again.
	// Typically we should actively reconcile the controlled objects, but in this case
	// we do not expect the contents of the Secret to change at all so its okay to skip it, unless
	// the Secret or its data are missing altogether.
	// Moreover, even if the Secret does change, there's nothing that the DataImporter can do
	// because it has already started with the initially provided details.
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(secret), secret); err == nil {
		if val, ok := secret.Data[dataImportJSONSecretKey]; ok && len(val) > 0 {
			return nil
		}
	} else if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to get data import request secret: %w", err)
	}

	req := dataimporterspec.Spec{}

	dbUser, dbPassword, err := r.getDBRootUserCredentials(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get database root user credentials: %w", err)
	}
	req.Target = dataimporterspec.Target{
		User:     dbUser,
		Password: dbPassword,
		Host:     db.Status.Hostname,
		Port:     strconv.Itoa(int(db.Status.Port)),
		Type:     string(db.Spec.Engine.Type),
		DatabaseClusterRef: &dataimporterspec.ObjectReference{
			Name:      db.GetName(),
			Namespace: db.GetNamespace(),
		},
	}

	// Get S3 info
	s3Info, err := r.getS3Info(ctx, diJob)
	if err != nil {
		return fmt.Errorf("failed to get S3 info: %w", err)
	}
	req.Source.S3 = s3Info
	req.Source.Path = diJob.Spec.Source.Path

	cfgMap := map[string]any{}

	if cfg := diJob.Spec.Config; cfg != nil {
		if err := json.Unmarshal(cfg.Raw, &cfgMap); err != nil {
			return fmt.Errorf("failed to unmarshal data import job parameters: %w", err)
		}
	}
	req.Config = cfgMap

	reqJSON, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal data import payload: %w", err)
	}
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Data = map[string][]byte{
			dataImportJSONSecretKey: reqJSON,
		}
		if err := controllerutil.SetControllerReference(diJob, secret, r.Scheme); err != nil {
			return fmt.Errorf("failed to set controller reference: %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to create or update data import request secret: %w", err)
	}
	return nil
}

func (r *DataImportJobReconciler) getS3Info(
	ctx context.Context,
	dij *everestv1alpha1.DataImportJob,
) (*dataimporterspec.S3, error) {
	if dij.Spec.Source.S3 == nil {
		return nil, errors.New("s3 info not provided")
	}
	info := dataimporterspec.S3{}

	keyID, keySecret, err := r.getS3Credentials(ctx, dij)
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 credentials: %w", err)
	}
	info.AccessKeyID = keyID
	info.SecretKey = keySecret
	info.Bucket = dij.Spec.Source.S3.Bucket
	info.Region = dij.Spec.Source.S3.Region
	info.EndpointURL = dij.Spec.Source.S3.EndpointURL
	info.VerifyTLS = pointer.Get(dij.Spec.Source.S3.VerifyTLS)
	info.ForcePathStyle = pointer.Get(dij.Spec.Source.S3.ForcePathStyle)
	return &info, nil
}

func (r *DataImportJobReconciler) getS3Credentials(
	ctx context.Context,
	dij *everestv1alpha1.DataImportJob,
) (string, string, error) {
	namespace := dij.GetNamespace()
	secretName := dij.Spec.Source.S3.CredentialsSecretName
	secret := &corev1.Secret{}
	if err := r.Client.Get(ctx, client.ObjectKey{
		Name:      secretName,
		Namespace: namespace,
	}, secret); err != nil {
		return "", "", err
	}

	// Take ownership of the secret
	if err := controllerutil.SetControllerReference(dij, secret, r.Scheme); err != nil &&
		!errors.Is(err, &controllerutil.AlreadyOwnedError{}) {
		return "", "", fmt.Errorf("failed to set controller reference for secret %s: %w", secretName, err)
	}
	if err := r.Client.Update(ctx, secret); err != nil {
		return "", "", fmt.Errorf("failed to update secret %s: %w", secretName, err)
	}

	accessKey := secret.Data["AWS_ACCESS_KEY_ID"]
	secretKey := secret.Data["AWS_SECRET_ACCESS_KEY"]
	return string(accessKey), string(secretKey), nil
}

func (r *DataImportJobReconciler) getDBRootUserCredentials(
	ctx context.Context,
	db *everestv1alpha1.DatabaseCluster,
) (string, string, error) {
	secret := &corev1.Secret{}
	if err := r.Client.Get(ctx, client.ObjectKey{
		Name:      db.Spec.Engine.UserSecretsName,
		Namespace: db.GetNamespace(),
	}, secret); err != nil {
		return "", "", err
	}

	switch db.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePostgresql:
		username := "postgres"
		pwd, ok := secret.Data["password"]
		if !ok {
			return "", "", errors.New("postgres password not found in secret")
		}
		return username, string(pwd), nil
	case everestv1alpha1.DatabaseEnginePXC:
		username := "root"
		pwd, ok := secret.Data["root"]
		if !ok {
			return "", "", errors.New("root password not found in secret")
		}
		return username, string(pwd), nil
	case everestv1alpha1.DatabaseEnginePSMDB:
		username, ok := secret.Data["MONGODB_DATABASE_ADMIN_USER"]
		if !ok {
			return "", "", errors.New("mongodb admin user not found in secret")
		}
		pwd, ok := secret.Data["MONGODB_DATABASE_ADMIN_PASSWORD"]
		if !ok {
			return "", "", errors.New("mongodb admin password not found in secret")
		}
		return string(username), string(pwd), nil
	default:
		return "", "", fmt.Errorf("unsupported database engine type: %s", db.Spec.Engine.Type)
	}
}

func (r *DataImportJobReconciler) ensureImportJob(
	ctx context.Context,
	useServiceAccount bool,
	diJob *everestv1alpha1.DataImportJob,
	di *everestv1alpha1.DataImporter,
) error {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataImporterJobName(diJob),
			Namespace: diJob.GetNamespace(),
		},
	}

	diJob.Status.JobName = job.GetName()

	// Check if the job already exists.
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get import job: %w", err)
		}
	} else if err == nil {
		return nil
	}

	serviceAccount := ""
	if useServiceAccount {
		serviceAccount = r.getServiceAccountName(diJob)
	}
	job.Spec = r.getJobSpec(diJob, di, serviceAccount)
	if err := controllerutil.SetControllerReference(diJob, job, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}
	if err := r.Client.Create(ctx, job); err != nil {
		return err
	}
	diJob.Status.StartedAt = pointer.To(metav1.Now())
	return nil
}

func dataImporterJobName(diJob *everestv1alpha1.DataImportJob) string {
	uuid := diJob.GetUID()
	hash := md5.Sum([]byte(uuid)) //nolint:gosec
	hashStr := hex.EncodeToString(hash[:])
	return fmt.Sprintf("%s-%s", diJob.GetName(), hashStr[:6])
}

func dataImporterRequestSecretName(diJob *everestv1alpha1.DataImportJob) string {
	return dataImporterJobName(diJob) + dataImporterRequestSecretNameSuffix
}

func (r *DataImportJobReconciler) getJobSpec(
	diJob *everestv1alpha1.DataImportJob,
	di *everestv1alpha1.DataImporter,
	serviceAccountName string,
) batchv1.JobSpec {
	spec := batchv1.JobSpec{
		// Setting it to 0 means we will not retry on failure.
		// TODO: In EVEREST-2108, we will implement failurePolicy, and that's where we shall
		// implement retries. For now we disable retries so it can fail fast.
		// See: https://perconadev.atlassian.net/browse/EVEREST-2108
		BackoffLimit: pointer.ToInt32(0),
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: pointer.ToInt64(30), //nolint:mnd  // TODO: make this configurable?
				ServiceAccountName:            serviceAccountName,
				RestartPolicy:                 corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:    "importer",
					Image:   di.Spec.JobSpec.Image,
					Command: di.Spec.JobSpec.Command,
					Args:    []string{fmt.Sprintf("%s/%s", payloadMountPath, dataImportJSONSecretKey)},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "payload",
							MountPath: payloadMountPath,
							ReadOnly:  true,
						},
					},
				}},
				Volumes: []corev1.Volume{
					{
						Name: "payload",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: dataImporterRequestSecretName(diJob),
							},
						},
					},
				},
			},
		},
	}
	return spec
}

func (r *DataImportJobReconciler) ensureRBACResources(
	ctx context.Context,
	diJob *everestv1alpha1.DataImportJob,
	permissions, clusterPermissions []rbacv1.PolicyRule,
) error {
	if len(permissions) > 0 {
		if err := r.ensureRole(ctx, permissions, diJob); err != nil {
			return fmt.Errorf("failed to ensure role: %w", err)
		}
		if err := r.ensureRoleBinding(ctx, diJob); err != nil {
			return fmt.Errorf("failed to ensure role binding: %w", err)
		}
	}

	if len(clusterPermissions) > 0 {
		if err := r.ensureClusterRole(ctx, clusterPermissions, diJob); err != nil {
			return fmt.Errorf("failed to ensure cluster role: %w", err)
		}
		if err := r.ensureClusterRoleBinding(ctx, diJob); err != nil {
			return fmt.Errorf("failed to ensure cluster role binding: %w", err)
		}
	}
	return nil
}

// Returns: [done(bool), error] .
func (r *DataImportJobReconciler) handleFinalizers(
	ctx context.Context,
	diJob *everestv1alpha1.DataImportJob,
) (bool, error) {
	if controllerutil.ContainsFinalizer(diJob, consts.DataImportJobRBACCleanupFinalizer) {
		return r.deleteResourcesInOrder(ctx, diJob)
	}

	return true, nil
}

// Returns: [done(bool), error] .
func (r *DataImportJobReconciler) deleteJob(ctx context.Context, diJob *everestv1alpha1.DataImportJob) (bool, error) {
	jobName := diJob.Status.JobName
	if jobName == "" {
		return true, nil
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: diJob.GetNamespace(),
		},
	}
	// Terminate the Job.
	if err := r.Client.Delete(ctx, job, &client.DeleteOptions{
		PropagationPolicy: pointer.To(metav1.DeletePropagationForeground),
	}); client.IgnoreNotFound(err) != nil {
		return false, fmt.Errorf("failed to delete job %s: %w", jobName, err)
	}
	// Ensure Pods have terminated before proceeding.
	const jobNameLabel = "job-name"

	pods := &corev1.PodList{}
	if err := r.Client.List(ctx, pods, client.InNamespace(diJob.GetNamespace()), client.MatchingLabels{
		jobNameLabel: jobName,
	}); err != nil {
		return false, fmt.Errorf("failed to list pods for job %s: %w", jobName, err)
	}

	return len(pods.Items) == 0, nil // if no pods are running, we can proceed with RBAC cleanup.
}

// Returns: [done(bool), error] .
func (r *DataImportJobReconciler) deleteRBAC(ctx context.Context, diJob *everestv1alpha1.DataImportJob) (bool, error) {
	// List of RBAC resources.
	resources := []client.Object{
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      r.getRoleBindingName(diJob),
				Namespace: diJob.GetNamespace(),
			},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      r.getRoleName(diJob),
				Namespace: diJob.GetNamespace(),
			},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: r.getClusterRoleBindingName(diJob),
			},
		},
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name:      r.getClusterRoleName(diJob),
				Namespace: diJob.GetNamespace(),
			},
		},
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      r.getServiceAccountName(diJob),
				Namespace: diJob.GetNamespace(),
			},
		},
	}
	allGone := true

	for _, res := range resources {
		err := r.Client.Delete(ctx, res)
		if err == nil {
			allGone = false // Even one successful deletion indicates that not all resources are gone yet.
		} else if client.IgnoreNotFound(err) != nil {
			return false, fmt.Errorf("failed to delete resource %s: %w", res.GetName(), err)
		}
	}

	return allGone, nil
}

// Returns: [done(bool), error] .
func (r *DataImportJobReconciler) deleteResourcesInOrder(ctx context.Context, diJob *everestv1alpha1.DataImportJob) (bool, error) {
	ok, err := r.deleteJob(ctx, diJob)
	if err != nil {
		return false, fmt.Errorf("failed to delete job: %w", err)
	}

	if !ok {
		return false, nil // do not proceed with RBAC cleanup if job deletion is not done
	}

	ok, err = r.deleteRBAC(ctx, diJob)
	if err != nil {
		return false, fmt.Errorf("failed to delete RBAC resources: %w", err)
	}

	if !ok {
		return false, nil
	}
	if controllerutil.RemoveFinalizer(diJob, consts.DataImportJobRBACCleanupFinalizer) {
		if err := r.Client.Update(ctx, diJob); err != nil {
			return false, fmt.Errorf("failed to remove ordered cleanup finalizer: %w", err)
		}
	}

	return true, nil
}

func (r *DataImportJobReconciler) getClusterRoleBindingName(diJob *everestv1alpha1.DataImportJob) string {
	return diJob.GetName() + "-clusterrolebinding"
}

func (r *DataImportJobReconciler) ensureClusterRoleBinding(
	ctx context.Context,
	diJob *everestv1alpha1.DataImportJob,
) error {
	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: r.getClusterRoleBindingName(diJob),
		},
	}
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, clusterRoleBinding, func() error {
		clusterRoleBinding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.SchemeGroupVersion.Group,
			Kind:     kindClusterRole,
			Name:     r.getClusterRoleName(diJob),
		}
		clusterRoleBinding.Subjects = []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      r.getServiceAccountName(diJob),
				Namespace: diJob.GetNamespace(),
			},
		}
		clusterRoleBinding.SetLabels(map[string]string{
			consts.DataImportJobRefNameLabel:      diJob.GetName(),
			consts.DataImportJobRefNamespaceLabel: diJob.GetNamespace(),
		})
		return nil
	}); err != nil {
		return fmt.Errorf("failed to ensure cluster role binding: %w", err)
	}
	return nil
}

func (r *DataImportJobReconciler) getClusterRoleName(diJob *everestv1alpha1.DataImportJob) string {
	return diJob.GetName() + "-clusterrole"
}

func (r *DataImportJobReconciler) ensureClusterRole(
	ctx context.Context,
	permissions []rbacv1.PolicyRule,
	diJob *everestv1alpha1.DataImportJob,
) error {
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: r.getClusterRoleName(diJob),
		},
	}
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, clusterRole, func() error {
		clusterRole.SetLabels(map[string]string{
			consts.DataImportJobRefNameLabel:      diJob.GetName(),
			consts.DataImportJobRefNamespaceLabel: diJob.GetNamespace(),
		})
		clusterRole.Rules = permissions
		return nil
	}); err != nil {
		return fmt.Errorf("failed to ensure cluster role: %w", err)
	}
	return nil
}

func (r *DataImportJobReconciler) getRoleBindingName(diJob *everestv1alpha1.DataImportJob) string {
	return diJob.GetName() + "-rolebinding"
}

func (r *DataImportJobReconciler) ensureRoleBinding(
	ctx context.Context,
	diJob *everestv1alpha1.DataImportJob,
) error {
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.getRoleBindingName(diJob),
			Namespace: diJob.GetNamespace(),
		},
	}
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, roleBinding, func() error {
		roleBinding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.SchemeGroupVersion.Group,
			Kind:     kindRole,
			Name:     r.getRoleName(diJob),
		}
		roleBinding.Subjects = []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      r.getServiceAccountName(diJob),
				Namespace: diJob.GetNamespace(),
			},
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to ensure role binding: %w", err)
	}
	return nil
}

func (r *DataImportJobReconciler) getRoleName(diJob *everestv1alpha1.DataImportJob) string {
	return diJob.GetName() + "-role"
}

func (r *DataImportJobReconciler) ensureRole(
	ctx context.Context,
	permissions []rbacv1.PolicyRule,
	diJob *everestv1alpha1.DataImportJob,
) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.getRoleName(diJob),
			Namespace: diJob.GetNamespace(),
		},
	}
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Rules = permissions
		return nil
	}); err != nil {
		return fmt.Errorf("failed to ensure role: %w", err)
	}
	return nil
}

func (r *DataImportJobReconciler) getServiceAccountName(diJob *everestv1alpha1.DataImportJob) string {
	return diJob.GetName() + "-sa"
}

func (r *DataImportJobReconciler) ensureServiceAccount(
	ctx context.Context,
	diJob *everestv1alpha1.DataImportJob,
) error {
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.getServiceAccountName(diJob),
			Namespace: diJob.GetNamespace(),
		},
	}
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, serviceAccount, func() error {
		return nil
	}); err != nil {
		return fmt.Errorf("failed to ensure service account: %w", err)
	}
	return nil
}
