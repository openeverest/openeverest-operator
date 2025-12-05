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

package predicates

import (
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// GetBackupStoragePredicate returns a predicate that filters events for BackupStorage resources.
func GetBackupStoragePredicate() predicate.Funcs {
	return predicate.Funcs{
		// When BackupStorage is created, it is not used in any DatabaseCluster yet.
		// Nothing to process.
		CreateFunc: func(_ event.CreateEvent) bool {
			return false
		},

		UpdateFunc: func(e event.UpdateEvent) bool {
			oldBS, oldOk := e.ObjectOld.(*everestv1alpha1.BackupStorage)
			newBS, newOk := e.ObjectNew.(*everestv1alpha1.BackupStorage)
			if !oldOk || !newOk {
				return false
			}

			// Trigger reconciliation only if meaning fields have changed
			return oldBS.Spec.Type != newBS.Spec.Type ||
				oldBS.Spec.Bucket != newBS.Spec.Bucket ||
				oldBS.Spec.Region != newBS.Spec.Region ||
				oldBS.Spec.EndpointURL != newBS.Spec.EndpointURL ||
				oldBS.Spec.VerifyTLS != newBS.Spec.VerifyTLS ||
				oldBS.Spec.ForcePathStyle != newBS.Spec.ForcePathStyle ||
				oldBS.Spec.CredentialsSecretName != newBS.Spec.CredentialsSecretName
		},

		// BackupStorage can be deleted only in case it is not used -> no need to reconcile DatabaseClusters.
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return false
		},

		// Nothing to process on generic events
		GenericFunc: func(_ event.GenericEvent) bool {
			return false
		},
	}
}
