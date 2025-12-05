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

// Package utils ...
//

package utils

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// IsEverestReadOnlyObject Checks whether the Everest object is in use.
// Returns true in case "everest.percona.com/readonly-protection" finalizer is present.
func IsEverestReadOnlyObject(obj client.Object) bool {
	return controllerutil.ContainsFinalizer(obj, everestv1alpha1.ReadOnlyFinalizer)
}

// IsEverestObjectInUse Checks whether the Everest object is in use.
// Returns true in case "everest.percona.com/in-use-protection" finalizer is present.
func IsEverestObjectInUse(obj client.Object) bool {
	return controllerutil.ContainsFinalizer(obj, everestv1alpha1.InUseResourceFinalizer)
}
