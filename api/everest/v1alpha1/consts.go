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

const (
	// ReadOnlyFinalizer is the finalizer for marking the resource as read-only.
	// User cannot delete resources marked by this finalizer directly.
	ReadOnlyFinalizer = "everest.percona.com/readonly-protection"

	// InUseResourceFinalizer is the finalizer for marking the resource as "in-use"
	// and prevents deletion of the resource.
	InUseResourceFinalizer = "everest.percona.com/in-use-protection"
)
