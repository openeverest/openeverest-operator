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
	"errors"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

var (
	// .spec.
	specPath = field.NewPath("spec")

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

	// Deletion errors.
	errDeleteInUse = errors.New("is used by some DB cluster and cannot be deleted")
)
