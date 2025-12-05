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

package common

import "errors"

var (
	// ErrPitrTypeIsNotSupported is an error for unsupported PITR type.
	ErrPitrTypeIsNotSupported = errors.New("unknown PITR type")
	// ErrPitrTypeLatest is an error for 'latest' being an unsupported PITR type.
	ErrPitrTypeLatest = errors.New("'latest' type is not supported by Everest yet")
	// ErrPitrEmptyDate is an error for missing PITR date.
	ErrPitrEmptyDate = errors.New("no date provided for PITR of type 'date'")

	// ErrPSMDBOneStorageRestriction is an error for using more than one storage for psmdb clusters.
	ErrPSMDBOneStorageRestriction = errors.New("using more than one storage is not allowed for psmdb clusters")
)
