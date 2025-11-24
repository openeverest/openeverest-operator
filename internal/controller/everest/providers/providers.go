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

// Package providers contains the providers for the DB operators supported by everest.
package providers

import (
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// ProviderOptions contains options for configuring DB providers.
type ProviderOptions struct {
	C        client.Client
	DB       *everestv1alpha1.DatabaseCluster
	DBEngine *everestv1alpha1.DatabaseEngine
}

// HookResult is the result of a pre-reconcile hook.
type HookResult struct {
	Requeue      bool
	RequeueAfter time.Duration
	Message      string
}
