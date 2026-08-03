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

package enginefeatures

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestGetSplitHorizonDNSConfigPredicate(t *testing.T) {
	t.Parallel()

	p := GetSplitHorizonDNSConfigPredicate()

	// Create, Update and Delete events should be reconciled; only Generic
	// events are filtered out.
	assert.True(t, p.Create(event.CreateEvent{}), "create events should be processed")
	assert.True(t, p.Update(event.UpdateEvent{}), "update events should be processed")
	assert.True(t, p.Delete(event.DeleteEvent{}), "delete events should be processed")
	assert.False(t, p.Generic(event.GenericEvent{}), "generic events should be ignored")
}
