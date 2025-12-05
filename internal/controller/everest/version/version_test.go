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

package version

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

func TestVersion(t *testing.T) {
	t.Parallel()
	v, err := NewVersion("1.11.0")
	require.NoError(t, err)

	assert.Equal(t, "1.11.0", v.ToCRVersion())
	assert.Equal(t, "1.11.0", v.String())
	assert.Equal(t, "v1.11.0", v.ToSemver())
	assert.Equal(t, "v1-11-0", v.ToK8sVersion())
	assert.Equal(t, "pxc.percona.com/v1-11-0", v.ToAPIVersion("pxc.percona.com"))
}
