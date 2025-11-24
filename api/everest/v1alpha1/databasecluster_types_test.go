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
	"reflect"
	"strconv"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestDatabaseClusterReconciler_toCIDR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		ranges []IPSourceRange
		want   []IPSourceRange
	}{
		{
			name:   "shall not make any changes",
			ranges: []IPSourceRange{"1.1.1.1/32", "1.1.1.1/24", "2001:db8:abcd:0012::0/64", "2001:db8:abcd:0012::0/128"},
			want:   []IPSourceRange{"1.1.1.1/32", "1.1.1.1/24", "2001:db8:abcd:0012::0/64", "2001:db8:abcd:0012::0/128"},
		},
		{
			name:   "shall not fail with empty",
			ranges: []IPSourceRange{},
			want:   []IPSourceRange{},
		},
		{
			name:   "shall fix ipv4 and ipv6",
			ranges: []IPSourceRange{"1.1.1.1/32", "1.1.1.1", "2001:db8:abcd:0012::0/64", "2001:db8:abcd:0012::0"},
			want:   []IPSourceRange{"1.1.1.1/32", "1.1.1.1/32", "2001:db8:abcd:0012::0/64", "2001:db8:abcd:0012::0/128"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := Expose{}
			if got := e.toCIDR(tt.ranges); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expose.toCIDR() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatabaseCluster_Size(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		engine       Engine
		expectedSize EngineSize
	}{
		{
			engine:       Engine{Resources: Resources{Memory: resource.MustParse("2Gi")}},
			expectedSize: EngineSizeSmall,
		},
		{
			engine:       Engine{Resources: Resources{Memory: resource.MustParse("3Gi")}},
			expectedSize: EngineSizeSmall,
		},
		{
			engine:       Engine{Resources: Resources{Memory: resource.MustParse("8Gi")}},
			expectedSize: EngineSizeMedium,
		},
		{
			engine:       Engine{Resources: Resources{Memory: resource.MustParse("12Gi")}},
			expectedSize: EngineSizeMedium,
		},
		{
			engine:       Engine{Resources: Resources{Memory: resource.MustParse("32Gi")}},
			expectedSize: EngineSizeLarge,
		},
		{
			engine:       Engine{Resources: Resources{Memory: resource.MustParse("64Gi")}},
			expectedSize: EngineSizeLarge,
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			if tc.engine.Size() != tc.expectedSize {
				t.Errorf("expected size %s, got %s", tc.expectedSize, tc.engine.Size())
			}
		})
	}
}
