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

	corev1 "k8s.io/api/core/v1"
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
		{
			// Limits takes precedence over the deprecated Memory field.
			engine:       Engine{Resources: Resources{Memory: resource.MustParse("2Gi"), Limits: &ResourceSpec{Memory: resource.MustParse("8Gi")}}},
			expectedSize: EngineSizeMedium,
		},
		{
			// Requests does not affect sizing.
			engine:       Engine{Resources: Resources{Limits: &ResourceSpec{Memory: resource.MustParse("32Gi")}, Requests: &ResourceSpec{Memory: resource.MustParse("2Gi")}}},
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

func TestResources_ToResourceRequirements(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		resources    Resources
		wantLimits   corev1.ResourceList
		wantRequests corev1.ResourceList
	}{
		{
			name:         "legacy fields populate limits and leave requests empty",
			resources:    Resources{CPU: resource.MustParse("1"), Memory: resource.MustParse("2Gi")},
			wantLimits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
			wantRequests: corev1.ResourceList{},
		},
		{
			name: "explicit limits and requests are honored separately",
			resources: Resources{
				Limits:   &ResourceSpec{CPU: resource.MustParse("2"), Memory: resource.MustParse("4Gi")},
				Requests: &ResourceSpec{CPU: resource.MustParse("1"), Memory: resource.MustParse("2Gi")},
			},
			wantLimits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
			wantRequests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
		},
		{
			name: "explicit limits take precedence over deprecated fields",
			resources: Resources{
				CPU:    resource.MustParse("8"),
				Memory: resource.MustParse("16Gi"),
				Limits: &ResourceSpec{CPU: resource.MustParse("2"), Memory: resource.MustParse("4Gi")},
			},
			wantLimits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
			wantRequests: corev1.ResourceList{},
		},
		{
			name: "partial requests are respected exactly without filling missing values",
			resources: Resources{
				Limits:   &ResourceSpec{CPU: resource.MustParse("2"), Memory: resource.MustParse("4Gi")},
				Requests: &ResourceSpec{CPU: resource.MustParse("1")},
			},
			wantLimits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
			wantRequests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
		},
		{
			name:         "zero values produce empty maps",
			resources:    Resources{},
			wantLimits:   corev1.ResourceList{},
			wantRequests: corev1.ResourceList{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.resources.ToResourceRequirements()
			if !resourceListEqual(got.Limits, tc.wantLimits) {
				t.Errorf("limits = %v, want %v", got.Limits, tc.wantLimits)
			}
			if !resourceListEqual(got.Requests, tc.wantRequests) {
				t.Errorf("requests = %v, want %v", got.Requests, tc.wantRequests)
			}
		})
	}
}

func TestResources_UsesLegacyResourceFields(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		resources Resources
		want      bool
	}{
		{
			name:      "only deprecated fields set",
			resources: Resources{CPU: resource.MustParse("1"), Memory: resource.MustParse("2Gi")},
			want:      true,
		},
		{
			name:      "empty resources",
			resources: Resources{},
			want:      true,
		},
		{
			name:      "explicit limits set",
			resources: Resources{Limits: &ResourceSpec{CPU: resource.MustParse("1")}},
			want:      false,
		},
		{
			name:      "explicit requests set",
			resources: Resources{Requests: &ResourceSpec{CPU: resource.MustParse("1")}},
			want:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.resources.UsesLegacyResourceFields(); got != tc.want {
				t.Errorf("UsesLegacyResourceFields() = %v, want %v", got, tc.want)
			}
		})
	}
}

func resourceListEqual(a, b corev1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		other, ok := b[k]
		if !ok || v.Cmp(other) != 0 {
			return false
		}
	}
	return true
}
