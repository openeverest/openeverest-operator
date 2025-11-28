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

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

const (
	// Consts used for calculating resources.

	// Kibibyte represents 1 KiB.
	Kibibyte = 1024
	// Mebibyte represents 1 MiB.
	Mebibyte = 1024 * Kibibyte
	// pmmClientRequestCPUSmall are the default CPU requests for PMM client in small clusters.
	pmmClientRequestCPUSmall = 95
	// pmmClientRequestCPUMedium are the default CPU requests for PMM client in medium clusters.
	pmmClientRequestCPUMedium = 228
	// pmmClientRequestCPULarge are the default CPU requests for PMM client in large clusters.
	pmmClientRequestCPULarge = 228
)

var (
	// NOTE: provided below values were taken from the tool https://github.com/Tusamarco/mysqloperatorcalculator

	// PmmResourceRequirementsSmall is the resource requirements for PMM for small clusters.
	PmmResourceRequirementsSmall = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			// 97.27Mi = 97 MiB + 276 KiB = 99604 KiB
			corev1.ResourceMemory: *resource.NewQuantity(97*Mebibyte+276*Kibibyte, resource.BinarySI),
			corev1.ResourceCPU:    *resource.NewScaledQuantity(pmmClientRequestCPUSmall, resource.Milli),
		},
	}

	// PmmResourceRequirementsMedium is the resource requirements for PMM for medium clusters.
	PmmResourceRequirementsMedium = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			// 194.5Mi = 194 MiB + 512 KiB = 199168 KiB
			corev1.ResourceMemory: *resource.NewQuantity(194*Mebibyte+512*Kibibyte, resource.BinarySI),
			corev1.ResourceCPU:    *resource.NewScaledQuantity(pmmClientRequestCPUMedium, resource.Milli),
		},
	}

	// PmmResourceRequirementsLarge is the resource requirements for PMM for large clusters.
	PmmResourceRequirementsLarge = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			// 778.23Mi = 778 MiB + 235 KiB = 796907 KiB
			corev1.ResourceMemory: *resource.NewQuantity(778*Mebibyte+235*Kibibyte, resource.BinarySI),
			corev1.ResourceCPU:    *resource.NewScaledQuantity(pmmClientRequestCPULarge, resource.Milli),
		},
	}
)

// CalculatePMMResources returns the resource requirements for PMM based on database engine size.
func CalculatePMMResources(dbEnginSize everestv1alpha1.EngineSize) corev1.ResourceRequirements {
	switch dbEnginSize {
	case everestv1alpha1.EngineSizeMedium:
		return PmmResourceRequirementsMedium
	case everestv1alpha1.EngineSizeLarge:
		return PmmResourceRequirementsLarge
	default:
		return PmmResourceRequirementsSmall
	}
}
