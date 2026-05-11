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

package psmdb

import (
	"testing"

	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

//nolint:maintidx
func TestGetPMMResources(t *testing.T) {
	t.Parallel()

	type res struct {
		memory string
		cpu    string
	}

	type resourceString struct {
		requests res
		limits   res
	}

	tests := []struct {
		name           string
		isNewDBCluster bool
		dbSpec         *everestv1alpha1.DatabaseClusterSpec
		curPsmdbSpec   *psmdbv1.PerconaServerMongoDBSpec
		want           corev1.ResourceRequirements
		wantString     resourceString
	}{
		// enable monitoring for new DB cluster w/o requested resources
		{
			name:           "New small cluster w/o requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
			},
			curPsmdbSpec: nil,
			want:         common.PmmResourceRequirementsSmall,
			wantString: resourceString{
				requests: res{
					memory: "99604Ki",
					cpu:    "95m",
				},
			},
		},
		{
			name:           "New medium cluster w/o requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPsmdbSpec: nil,
			want:         common.PmmResourceRequirementsMedium,
			wantString: resourceString{
				requests: res{
					memory: "199168Ki",
					cpu:    "228m",
				},
			},
		},
		{
			name:           "New large cluster w/o requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
			},
			curPsmdbSpec: nil,
			want:         common.PmmResourceRequirementsLarge,
			wantString: resourceString{
				requests: res{
					memory: "796907Ki",
					cpu:    "228m",
				},
			},
		},
		// enable monitoring for new DB cluster with requested resources
		{
			name:           "New small cluster with requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("100m"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("110M"),
						},
					},
				},
			},
			curPsmdbSpec: nil,
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("99604Ki"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("110M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "99604Ki",
					cpu:    "100m",
				},
				limits: res{
					memory: "110M",
				},
			},
		},
		{
			name:           "New medium cluster with requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("300M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("300m"),
						},
					},
				},
			},
			curPsmdbSpec: nil,
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("228m"),
					corev1.ResourceMemory: resource.MustParse("300M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("300m"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "300M",
					cpu:    "228m",
				},
				limits: res{
					cpu: "300m",
				},
			},
		},
		{
			name:           "New large cluster with requested resources",
			isNewDBCluster: true,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("500M"),
						},
					},
				},
			},
			curPsmdbSpec: nil,
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("228m"),
					corev1.ResourceMemory: resource.MustParse("796907Ki"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("500M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "796907Ki",
					cpu:    "228m",
				},
				limits: res{
					memory: "500M",
					cpu:    "500m",
				},
			},
		},
		// enable monitoring for existing DB cluster w/o monitoring and w/o requested resources
		{
			name:           "Existing small cluster w/o monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("1G"),
								},
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsSmall,
			wantString: resourceString{
				requests: res{
					memory: "99604Ki",
					cpu:    "95m",
				},
			},
		},
		{
			name:           "Existing medium cluster w/o monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("8G"),
								},
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsMedium,
			wantString: resourceString{
				requests: res{
					memory: "199168Ki",
					cpu:    "228m",
				},
			},
		},
		{
			name:           "Existing large cluster w/o monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("32G"),
								},
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsLarge,
			wantString: resourceString{
				requests: res{
					memory: "796907Ki",
					cpu:    "228m",
				},
			},
		},
		// enable monitoring for existing DB cluster w/o monitoring and with requested resources
		{
			name:           "Existing small cluster w/o monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("100M"),
						},
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("1G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("100M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "100M",
					cpu:    "100m",
				},
			},
		},
		{
			name:           "Existing medium cluster w/o monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("300m"),
							corev1.ResourceMemory: resource.MustParse("300M"),
						},
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("8G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("300m"),
					corev1.ResourceMemory: resource.MustParse("300M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "300M",
					cpu:    "300m",
				},
			},
		},
		{
			name:           "Existing large cluster w/o monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("500M"),
						},
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("32G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("500M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "500M",
					cpu:    "500m",
				},
			},
		},
		// enable monitoring and change DB engine size for existing DB cluster w/o monitoring and w/o requested resources
		{
			name:           "Existing small->medium cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("1G"),
								},
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsMedium,
			wantString: resourceString{
				requests: res{
					memory: "199168Ki",
					cpu:    "228m",
				},
			},
		},
		{
			name:           "Existing medium->large cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("8G"),
								},
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsLarge,
			wantString: resourceString{
				requests: res{
					memory: "796907Ki",
					cpu:    "228m",
				},
			},
		},
		{
			name:           "Existing large->medium cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("32G"),
								},
							},
						},
					},
				},
			},
			want: common.PmmResourceRequirementsMedium,
			wantString: resourceString{
				requests: res{
					memory: "199168Ki",
					cpu:    "228m",
				},
			},
		},
		// enable monitoring and change DB engine size for existing DB cluster w/o monitoring and with requested resources
		{
			name:           "Existing small->medium cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("100M"),
						},
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("1G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("100M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "100M",
					cpu:    "100m",
				},
			},
		},
		{
			name:           "Existing medium->large cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("300m"),
							corev1.ResourceMemory: resource.MustParse("300M"),
						},
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("8G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("300m"),
					corev1.ResourceMemory: resource.MustParse("300M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "300M",
					cpu:    "300m",
				},
			},
		},
		{
			name:           "Existing large ->medium cluster w/o monitoring and w/o requested resources and change DB engine size",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("500M"),
						},
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: false,
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("32G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("500M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "500M",
					cpu:    "500m",
				},
			},
		},
		// enable monitoring for existing DB cluster with monitoring and w/o requested resources - keep current resources
		{
			name:           "Existing small cluster with monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("110m"),
							corev1.ResourceMemory: resource.MustParse("110M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("115m"),
							corev1.ResourceMemory: resource.MustParse("115M"),
						},
					},
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("1G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("110m"),
					corev1.ResourceMemory: resource.MustParse("110M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("115m"),
					corev1.ResourceMemory: resource.MustParse("115M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "110M",
					cpu:    "110m",
				},
				limits: res{
					memory: "115M",
					cpu:    "115m",
				},
			},
		},
		{
			name:           "Existing medium cluster with monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("220m"),
							corev1.ResourceMemory: resource.MustParse("220M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("225m"),
							corev1.ResourceMemory: resource.MustParse("225M"),
						},
					},
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("8G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("220m"),
					corev1.ResourceMemory: resource.MustParse("220M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("225m"),
					corev1.ResourceMemory: resource.MustParse("225M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "220M",
					cpu:    "220m",
				},
				limits: res{
					memory: "225M",
					cpu:    "225m",
				},
			},
		},
		{
			name:           "Existing large cluster with monitoring and w/o requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("330m"),
							corev1.ResourceMemory: resource.MustParse("330M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("335m"),
							corev1.ResourceMemory: resource.MustParse("335M"),
						},
					},
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("32G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("330m"),
					corev1.ResourceMemory: resource.MustParse("330M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("335m"),
					corev1.ResourceMemory: resource.MustParse("335M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "330M",
					cpu:    "330m",
				},
				limits: res{
					memory: "335M",
					cpu:    "335m",
				},
			},
		},
		// enable monitoring for existing DB cluster with monitoring and with requested resources - merge resources
		{
			name:           "Existing small cluster with monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("1G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("111m"),
							corev1.ResourceMemory: resource.MustParse("111M"),
						},
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("110m"),
							corev1.ResourceMemory: resource.MustParse("110M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("115m"),
							corev1.ResourceMemory: resource.MustParse("115M"),
						},
					},
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("1G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("111m"),
					corev1.ResourceMemory: resource.MustParse("111M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("115m"),
					corev1.ResourceMemory: resource.MustParse("115M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "111M",
					cpu:    "111m",
				},
				limits: res{
					memory: "115M",
					cpu:    "115m",
				},
			},
		},
		{
			name:           "Existing medium cluster with monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("8G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("222m"),
							corev1.ResourceMemory: resource.MustParse("222M"),
						},
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("220m"),
							corev1.ResourceMemory: resource.MustParse("220M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("225m"),
							corev1.ResourceMemory: resource.MustParse("225M"),
						},
					},
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("8G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("222m"),
					corev1.ResourceMemory: resource.MustParse("222M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("225m"),
					corev1.ResourceMemory: resource.MustParse("225M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "222M",
					cpu:    "222m",
				},
				limits: res{
					memory: "225M",
					cpu:    "225m",
				},
			},
		},
		{
			name:           "Existing large cluster with monitoring and with requested resources",
			isNewDBCluster: false,
			dbSpec: &everestv1alpha1.DatabaseClusterSpec{
				Engine: everestv1alpha1.Engine{
					Resources: everestv1alpha1.Resources{
						Memory: resource.MustParse("32G"),
					},
				},
				Monitoring: &everestv1alpha1.Monitoring{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("333m"),
							corev1.ResourceMemory: resource.MustParse("333M"),
						},
					},
				},
			},
			curPsmdbSpec: &psmdbv1.PerconaServerMongoDBSpec{
				PMM: psmdbv1.PMMSpec{
					Enabled: true,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("330m"),
							corev1.ResourceMemory: resource.MustParse("330M"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("335m"),
							corev1.ResourceMemory: resource.MustParse("335M"),
						},
					},
				},
				Replsets: []*psmdbv1.ReplsetSpec{
					{
						Name: rsName(0),
						MultiAZ: psmdbv1.MultiAZ{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("32G"),
								},
							},
						},
					},
				},
			},
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("333m"),
					corev1.ResourceMemory: resource.MustParse("333M"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("335m"),
					corev1.ResourceMemory: resource.MustParse("335M"),
				},
			},
			wantString: resourceString{
				requests: res{
					memory: "333M",
					cpu:    "333m",
				},
				limits: res{
					memory: "335M",
					cpu:    "335m",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			calculatedResources := getPMMResources(tt.isNewDBCluster, tt.dbSpec, tt.curPsmdbSpec)
			assert.True(t, tt.want.Requests.Cpu().Equal(*calculatedResources.Requests.Cpu()))
			assert.True(t, tt.want.Requests.Memory().Equal(*calculatedResources.Requests.Memory()))
			assert.True(t, tt.want.Limits.Cpu().Equal(*calculatedResources.Limits.Cpu()))
			assert.True(t, tt.want.Limits.Memory().Equal(*calculatedResources.Limits.Memory()))

			if tt.wantString.requests.memory != "" {
				assert.Equal(t, tt.wantString.requests.memory, calculatedResources.Requests.Memory().String())
			}

			if tt.wantString.requests.cpu != "" {
				assert.Equal(t, tt.wantString.requests.cpu, calculatedResources.Requests.Cpu().String())
			}

			if tt.wantString.limits.memory != "" {
				assert.Equal(t, tt.wantString.limits.memory, calculatedResources.Limits.Memory().String())
			}

			if tt.wantString.limits.cpu != "" {
				assert.Equal(t, tt.wantString.limits.cpu, calculatedResources.Limits.Cpu().String())
			}
		})
	}
}
