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

package controllers

import (
	"fmt"
	"testing"
	"time"

	opfwv1alpha1 "github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

func Test_ParseCSVName(t *testing.T) {
	t.Parallel()
	name, version := parseOperatorCSVName("percona-server-mongodb-operator.v1.0.0")
	assert.Equal(t, "percona-server-mongodb-operator", name)
	assert.Equal(t, "1.0.0", version)

	name, version = parseOperatorCSVName("percona-server-mongodb-operator")
	assert.Equal(t, "", name)
	assert.Equal(t, "", version)

	name, version = parseOperatorCSVName("")
	assert.Equal(t, "", name)
	assert.Equal(t, "", version)
}

func TestGetInstallPlanRefsForUpgrade(t *testing.T) {
	t.Parallel()
	dbEngine := &everestv1alpha1.DatabaseEngine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-db-operator",
			Namespace: "everest",
		},
		Spec: everestv1alpha1.DatabaseEngineSpec{},
		Status: everestv1alpha1.DatabaseEngineStatus{
			OperatorVersion: "0.0.1",
		},
	}

	subscription := &opfwv1alpha1.Subscription{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operators.coreos.com/v1alpha1",
			Kind:       "Subscription",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-db-operator",
			Namespace: "everest",
			UID:       "uid-1234",
		},
	}

	ownerRef := metav1.OwnerReference{
		APIVersion: subscription.APIVersion,
		Kind:       subscription.Kind,
		Name:       subscription.GetName(),
		UID:        subscription.GetUID(),
	}

	testCases := []struct {
		installplans *opfwv1alpha1.InstallPlanList
		expected     map[string]string
	}{
		{
			installplans: &opfwv1alpha1.InstallPlanList{
				Items: []opfwv1alpha1.InstallPlan{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "installplan-1",
							CreationTimestamp: metav1.Time{
								Time: time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC),
							},
							OwnerReferences: []metav1.OwnerReference{ownerRef},
						},
						Spec: opfwv1alpha1.InstallPlanSpec{
							ClusterServiceVersionNames: []string{"test-db-operator.v0.0.2"},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "installplan-2",
							CreationTimestamp: metav1.Time{
								Time: time.Date(2024, 1, 1, 10, 35, 0, 0, time.UTC),
							},
							OwnerReferences: []metav1.OwnerReference{ownerRef},
						},
						Spec: opfwv1alpha1.InstallPlanSpec{
							ClusterServiceVersionNames: []string{
								"test-db-operator.v0.0.2",
								"test-anotherdb-operator.v0.0.1",
							},
						},
					},
				},
			},
			expected: map[string]string{
				"0.0.2": "installplan-2",
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			t.Parallel()
			result := getInstallPlanRefsForUpgrade(dbEngine, subscription, tc.installplans)
			assert.Equal(t, tc.expected, result)
		})
	}
}
