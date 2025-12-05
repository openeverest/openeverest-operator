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

package pxc

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	// A haproxyConfigDefault is the default HAProxy configuration.
	//nolint:lll
	haProxyConfigDefault = `
    global
      log stdout format raw local0
      maxconn 4048
      external-check
      insecure-fork-wanted
      hard-stop-after 10s
      stats socket /etc/haproxy/pxc/haproxy.sock mode 600 expose-fd listeners level admin

    defaults
      no option dontlognull
      log-format '{"time":"%t", "client_ip": "%ci", "client_port":"%cp", "backend_source_ip": "%bi", "backend_source_port": "%bp",  "frontend_name": "%ft", "backend_name": "%b", "server_name":"%s", "tw": "%Tw", "tc": "%Tc", "Tt": "%Tt", "bytes_read": "%B", "termination_state": "%ts", "actconn": "%ac", "feconn" :"%fc", "beconn": "%bc", "srv_conn": "%sc", "retries": "%rc", "srv_queue": "%sq", "backend_queue": "%bq" }'
      default-server init-addr last,libc,none
      log global
      mode tcp
      retries 10
      timeout client 28800s
      timeout connect 100500
      timeout server 28800s

    resolvers kubernetes
      parse-resolv-conf

    frontend galera-in
      bind *:3309 accept-proxy
      bind *:3306
      mode tcp
      option clitcpka
      default_backend galera-nodes

    frontend galera-admin-in
      bind *:33062
      mode tcp
      option clitcpka
      default_backend galera-admin-nodes

    frontend galera-replica-in
      bind *:3307
      mode tcp
      option clitcpka
      default_backend galera-replica-nodes

    frontend galera-mysqlx-in
      bind *:33060
      mode tcp
      option clitcpka
      default_backend galera-mysqlx-nodes

    frontend stats
      bind *:8404
      mode http
      http-request use-service prometheus-exporter if { path /metrics }
`

	haConnectionTimeout = 5000
)

// A haProxyEnvVars contains the environment variables to be set in the HAProxy container.
var haProxyEnvVars = map[string][]byte{
	"HA_CONNECTION_TIMEOUT": []byte(strconv.Itoa(haConnectionTimeout)),
}

var (
	// A haProxyResourceRequirementsSmall is the resource requirements for HAProxy for small clusters.
	haProxyResourceRequirementsSmall = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("195Mi"),
			corev1.ResourceCPU:    resource.MustParse("190m"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("204Mi"),
			corev1.ResourceCPU:    resource.MustParse("200m"),
		},
	}

	// A haProxyResourceRequirementsMedium is the resource requirements for HAProxy for medium clusters.
	haProxyResourceRequirementsMedium = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("778Mi"),
			corev1.ResourceCPU:    resource.MustParse("532m"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("820Mi"),
			corev1.ResourceCPU:    resource.MustParse("560m"),
		},
	}

	// A haProxyResourceRequirementsLarge is the resource requirements for HAProxy for large clusters.
	haProxyResourceRequirementsLarge = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("2.84Gi"),
			corev1.ResourceCPU:    resource.MustParse("818m"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("3Gi"),
			corev1.ResourceCPU:    resource.MustParse("861m"),
		},
	}
)
