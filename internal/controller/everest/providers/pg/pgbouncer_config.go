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

package pg

import (
	"fmt"
	"strings"

	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/postgres-operator.crunchydata.com/v1beta1"
	"gopkg.in/yaml.v3"
)

// rawPGBouncerConfig is the YAML structure for DatabaseCluster.spec.proxy.config.
// Supports top-level keys: global, databases, users.
// See https://www.pgbouncer.org/config.html
type rawPGBouncerConfig struct {
	Global    map[string]interface{} `yaml:"global"`
	Databases map[string]interface{} `yaml:"databases"`
	Users     map[string]interface{} `yaml:"users"`
}

// ParsePGBouncerConfig parses the proxy config YAML string from DatabaseCluster.spec.proxy.config
// into a PGBouncerConfiguration. Empty or whitespace-only config returns an empty config.
func ParsePGBouncerConfig(configYAML string) (crunchyv1beta1.PGBouncerConfiguration, error) {
	out := crunchyv1beta1.PGBouncerConfiguration{}
	s := strings.TrimSpace(configYAML)
	if s == "" {
		return out, nil
	}

	var raw rawPGBouncerConfig
	if err := yaml.Unmarshal([]byte(s), &raw); err != nil {
		return out, fmt.Errorf("parse pgBouncer config YAML: %w", err)
	}

	out.Global = toStringMap(raw.Global)
	out.Databases = toStringMap(raw.Databases)
	out.Users = toStringMap(raw.Users)
	return out, nil
}

func toStringMap(m map[string]interface{}) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if v == nil {
			out[k] = ""
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	return out
}
