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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

type (
	// Service used for the integration with Percona Version Service.
	Service struct {
		url string
	}
	// Matrix represents the response from the version service.
	Matrix struct {
		Backup       map[string]*everestv1alpha1.Component `json:"backup"`
		Mongod       map[string]*everestv1alpha1.Component `json:"mongod"`
		PXC          map[string]*everestv1alpha1.Component `json:"pxc"`
		ProxySQL     map[string]*everestv1alpha1.Component `json:"proxysql"`
		HAProxy      map[string]*everestv1alpha1.Component `json:"haproxy"`
		LogCollector map[string]*everestv1alpha1.Component `json:"logCollector"`
		Postgresql   map[string]*everestv1alpha1.Component `json:"postgresql"`
		PGBackRest   map[string]*everestv1alpha1.Component `json:"pgbackrest"`
		PGBouncer    map[string]*everestv1alpha1.Component `json:"pgbouncer"`
	}
	// Response is a response model for version service response parsing.
	Response struct {
		Versions []struct {
			Matrix Matrix `json:"matrix"`
		} `json:"versions"`
	}
)

const (
	defaultVersionServiceURL        = "https://check.percona.com/versions/v1"
	versionServiceStatusRecommended = "recommended"
)

var operatorNames = map[everestv1alpha1.EngineType]string{
	everestv1alpha1.DatabaseEnginePXC:        "pxc-operator",
	everestv1alpha1.DatabaseEnginePSMDB:      "psmdb-operator",
	everestv1alpha1.DatabaseEnginePostgresql: "pg-operator",
}

// NewVersionService creates a version service client.
func NewVersionService() *Service {
	versionServiceURL := os.Getenv("PERCONA_VERSION_SERVICE_URL")
	if versionServiceURL == "" {
		versionServiceURL = defaultVersionServiceURL
	}
	return &Service{url: versionServiceURL}
}

// GetVersions returns a matrix of available versions for a database engine.
func (v *Service) GetVersions(engineType everestv1alpha1.EngineType, operatorVersion string) (*Matrix, error) {
	resp, err := http.Get(fmt.Sprintf("%s/%s/%s", v.url, operatorNames[engineType], operatorVersion)) //nolint:noctx
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	var vr Response
	if err := json.NewDecoder(resp.Body).Decode(&vr); err != nil {
		return nil, err
	}
	if len(vr.Versions) == 0 {
		return nil, errors.New("no versions returned from version service")
	}
	return &vr.Versions[0].Matrix, nil
}
