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

// Package dataimporterspec ...
package dataimporterspec

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// ReadFromFilepath reads the configuration from a JSON file at the specified filepath.
// Implements the dash convention (hyphen convention) to read from stdin if the filepath is "-".
func (in *Spec) ReadFromFilepath(filepath string) error {
	var reader io.Reader
	if filepath == "-" {
		// Use the dash convention (hyphen convention) to read from stdin
		reader = os.Stdin
	} else {
		file, err := os.Open(filepath) //nolint:gosec
		if err != nil {
			return fmt.Errorf("error opening config file: %w", err)
		}
		defer file.Close() //nolint:errcheck
		reader = file
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}
	return json.Unmarshal(data, in)
}
