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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/go-ini/ini"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/postgres-operator.crunchydata.com/v1beta1"
)

const (
	pgBouncerSectionGlobal    = "pgbouncer"
	pgBouncerSectionDatabases = "databases"
	pgBouncerSectionUsers     = "users"
)

// ConfigParser represents a parser for PG config.
type ConfigParser struct {
	config string
}

// NewConfigParser returns a new parser for PG config.
func NewConfigParser(config string) *ConfigParser {
	return &ConfigParser{
		config: config,
	}
}

// ParsePGConfig parses a PG config file.
func (p *ConfigParser) ParsePGConfig() (map[string]any, error) {
	res := make(map[string]any)
	b := bufio.NewReader(strings.NewReader(p.config))

	for {
		l, bErr := b.ReadBytes('\n')
		if bErr != nil && !errors.Is(bErr, io.EOF) {
			return nil, bErr
		}

		parser, err := p.newParser(bytes.TrimRight(l, "\r\n"))
		if err != nil {
			return nil, err
		}

		ks := parser.Section("").Keys()
		if len(ks) > 0 {
			if len(ks) > 1 {
				return nil, fmt.Errorf("too many keys in PG config line %q", l)
			}

			k := ks[0]
			res[k.Name()] = k.String()
		}

		if errors.Is(bErr, io.EOF) {
			break
		}
	}

	return res, nil
}

func (p *ConfigParser) newParser(line []byte) (*ini.File, error) {
	delims := "="
	if !p.lineUsesEqualSign(line) {
		delims = " "
	}

	return ini.LoadSources(ini.LoadOptions{
		KeyValueDelimiters: delims,
	}, line)
}

// ParsePgBouncerConfig parses a free-form pgbouncer.ini string into the
// upstream crunchyv1beta1.PGBouncerConfiguration struct.
//
// The input is expected to use the standard pgbouncer.ini sections:
//
//	[pgbouncer]   -> mapped to Global
//	[databases]   -> mapped to Databases
//	[users]       -> mapped to Users
//
// Keys outside any section (i.e. flat `key = value` lines without a section
// header) are treated as if they belonged to the [pgbouncer] section, so users
// can paste a minimal snippet without a header. Unknown sections result in an
// error so users get fast feedback.
//
// Duplicate keys within a section are resolved by last-one-wins (the default
// behavior of the underlying ini parser).
//
// An empty input returns a zero-value struct without error (no-op).
func ParsePgBouncerConfig(config string) (crunchyv1beta1.PGBouncerConfiguration, error) {
	out := crunchyv1beta1.PGBouncerConfiguration{}
	if strings.TrimSpace(config) == "" {
		return out, nil
	}

	f, err := ini.LoadSources(ini.LoadOptions{
		IgnoreInlineComment: false,
	}, []byte(config))
	if err != nil {
		return crunchyv1beta1.PGBouncerConfiguration{}, fmt.Errorf("failed to parse pgbouncer config: %w", err)
	}

	for _, name := range f.SectionStrings() {
		sec := f.Section(name)
		// Treat the default (unnamed) section as [pgbouncer] so users can
		// paste flat key=value lines without a section header.
		effective := strings.ToLower(name)
		if name == ini.DefaultSection {
			effective = pgBouncerSectionGlobal
		}

		switch effective {
		case pgBouncerSectionGlobal:
			if len(sec.Keys()) == 0 {
				continue
			}
			if out.Global == nil {
				out.Global = make(map[string]string)
			}
			for k, v := range sec.KeysHash() {
				out.Global[k] = v
			}
		case pgBouncerSectionDatabases:
			if len(sec.Keys()) == 0 {
				continue
			}
			if out.Databases == nil {
				out.Databases = make(map[string]string)
			}
			for k, v := range sec.KeysHash() {
				out.Databases[k] = v
			}
		case pgBouncerSectionUsers:
			if len(sec.Keys()) == 0 {
				continue
			}
			if out.Users == nil {
				out.Users = make(map[string]string)
			}
			for k, v := range sec.KeysHash() {
				out.Users[k] = v
			}
		default:
			return crunchyv1beta1.PGBouncerConfiguration{}, fmt.Errorf(
				"unknown pgbouncer config section %q: expected one of [pgbouncer], [databases], [users]", name)
		}
	}

	return out, nil
}

// PG config supports the following two formats per config line:
// name = value
// name value
//
// This method helps determine which one it is.
func (p *ConfigParser) lineUsesEqualSign(line []byte) bool {
	idxSpace := bytes.Index(line, []byte{' '})
	idxEqual := bytes.Index(line, []byte{'='})

	if idxSpace == -1 {
		return true
	}

	if idxEqual == -1 {
		return false
	}
	if idxEqual < idxSpace {
		return true
	}

	for i := idxSpace + 1; i < idxEqual; i++ {
		if line[i] != ' ' {
			return false
		}
	}

	return true
}
