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
