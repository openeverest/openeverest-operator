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

package utils

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"regexp"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	maxNameLength = 22
)

var (
	// Regexp used to validate RFC1035 compatible names.
	rfc1035Regexp = regexp.MustCompile("^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$")

	// ErrNameNotRFC1035Compatible appears when some of the provided names are not RFC1035 compatible.
	ErrNameNotRFC1035Compatible = func(fieldName string) error {
		return fmt.Errorf(`'%s' is not RFC 1035 compatible. The name should contain only lowercase alphanumeric characters or '-', start with an alphabetic character, end with an alphanumeric character`, //nolint:lll
			fieldName,
		)
	}

	// ErrNameTooLong when the given fieldName is too long.
	ErrNameTooLong = func(fieldName string) error {
		return fmt.Errorf("'%s' can be at most 22 characters long", fieldName)
	}
)

// ValidateRFC1035 names to be RFC-1035 compatible https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#rfc-1035-label-names
func ValidateRFC1035(s, name string) error {
	if !rfc1035Regexp.MatchString(s) {
		return ErrNameNotRFC1035Compatible(name)
	}

	return nil
}

// ValidateEverestResourceName names to be RFC-1035 compatible https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#rfc-1035-label-names.
// It has an additional limitation at most 22 characters.
func ValidateEverestResourceName(s, name string) error {
	if len(s) > maxNameLength {
		return ErrNameTooLong(name)
	}

	return ValidateRFC1035(s, name)
}

// ValidateURL checks if the given string is a valid URL.
func ValidateURL(urlStr string) bool {
	_, err := url.ParseRequestURI(urlStr)
	return err == nil
}

// ValidateDNSName checks if the given string is a valid DNS name.
func ValidateDNSName(dnsName string) []string {
	return validation.IsDNS1123Subdomain(dnsName)
}

// IsBase64Encoded checks if the given string is a valid base64 encoded string.
func IsBase64Encoded(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return len(s)%4 == 0 && len(s) > 0 && err == nil
}
