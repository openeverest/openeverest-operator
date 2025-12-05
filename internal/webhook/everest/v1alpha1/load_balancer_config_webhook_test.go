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

package v1alpha1

import "testing"

var validKeys = []string{
	"my-name",
	"example.com/my-name",
	"sub.domain.example.com/my-name",
	"sub-domain.example.com/my_name-1.0",
	"a/b",
	"a.b/c",
	"very-long-subdomain.another-very-long-subdomain.example.com/short-name",
}

var invalidKeys = []string{
	"example.com/",         // No name
	"/my-name",             // empty prefix is invalid according to the prefix requirements
	"example.com//my-name", // double slash
	"example..com/my-name", // consecutive dots in subdomain
	"example.com/MyName",   // invalid char
	"example.com/my-name_", // invalid char
	"example.com/my-name-", // ends with invalid char
	"example.com/-my-name", // starts with invalid char
	"toooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooolong.com/my-name", // subdomain too long
	"example.com/tooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooolong",  // name too long
	".example.com/my-name", // subdomain starts with invalid char
	"example.com./my-name", // subdomain ends with invalid char
	"-example.com/my-name", // subdomain starts with invalid char
	"example.com-/my-name", // subdomain ends with invalid char
}

func TestAnnotationKeyValidation(t *testing.T) {
	t.Parallel()

	for _, key := range validKeys {
		if !annotationKeyRegex.MatchString(key) {
			t.Errorf("Key '%s' should be VALID, but it's INVALID", key)
		}
	}

	for _, key := range invalidKeys {
		if annotationKeyRegex.MatchString(key) {
			t.Errorf("Key '%s' should be INVALID, but it's VALID", key)
		}
	}
}
