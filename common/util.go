// Copyright 2021-2022 The httpmq Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
)

// DeepCopy is helper function for performing deep-copy
//
// USE ONLY WHEN ABSOLUTELY NEEDED
func DeepCopy(src, dst interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

// ValidateTopLevelEntityName performs validation of stream and consumer names.
//
// A valid name must
//  * not include illegal characters
func ValidateTopLevelEntityName(name string, validate *validator.Validate) error {
	type nameWrapper struct {
		Name string `validate:"alphanum|uuid"`
	}
	t := nameWrapper{Name: name}

	return validate.Struct(&t)
}

// ValidateSubjectName performs validation of subject names
//
// A valid name must
//  * not include illegal characters
func ValidateSubjectName(subject string) error {
	const illegalChars = " \t>\n\r`~!@#$%^&()_+=[]{}\\|;:'\",<>/?"

	if strings.ContainsAny(subject, illegalChars) {
		return fmt.Errorf("subject '%s' contains illegal characters", subject)
	}
	return nil
}

// GetStrPtr is a helper function to convert a string to *string
func GetStrPtr(s string) *string {
	return &s
}
