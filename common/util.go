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
	"context"
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
)

// Component is the base structure for all components
type Component struct {
	// LogTags the Apex logging message metadata tags
	LogTags log.Fields
}

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

// UpdateLogTags add additional fields to the existing log tags with information from Context
func UpdateLogTags(ctxt context.Context, original log.Fields) (log.Fields, error) {
	newLogTags := log.Fields{}
	if err := DeepCopy(&original, &newLogTags); err != nil {
		log.WithError(err).WithFields(original).Errorf("Failed to deep-copy logtags")
		return original, err
	}
	if ctxt.Value(RequestParam{}) != nil {
		v, ok := ctxt.Value(RequestParam{}).(RequestParam)
		if ok {
			v.UpdateLogTags(newLogTags)
		}
	}
	return newLogTags, nil
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
