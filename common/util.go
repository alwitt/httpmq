package common

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/apex/log"
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
func UpdateLogTags(original log.Fields, ctxt context.Context) (log.Fields, error) {
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
