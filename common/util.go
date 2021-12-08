package common

import (
	"bytes"
	"encoding/gob"

	"github.com/apex/log"
)

// Component base structure for a Component
type Component struct {
	LogTags log.Fields
}

// DeepCopy helper function for performing deep-copy
//
// USE ONLY WHEN ABSOLUTELY NEEDED
func DeepCopy(src, dst interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}
