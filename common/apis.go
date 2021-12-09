package common

import (
	"fmt"

	"github.com/apex/log"
)

// RequestParam used to add a request parameters into its context
type RequestParam struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	URI    string `json:"uri"`
}

// UpdateLogTags update Apex log.Fields map with values in this
func (i *RequestParam) UpdateLogTags(tags log.Fields) {
	tags["request_id"] = i.ID
	tags["request_method"] = i.Method
	tags["request_uri"] = fmt.Sprintf("'%s'", i.URI)
}
