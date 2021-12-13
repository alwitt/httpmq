package common

import (
	"fmt"

	"github.com/apex/log"
)

// RequestParam is a helper object for logging a request's parameters into its context
type RequestParam struct {
	// ID is the request ID
	ID string `json:"id"`
	// Method is the request method: DELETE, POST, PUT, GET, etc.
	Method string `json:"method" `
	// URI is the request URI
	URI string `json:"uri"`
}

// UpdateLogTags updates Apex log.Fields map with values the requests's parameters
func (i *RequestParam) UpdateLogTags(tags log.Fields) {
	tags["request_id"] = i.ID
	tags["request_method"] = i.Method
	tags["request_uri"] = fmt.Sprintf("'%s'", i.URI)
}
