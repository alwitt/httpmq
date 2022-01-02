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

package apis

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/alwitt/httpmq/common"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// ErrorDetail is the response detail in case of error
type ErrorDetail struct {
	// Code is the response code
	Code int `json:"code" validate:"required"`
	// Msg is an optional descriptive message
	Msg *string `json:"message,omitempty"`
}

// StandardResponse standard REST API response
type StandardResponse struct {
	// Success indicates whether the request was successful
	Success bool `json:"success" validate:"required"`
	// Error are details in case of errors
	Error *ErrorDetail `json:"error,omitempty"`
}

// getStdRESTSuccessMsg defines a standard success message
func getStdRESTSuccessMsg() StandardResponse {
	return StandardResponse{Success: true}
}

// getStdRESTErrorMsg defines a standard error message
func getStdRESTErrorMsg(code int, message *string) StandardResponse {
	return StandardResponse{
		Success: false, Error: &ErrorDetail{Code: code, Msg: message},
	}
}

// writeRESTResponse writes a REST response
func writeRESTResponse(
	w http.ResponseWriter, r *http.Request, respCode int, resp interface{},
) error {
	w.Header().Set("content-type", "application/json")
	if r.Context().Value(common.RequestParam{}) != nil {
		v, ok := r.Context().Value(common.RequestParam{}).(common.RequestParam)
		if ok {
			w.Header().Add("Httpmq-Request-ID", v.ID)
		}
	}
	w.WriteHeader(respCode)
	t, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
	if _, err = w.Write(t); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
	return nil
}

// ========================================================================================

// MethodHandlers DICT of method-endpoint handler
type MethodHandlers map[string]http.HandlerFunc

// RegisterPathPrefix registers new method handler for a path prefix
func RegisterPathPrefix(
	parentRouter *mux.Router, pathPrefix string, methodHandlers MethodHandlers,
) *mux.Router {
	router := parentRouter.PathPrefix(pathPrefix).Subrouter()
	for method, handler := range methodHandlers {
		router.Methods(method).Path("").HandlerFunc(handler)
	}
	return router
}

// ========================================================================================

// APIRestHandler base REST handler
type APIRestHandler struct {
	common.Component
	startOfRequestLog, endOfRequestLog string
	offLimitHeadersForLog              map[string]bool
}

// addRequestMetaToLog helper function for adding request metadata to a log message
func (h APIRestHandler) addRequestMetaToLog(metadata log.Fields, r *http.Request) {
	for headerField, headerValues := range r.Header {
		if _, present := h.offLimitHeadersForLog[headerField]; !present {
			metadata[headerField] = headerValues
		}
	}
	metadata["host"] = r.Host
	metadata["referer"] = r.Referer()
}

// reply helper function for writing responses
func (h APIRestHandler) reply(
	w http.ResponseWriter, respCode int, resp interface{}, restCall string, r *http.Request,
) {
	localLogTags, _ := common.UpdateLogTags(r.Context(), h.LogTags)
	if err := writeRESTResponse(w, r, respCode, &resp); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Failed to write REST response for %s", restCall,
		)
		return
	}
	// Produce a final log logging all aspects of the request
	localLogTags["response_code"] = respCode
	{
		t, _ := json.Marshal(&resp)
		localLogTags["response_size"] = len(t)
	}
	localLogTags["response_timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
	// Request params
	h.addRequestMetaToLog(localLogTags, r)
	log.WithFields(localLogTags).Warn(h.endOfRequestLog)
}

// Write logging support
func (h APIRestHandler) Write(p []byte) (n int, err error) {
	log.WithFields(h.LogTags).Infof("%s", p)
	return len(p), nil
}

// attachRequestID middleware function to attach a request ID to a API request
func (h APIRestHandler) attachRequestID(next http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		// use provided request id from incoming request if any
		reqID := r.Header.Get("Httpmq-Request-ID")
		if reqID == "" {
			// or use some generated string
			reqID = uuid.New().String()
		}
		// Construct new request param tracking
		params := common.RequestParam{
			ID: reqID, Method: r.Method, URI: r.URL.String(), Timestamp: time.Now(),
		}
		ctx := context.WithValue(r.Context(), common.RequestParam{}, params)
		localLogTags, _ := common.UpdateLogTags(ctx, h.LogTags)
		h.addRequestMetaToLog(localLogTags, r)
		log.WithFields(localLogTags).Warn(h.startOfRequestLog)

		next(rw, r.WithContext(ctx))
	}
}
