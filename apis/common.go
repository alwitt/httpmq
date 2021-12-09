package apis

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"gitlab.com/project-nan/httpmq/common"
)

// ErrorDetail in case of REST error, the response
type ErrorDetail struct {
	Code int     `json:"code"`
	Msg  *string `json:"message,omitempty"`
}

// StandardResponse standard REST API response
type StandardResponse struct {
	Success bool         `json:"success"`
	Error   *ErrorDetail `json:"error,omitempty"`
}

// getStdRESTSuccessMsg define a standard success message
func getStdRESTSuccessMsg() StandardResponse {
	return StandardResponse{Success: true}
}

// getStdRESTErrorMsg define a standard error message
func getStdRESTErrorMsg(code int, message *string) StandardResponse {
	return StandardResponse{
		Success: false, Error: &ErrorDetail{Code: code, Msg: message},
	}
}

// writeRESTResponse write a REST response
func writeRESTResponse(w http.ResponseWriter, respCode int, resp interface{}) error {
	w.WriteHeader(respCode)
	t, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(500)
		return err
	}
	if _, err = w.Write(t); err != nil {
		w.WriteHeader(500)
		return err
	}
	return nil
}

// ========================================================================================
// MethodHandlers DICT of method-endpoint handler
type MethodHandlers map[string]http.HandlerFunc

// RegisterPathPrefix Register new method handler for an end-point
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
}

// reply helper function for writing responses
func (h APIRestHandler) reply(
	w http.ResponseWriter, respCode int, resp interface{}, restCall string,
) {
	if err := writeRESTResponse(w, respCode, &resp); err != nil {
		log.WithError(err).WithFields(h.LogTags).Errorf(
			"Failed to write REST response for %s", restCall,
		)
	}
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
		log.WithFields(h.LogTags).Debugf("New request ID %s", reqID)
		ctx := context.WithValue(
			r.Context(), common.RequestParam{}, common.RequestParam{
				ID: reqID, Method: r.Method, URI: r.URL.String(),
			},
		)

		next(rw, r.WithContext(ctx))
	}
}
