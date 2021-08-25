package rest

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
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

// GetStdRESTSuccessMsg define a standard success message
func GetStdRESTSuccessMsg() StandardResponse {
	return StandardResponse{Success: true}
}

// GetStdRESTErrorMsg define a standard error message
func GetStdRESTErrorMsg(code int, message *string) StandardResponse {
	return StandardResponse{
		Success: false, Error: &ErrorDetail{Code: code, Msg: message},
	}
}

// WriteRESTResponse write a REST response
func WriteRESTResponse(w http.ResponseWriter, respCode int, resp interface{}) error {
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
