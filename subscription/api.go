package subscription

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/apex/log"
	"github.com/gorilla/mux"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/rest"
)

const (
	ErrorInvalidRequest = iota
	ErrorSessionRegFailed
)

// ========================================================================================
// APIHandlerSubscriptionController REST API handler for subscription controller
type APIHandlerSubscriptionController struct {
	common.Component
	controller SubscriptionRecorder
}

// DefineSubscriptionRESTController define
func DefineSubscriptionRESTController(
	controller SubscriptionRecorder,
) APIHandlerSubscriptionController {
	logTags := log.Fields{
		"module": "subscription", "component": "rest-api",
	}
	return APIHandlerSubscriptionController{
		Component:  common.Component{LogTags: logTags},
		controller: controller,
	}
}

func (h *APIHandlerSubscriptionController) reply(
	w http.ResponseWriter, respCode int, resp interface{}, restCall string,
) {
	if err := rest.WriteRESTResponse(w, respCode, &resp); err != nil {
		log.WithError(err).WithFields(h.LogTags).Errorf(
			"Failed to write REST response for %s", restCall,
		)
	}
}

func (h *APIHandlerSubscriptionController) readIDTuple(r *http.Request) (
	string, string, time.Time, error,
) {
	vars := mux.Vars(r)
	nodeID, ok := vars["nodeID"]
	if !ok {
		return "", "", time.Time{}, fmt.Errorf("node ID missing from call")
	}
	clientID, ok := vars["clientID"]
	if !ok {
		return "", "", time.Time{}, fmt.Errorf("client ID missing from call")
	}
	timestampAsString := r.URL.Query().Get("timestamp")
	unixTimeStamp, err := strconv.ParseInt(timestampAsString, 10, 64)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("unable to parse timestamp %s", timestampAsString)
	}
	timestamp := time.Unix(unixTimeStamp, 0)
	return nodeID, clientID, timestamp, nil
}

// ----------------------------------------------------------------------------------------

// NewClientSession godoc
// @Summary Register new client session
// @Description Register new client subscription session
// @tags subscription,post
// @Produce json
// @Param nodeID path string true "node ID"
// @Param clientID path string true "client ID"
// @Param timestamp query uint64 true "timestamp"
// @Success 200 {object} rest.StandardResponse "success"
// @Failure 400 {object} rest.StandardResponse "input error"
// @Failure 403 {string} string "auth error"
// @Failure 404 {string} string "not found"
// @Failure 500 {object} rest.StandardResponse "error"
// @Router /node/{nodeID}/client/{clientID} [post]
func (h *APIHandlerSubscriptionController) NewClientSession(w http.ResponseWriter, r *http.Request) {
	nodeID, clientID, timestamp, err := h.readIDTuple(r)
	if err != nil {
		msg := fmt.Sprintf("bad request: %s", err)
		response := rest.GetStdRESTErrorMsg(ErrorInvalidRequest, &msg)
		h.reply(w, http.StatusBadRequest, &response, "POST /node/{nodeID}/client/{clientID}")
		return
	}

	// Register the client subscription session
	_, err = h.controller.LogClientSession(clientID, nodeID, timestamp)
	if err != nil {
		msg := fmt.Sprintf("Failed to register new client session: %s", err)
		response := rest.GetStdRESTErrorMsg(ErrorSessionRegFailed, &msg)
		h.reply(
			w, http.StatusInternalServerError, &response, "POST /node/{nodeID}/client/{clientID}",
		)
		return
	}

	response := rest.GetStdRESTSuccessMsg()
	h.reply(w, http.StatusAccepted, &response, "POST /node/{nodeID}/client/{clientID}")
}

// NewClientSessionHandler Wrapper around NewClientSession
func (h *APIHandlerSubscriptionController) NewClientSessionHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.NewClientSession(w, r)
	}
}

// ----------------------------------------------------------------------------------------

// RenewClientSession godoc
// @Summary Renew client session
// @Description Renew client subscription session
// @tags subscription,put
// @Produce json
// @Param nodeID path string true "node ID"
// @Param clientID path string true "client ID"
// @Param timestamp query uint64 true "timestamp"
// @Success 200 {object} rest.StandardResponse "success"
// @Failure 400 {object} rest.StandardResponse "input error"
// @Failure 403 {string} string "auth error"
// @Failure 404 {string} string "not found"
// @Failure 500 {object} rest.StandardResponse "error"
// @Router /node/{nodeID}/client/{clientID} [put]
func (h *APIHandlerSubscriptionController) RenewClientSession(
	w http.ResponseWriter, r *http.Request,
) {
	nodeID, clientID, timestamp, err := h.readIDTuple(r)
	if err != nil {
		msg := fmt.Sprintf("bad request: %s", err)
		response := rest.GetStdRESTErrorMsg(ErrorInvalidRequest, &msg)
		h.reply(w, http.StatusBadRequest, &response, "PUT /node/{nodeID}/client/{clientID}")
		return
	}

	// Refresh the client subscription session
	err = h.controller.RefreshClientSession(clientID, nodeID, timestamp)
	if err != nil {
		msg := fmt.Sprintf("Failed to refresh client session: %s", err)
		response := rest.GetStdRESTErrorMsg(ErrorSessionRegFailed, &msg)
		h.reply(
			w, http.StatusInternalServerError, &response, "PUT /node/{nodeID}/client/{clientID}",
		)
		return
	}

	response := rest.GetStdRESTSuccessMsg()
	h.reply(w, http.StatusAccepted, &response, "PUT /node/{nodeID}/client/{clientID}")
}

// RenewClientSessionHandler Wrapper around RenewClientSession
func (h *APIHandlerSubscriptionController) RenewClientSessionHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.RenewClientSession(w, r)
	}
}

// ----------------------------------------------------------------------------------------

// DeleteClientSession godoc
// @Summary Delete client session
// @Description Delete client subscription session
// @tags subscription,delete
// @Produce json
// @Param nodeID path string true "node ID"
// @Param clientID path string true "client ID"
// @Param timestamp query uint64 true "timestamp"
// @Success 200 {object} rest.StandardResponse "success"
// @Failure 400 {object} rest.StandardResponse "input error"
// @Failure 403 {string} string "auth error"
// @Failure 404 {string} string "not found"
// @Failure 500 {object} rest.StandardResponse "error"
// @Router /node/{nodeID}/client/{clientID} [delete]
func (h *APIHandlerSubscriptionController) DeleteClientSession(
	w http.ResponseWriter, r *http.Request,
) {
	nodeID, clientID, timestamp, err := h.readIDTuple(r)
	if err != nil {
		msg := fmt.Sprintf("bad request: %s", err)
		response := rest.GetStdRESTErrorMsg(ErrorInvalidRequest, &msg)
		h.reply(w, http.StatusBadRequest, &response, "DELETE /node/{nodeID}/client/{clientID}")
		return
	}

	// Delete client subscription session
	err = h.controller.ClearClientSession(clientID, nodeID, timestamp)
	if err != nil {
		msg := fmt.Sprintf("Failed to delete client session: %s", err)
		response := rest.GetStdRESTErrorMsg(ErrorSessionRegFailed, &msg)
		h.reply(
			w, http.StatusInternalServerError, &response, "DELETE /node/{nodeID}/client/{clientID}",
		)
		return
	}

	response := rest.GetStdRESTSuccessMsg()
	h.reply(w, http.StatusAccepted, &response, "DELETE /node/{nodeID}/client/{clientID}")
}

// DeleteClientSessionHandler Wrapper around DeleteClientSession
func (h *APIHandlerSubscriptionController) DeleteClientSessionHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.DeleteClientSession(w, r)
	}
}

// ----------------------------------------------------------------------------------------

// Alive godoc
// @Summary For liveness check
// @Description For liveness check
// @tags subscription,util,get
// @Produce html
// @Success 200 {string} string "success"
// @Failure 400 {string} string "error"
// @Failure 404 {string} string "error"
// @Failure 500 {string} string "error"
// @Router /alive [get]
func (h *APIHandlerSubscriptionController) Alive(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

// AliveHandler Wrapper around Alive
func (h *APIHandlerSubscriptionController) AliveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.Alive(w, r)
	}
}

// ----------------------------------------------------------------------------------------

// Ready godoc
// @Summary For readiness check
// @Description For readiness check
// @tags subscription,util,get
// @Produce html
// @Success 200 {string} string "success"
// @Failure 400 {string} string "error"
// @Failure 404 {string} string "error"
// @Failure 500 {string} string "error"
// @Router /ready [get]
func (h *APIHandlerSubscriptionController) Ready(w http.ResponseWriter, r *http.Request) {
	if h.controller.ReadySessionRecords() == nil {
		w.WriteHeader(200)
	} else {
		w.WriteHeader(404)
	}
}

// ReadyHandler Wrapper around Alive
func (h *APIHandlerSubscriptionController) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.Ready(w, r)
	}
}
