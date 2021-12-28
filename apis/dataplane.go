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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/core"
	"github.com/alwitt/httpmq/dataplane"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
)

// APIRestJetStreamDataplaneHandler REST handler for JetStream dataplane
type APIRestJetStreamDataplaneHandler struct {
	APIRestHandler
	natsClient   *core.NatsClient
	publisher    dataplane.JetStreamPublisher
	ackBroadcast dataplane.JetStreamACKBroadcaster
	validate     *validator.Validate
	baseContext  context.Context
	wg           *sync.WaitGroup
}

// GetAPIRestJetStreamDataplaneHandler define APIRestJetStreamDataplaneHandler
func GetAPIRestJetStreamDataplaneHandler(
	baseContext context.Context,
	client *core.NatsClient,
	httpConfig *common.HTTPConfig,
	runTimePublisher dataplane.JetStreamPublisher,
	ackBroadcast dataplane.JetStreamACKBroadcaster,
	wg *sync.WaitGroup,
) (APIRestJetStreamDataplaneHandler, error) {
	logTags := log.Fields{
		"module":    "rest",
		"component": "jetstream-dataplane",
	}
	offLimitHeaders := make(map[string]bool)
	for _, header := range httpConfig.Logging.DoNotLogHeaders {
		offLimitHeaders[header] = true
	}
	return APIRestJetStreamDataplaneHandler{
		APIRestHandler: APIRestHandler{
			Component:             common.Component{LogTags: logTags},
			startOfRequestLog:     httpConfig.Logging.StartOfRequestMessage,
			endOfRequestLog:       httpConfig.Logging.EndOfRequestMessage,
			offLimitHeadersForLog: offLimitHeaders,
		},
		natsClient:   client,
		publisher:    runTimePublisher,
		ackBroadcast: ackBroadcast,
		validate:     validator.New(),
		baseContext:  baseContext,
		wg:           wg,
	}, nil
}

// =======================================================================
// Message publish

// -----------------------------------------------------------------------

// PublishMessage godoc
// @Summary Publish a message
// @Description Publish a Base64 encoded message to a JetStream subject
// @tags Dataplane
// @Accept plain
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param subjectName path string true "JetStream subject to publish under"
// @Param message body string true "Message to publish in Base64 encoding"
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/data/subject/{subjectName} [post]
func (h APIRestJetStreamDataplaneHandler) PublishMessage(w http.ResponseWriter, r *http.Request) {
	restCall := "POST /v1/data/subject/{subjectName}"

	localLogTags, err := common.UpdateLogTags(r.Context(), h.LogTags)
	if err != nil {
		msg := "Prep failed"
		log.WithError(err).WithFields(h.LogTags).Error("Failed to update logtags")
		h.reply(
			w,
			http.StatusInternalServerError,
			getStdRESTErrorMsg(http.StatusInternalServerError, &msg),
			restCall,
			r,
		)
		return
	}

	vars := mux.Vars(r)
	subjectName, ok := vars["subjectName"]
	if !ok {
		msg := "No subject name provided"
		log.WithFields(localLogTags).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}
	if err := common.ValidateSubjectName(subjectName); err != nil {
		msg := "Invalid subject string"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	// Decode the message
	var decodedMsg []byte
	{
		decoder := base64.NewDecoder(base64.StdEncoding, r.Body)
		buf := new(bytes.Buffer)
		decodeNum, err := io.Copy(buf, decoder)
		if err != nil {
			msg := "Failed to base64 decode body"
			log.WithError(err).WithFields(localLogTags).Errorf(msg)
			h.reply(
				w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r,
			)
			return
		}
		if decodeNum == 0 {
			msg := "Base64 decode resulted in empty body"
			log.WithFields(localLogTags).Errorf(msg)
			h.reply(
				w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r,
			)
			return
		}
		decodedMsg = buf.Bytes()
	}

	// Publish the message
	if err := h.publisher.Publish(r.Context(), subjectName, decodedMsg); err != nil {
		msg := fmt.Sprintf("Unable to publish message to %s", subjectName)
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
		return
	}

	h.reply(w, http.StatusOK, getStdRESTSuccessMsg(), restCall, r)
}

// PublishMessageHandler Wrapper around PublishMessage
func (h APIRestJetStreamDataplaneHandler) PublishMessageHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.PublishMessage(w, r)
	})
}

// =======================================================================
// Message subscription

// -----------------------------------------------------------------------

// ReceiveMsgACK godoc
// @Summary Handle ACK for message
// @Description Process JetStream message ACK for a stream / consumer
// @tags Dataplane
// @Accept json
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Param consumerName path string true "JetStream consumer name"
// @Param sequenceNum body dataplane.AckSeqNum true "Message message sequence numbers"
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/data/stream/{streamName}/consumer/{consumerName}/ack [post]
func (h APIRestJetStreamDataplaneHandler) ReceiveMsgACK(w http.ResponseWriter, r *http.Request) {
	restCall := "POST /v1/data/stream/{streamName}/consumer/{consumerName}/ack"

	localLogTags, err := common.UpdateLogTags(r.Context(), h.LogTags)
	if err != nil {
		msg := "Prep failed"
		log.WithError(err).WithFields(h.LogTags).Error("Failed to update logtags")
		h.reply(
			w,
			http.StatusInternalServerError,
			getStdRESTErrorMsg(http.StatusInternalServerError, &msg),
			restCall,
			r,
		)
		return
	}

	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTags).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}
	consumerName, ok := vars["consumerName"]
	if !ok {
		msg := "No consumer name provided"
		log.WithFields(localLogTags).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}
	if err := common.ValidateTopLevelEntityName(consumerName, h.validate); err != nil {
		msg := "Invalid consumer name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	var sequence dataplane.AckSeqNum
	if err := json.NewDecoder(r.Body).Decode(&sequence); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	// Validate input
	if err := h.validate.Struct(&sequence); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	ackInfo := dataplane.AckIndication{
		Stream: streamName, Consumer: consumerName, SeqNum: dataplane.AckSeqNum{
			Stream: sequence.Stream, Consumer: sequence.Consumer,
		},
	}

	// Broadcast the ACK
	if err := h.ackBroadcast.BroadcastACK(r.Context(), ackInfo); err != nil {
		msg := fmt.Sprintf("Failed to broadcast ACK %s", ackInfo.String())
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
		return
	}

	h.reply(w, http.StatusOK, getStdRESTSuccessMsg(), restCall, r)
}

// ReceiveMsgACKHandler Wrapper around ReceiveMsgACK
func (h APIRestJetStreamDataplaneHandler) ReceiveMsgACKHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.ReceiveMsgACK(w, r)
	})
}

// -----------------------------------------------------------------------

// APIRestRespDataMessage wrapper object for one message from a stream
type APIRestRespDataMessage struct {
	StandardResponse
	dataplane.MsgToDeliver
}

// PushSubscribe godoc
// @Summary Establish a pull subscribe session
// @Description Establish a JetStream pull subscribe session for a client. This is a long lived
// server send event stream. The stream will close on client disconnect, server shutdown, or
// server internal error.
// @tags Dataplane
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Param consumerName path string true "JetStream consumer name"
// @Param subject_name query string true "JetStream subject to subscribe to"
// @Param max_msg_inflight query integer false "Max number of inflight messages (DEFAULT: 1)"
// @Param delivery_group query string false "Needed if consumer uses delivery groups"
// @Success 200 {object} APIRestRespDataMessage "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/data/stream/{streamName}/consumer/{consumerName} [get]
func (h APIRestJetStreamDataplaneHandler) PushSubscribe(w http.ResponseWriter, r *http.Request) {
	restCall := "GET /v1/data/stream/{streamName}/consumer/{consumerName}"

	localLogTagsInitial, err := common.UpdateLogTags(r.Context(), h.LogTags)
	if err != nil {
		msg := "Prep failed"
		log.WithError(err).WithFields(h.LogTags).Error("Failed to update logtags")
		h.reply(
			w,
			http.StatusInternalServerError,
			getStdRESTErrorMsg(http.StatusInternalServerError, &msg),
			restCall,
			r,
		)
		return
	}

	// --------------------------------------------------------------------------
	// Read operation parameters
	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTagsInitial).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTagsInitial).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}
	consumerName, ok := vars["consumerName"]
	if !ok {
		msg := "No consumer name provided"
		log.WithFields(localLogTagsInitial).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}
	if err := common.ValidateTopLevelEntityName(consumerName, h.validate); err != nil {
		msg := "Invalid consumer name"
		log.WithError(err).WithFields(localLogTagsInitial).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	// Read query parameters
	var subjectName string
	var deliveryGroup *string
	maxInflightMsg := 1
	deliveryGroup = nil
	requestQueries := r.URL.Query()
	// Read the subject
	{
		t, ok := requestQueries["subject_name"]
		if !ok || len(t) != 1 {
			msg := "Missing subscribe subject / Multiple subjects"
			log.WithFields(localLogTagsInitial).Errorf(msg)
			h.reply(
				w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r,
			)
			return
		}
		subjectName = t[0]
	}
	// Read the max inflight messages
	{
		t, ok := requestQueries["max_msg_inflight"]
		if ok {
			if len(t) != 1 {
				msg := "Multiple max_msg_inflight"
				log.WithFields(localLogTagsInitial).Errorf(msg)
				h.reply(
					w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r,
				)
				return
			}
			p, err := strconv.Atoi(t[0])
			if err != nil {
				msg := "Unable to parse max_msg_inflight"
				log.WithError(err).WithFields(localLogTagsInitial).Errorf(msg)
				h.reply(
					w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r,
				)
				return
			}
			maxInflightMsg = p
		}
	}
	// Read the delivery group
	{
		t, ok := requestQueries["delivery_group"]
		if ok {
			if len(t) != 1 {
				msg := "Multiple delivery groups"
				log.WithFields(localLogTagsInitial).Errorf(msg)
				h.reply(
					w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r,
				)
				return
			}
			deliveryGroup = new(string)
			*deliveryGroup = t[0]
		}
	}

	// --------------------------------------------------------------------------
	// Start operation

	// Define custom log tags for this instance
	logTags := log.Fields{
		"module":         "rest",
		"component":      "jetstream-dataplane",
		"instance":       "push-subscribe",
		"stream":         streamName,
		"subject":        subjectName,
		"consumer":       consumerName,
		"delivery_group": deliveryGroup,
	}
	if r.Context().Value(common.RequestParam{}) != nil {
		v, ok := r.Context().Value(common.RequestParam{}).(common.RequestParam)
		if ok {
			v.UpdateLogTags(logTags)
		}
	}

	// Create stream flusher
	writeFlusher, ok := w.(http.Flusher)
	if !ok {
		msg := "Streaming not supported"
		log.WithFields(logTags).Errorf(msg)
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
		return
	}

	// Create the dispatcher
	runtimeCtxt, cancel := context.WithCancel(r.Context())
	defer cancel()
	dispatcher, err := dataplane.GetPushMessageDispatcher(
		runtimeCtxt,
		h.natsClient,
		streamName,
		subjectName,
		consumerName,
		deliveryGroup,
		maxInflightMsg,
		h.wg,
	)
	if err != nil {
		msg := "Unable to define dispatcher"
		log.WithError(err).WithFields(logTags).Errorf(msg)
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
		return
	}

	// Handle error which occur when interacting with JetStream
	internalError := make(chan error, maxInflightMsg*2)
	errorHandler := func(err error) {
		internalError <- err
	}

	// Handle messages read from JetStream
	msgBuffer := make(chan *nats.Msg, maxInflightMsg*2)
	msgHandler := func(ctxt context.Context, msg *nats.Msg) error {
		select {
		case msgBuffer <- msg:
			return nil
		case <-ctxt.Done():
			return ctxt.Err()
		case <-runtimeCtxt.Done():
			return runtimeCtxt.Err()
		}
	}

	// Begin reading from JetStream
	if err := dispatcher.Start(msgHandler, errorHandler); err != nil {
		msg := "Unable to start dispatcher"
		log.WithError(err).WithFields(logTags).Errorf(msg)
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
		return
	}

	// Process events
	complete := false
	onError := func(err error, msg string) {
		cancel()
		complete = true
		log.WithError(err).WithFields(logTags).Errorf(msg)
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
	}
	for !complete {
		select {
		case <-h.baseContext.Done():
			// Server stopping
			complete = true
			log.WithFields(logTags).Info("Terminating PUSH subscription on server stop")
			msg := "Server stopping"
			h.reply(
				w, http.StatusInternalServerError, getStdRESTErrorMsg(
					http.StatusInternalServerError, &msg,
				), restCall, r,
			)
		case <-r.Context().Done():
			// Request closed
			complete = true
			log.WithFields(logTags).Info("Terminating PUSH subscription on request end")
			resp := APIRestRespDataMessage{
				StandardResponse: getStdRESTSuccessMsg(),
			}
			h.reply(w, http.StatusOK, resp, restCall, r)
		case err, ok := <-internalError:
			// Internal system error
			if ok {
				onError(err, "Error occurred interacting with JetStream")
			} else {
				err := fmt.Errorf("jetstream interaction internal error channel read fail")
				onError(err, "Internal error channel read fail")
			}
		case msg, ok := <-msgBuffer:
			// Send out a new message
			if ok && msg != nil {
				// Convert to transmission format
				converted, err := dataplane.ConvertJSMessageDeliver(subjectName, msg)
				if err != nil {
					onError(err, "Failed to convert message for transmission")
					break
				}
				resp := APIRestRespDataMessage{
					StandardResponse: StandardResponse{Success: true},
					MsgToDeliver:     converted,
				}
				// Serialize as JSON
				serialize, err := json.Marshal(&resp)
				if err != nil {
					onError(err, "Failed to serialize message for transmission")
					break
				}
				// Send and flush
				written, err := fmt.Fprintf(w, "%s\n", serialize)
				writeFlusher.Flush()
				if err != nil {
					onError(err, "Failed to transmit message")
					break
				}
				log.WithFields(logTags).Debugf("Written %dB", written)
			} else {
				err := fmt.Errorf("jetstream message channel read fail")
				onError(err, "Message channel read fail")
			}
		}
	}
	// On final flush
	writeFlusher.Flush()
}

// PushSubscribeHandler Wrapper around PushSubscribe
func (h APIRestJetStreamDataplaneHandler) PushSubscribeHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.PushSubscribe(w, r)
	})
}

// =======================================================================
// Health Checks

// -----------------------------------------------------------------------

// Alive godoc
// @Summary For dataplane REST API liveness check
// @Description Will return success to indicate dataplane REST API module is live
// @tags Dataplane
// @Produce json
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {string} string "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/data/alive [get]
func (h APIRestJetStreamDataplaneHandler) Alive(w http.ResponseWriter, r *http.Request) {
	restCall := "GET /v1/data/alive"
	h.reply(w, http.StatusOK, getStdRESTSuccessMsg(), restCall, r)
}

// AliveHandler Wrapper around Alive
func (h APIRestJetStreamDataplaneHandler) AliveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.Alive(w, r)
	}
}

// -----------------------------------------------------------------------

// Ready godoc
// @Summary For dataplane REST API readiness check
// @Description Will return success if dataplane REST API module is ready for use
// @tags Dataplane
// @Produce json
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {string} string "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/data/ready [get]
func (h APIRestJetStreamDataplaneHandler) Ready(w http.ResponseWriter, r *http.Request) {
	restCall := "GET /v1/data/ready"
	msg := "not ready"
	if h.natsClient.NATs().Status() == nats.CONNECTED {
		h.reply(w, http.StatusOK, getStdRESTSuccessMsg(), restCall, r)
	} else {
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
	}
}

// ReadyHandler Wrapper around Alive
func (h APIRestJetStreamDataplaneHandler) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.Ready(w, r)
	}
}
