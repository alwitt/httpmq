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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/management"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
)

// APIRestJetStreamManagementHandler REST handler for JetStream management
type APIRestJetStreamManagementHandler struct {
	goutils.RestAPIHandler
	core     management.JetStreamController
	validate *validator.Validate
}

// GetAPIRestJetStreamManagementHandler define APIRestJetStreamManagementHandler
func GetAPIRestJetStreamManagementHandler(
	core management.JetStreamController,
	httpConfig *common.HTTPConfig,
) (APIRestJetStreamManagementHandler, error) {
	logTags := log.Fields{
		"module":    "rest",
		"component": "jetstream-management",
	}
	offLimitHeaders := make(map[string]bool)
	for _, header := range httpConfig.Logging.DoNotLogHeaders {
		offLimitHeaders[header] = true
	}
	return APIRestJetStreamManagementHandler{
		RestAPIHandler: goutils.RestAPIHandler{
			Component: goutils.Component{
				LogTags: logTags,
				LogTagModifiers: []goutils.LogMetadataModifier{
					goutils.ModifyLogMetadataByRestRequestParam,
				},
			},
			CallRequestIDHeaderField: &httpConfig.Logging.RequestIDHeader,
			DoNotLogHeaders: func() map[string]bool {
				result := map[string]bool{}
				for _, v := range httpConfig.Logging.DoNotLogHeaders {
					result[v] = true
				}
				return result
			}(),
		}, core: core, validate: validator.New(),
	}, nil
}

// APIRestRespStreamConfig adhoc structure for persenting nats.StreamConfig
type APIRestRespStreamConfig struct {
	// Name is the stream name
	Name string `json:"name" validate:"required"`
	// Description is an optional description of the stream
	Description string `json:"description"`
	// Subjects is the list subjects this stream is listening on
	Subjects []string `json:"subjects"`
	// MaxConsumers is the max number of consumers allowed on the stream
	MaxConsumers int `json:"max_consumers" validate:"required"`
	// MaxMsgs is the max number of messages the stream will store.
	//
	// Oldest messages are removed once limit breached.
	MaxMsgs int64 `json:"max_msgs" validate:"required"`
	// MaxBytes is the max number of message bytes the stream will store.
	//
	// Oldest messages are removed once limit breached.
	MaxBytes int64 `json:"max_bytes" validate:"required"`
	// MaxBytes is the max duration (ns) the stream will store a message
	//
	// Messages breaching the limit will be removed.
	MaxAge time.Duration `json:"max_age" swaggertype:"primitive,integer" validate:"required"`
	// MaxMsgsPerSubject is the maximum number of subjects allowed on this stream
	MaxMsgsPerSubject int64 `json:"max_msgs_per_subject" validate:"required"`
	// MaxMsgSize is the max size of a message allowed in this stream
	MaxMsgSize int32 `json:"max_msg_size"`
}

// APIRestRespStreamState adhoc structure for persenting nats.StreamState
type APIRestRespStreamState struct {
	// Msgs is the number of messages in the stream
	Msgs uint64 `json:"messages" validate:"required"`
	// Bytes is the number of message bytes in the stream
	Bytes uint64 `json:"bytes" validate:"required"`
	// FirstSeq is the oldest message sequence number on the stream
	FirstSeq uint64 `json:"first_seq" validate:"required"`
	// FirstTime is the oldest message timestamp on the stream
	FirstTime time.Time `json:"first_ts" validate:"required"`
	// LastSeq is the newest message sequence number on the stream
	LastSeq uint64 `json:"last_seq" validate:"required"`
	// LastTime is the newest message timestamp on the stream
	LastTime time.Time `json:"last_ts" validate:"required"`
	// Consumers number of consumers on the stream
	Consumers int `json:"consumer_count" validate:"required"`
}

// APIRestRespStreamInfo adhoc structure for persenting nats.StreamInfo
type APIRestRespStreamInfo struct {
	// Config is the stream config parameters
	Config APIRestRespStreamConfig `json:"config" validate:"required"`
	// Created is the stream creation timestamp
	Created time.Time `json:"created" validate:"required"`
	// State is the stream current state
	State APIRestRespStreamState `json:"state" validate:"required"`
}

// convertStreamInfo convert *nats.StreamInfo into APIRestRespStreamInfo
func convertStreamInfo(original *nats.StreamInfo) APIRestRespStreamInfo {
	return APIRestRespStreamInfo{
		Config: APIRestRespStreamConfig{
			Name:              original.Config.Name,
			Description:       original.Config.Description,
			Subjects:          original.Config.Subjects,
			MaxConsumers:      original.Config.MaxConsumers,
			MaxMsgs:           original.Config.MaxMsgs,
			MaxBytes:          original.Config.MaxBytes,
			MaxAge:            original.Config.MaxAge,
			MaxMsgsPerSubject: original.Config.MaxMsgsPerSubject,
			MaxMsgSize:        original.Config.MaxMsgSize,
		},
		Created: original.Created,
		State: APIRestRespStreamState{
			Msgs:      original.State.Msgs,
			Bytes:     original.State.Bytes,
			FirstSeq:  original.State.FirstSeq,
			FirstTime: original.State.FirstTime,
			LastSeq:   original.State.LastSeq,
			LastTime:  original.State.LastTime,
			Consumers: original.State.Consumers,
		},
	}
}

// APIRestRespConsumerConfig adhoc structure for persenting nats.ConsumerConfig
type APIRestRespConsumerConfig struct {
	// Description an optional description of the consumer
	Description string `json:"notes"`
	// DeliverSubject subject this consumer is listening on
	DeliverSubject string `json:"deliver_subject"`
	// DeliverGroup is the delivery group if this consumer uses delivery group
	//
	// A consumer using delivery group allows multiple clients to subscribe under the same consumer
	// and group name tuple. For subjects this consumer listens to, the messages will be shared
	// amongst the connected clients.
	DeliverGroup string `json:"deliver_group"`
	// MaxDeliver max number of times a message can be deliveried (including retry) to this consumer
	MaxDeliver int `json:"max_deliver"`
	// AckWait duration (ns) to wait for an ACK for the delivery of a message
	AckWait time.Duration `json:"ack_wait" swaggertype:"primitive,integer" validate:"required"`
	// FilterSubject sets the consumer to filter for subjects matching this NATs subject string
	//
	// See https://docs.nats.io/nats-concepts/subjects
	FilterSubject string `json:"filter_subject"`
	// MaxWaiting NATS JetStream does not clearly document this
	MaxWaiting int `json:"max_waiting"`
	// MaxAckPending controls the max number of un-ACKed messages permitted in-flight
	MaxAckPending int `json:"max_ack_pending"`
}

// APIRestRespSequenceInfo adhoc structure for persenting nats.SequenceInfo
type APIRestRespSequenceInfo struct {
	// Consumer is consumer level sequence number
	Consumer uint64 `json:"consumer_seq" validate:"required"`
	// Stream is stream level sequence number
	Stream uint64 `json:"stream_seq" validate:"required"`
	// Last timestamp when these values updated
	Last *time.Time `json:"last_active,omitempty" validate:"omitempty,optional"`
}

// APIRestRespConsumerInfo adhoc structure for persenting nats.ConsumerInfo
type APIRestRespConsumerInfo struct {
	// Stream is the name of the stream
	Stream string `json:"stream_name" validate:"required"`
	// Name is the name of the consumer
	Name string `json:"name" validate:"required"`
	// Created is when this consumer was defined
	Created time.Time `json:"created" validate:"required"`
	// Config are the consumer config parameters
	Config APIRestRespConsumerConfig `json:"config" validate:"required"`
	// Delivered is the sequence number of the last message delivered
	Delivered APIRestRespSequenceInfo `json:"delivered" validate:"required"`
	// AckFloor is the sequence number of the last received ACKed
	//
	// For messages which failed to be ACKed (retry limit reached), the floor moves up to
	// include these message sequence numbers indicating these messages will not be retried.
	AckFloor APIRestRespSequenceInfo `json:"ack_floor" validate:"required"`
	// NumAckPending is the number of ACK pending / messages in-flight
	NumAckPending int `json:"num_ack_pending" validate:"required"`
	// NumRedelivered is the number of messages redelivered
	NumRedelivered int `json:"num_redelivered" validate:"required"`
	// NumWaiting NATS JetStream does not clearly document this
	NumWaiting int `json:"num_waiting" validate:"required"`
	// NumPending is the number of message to be delivered for this consumer
	NumPending uint64 `json:"num_pending" validate:"required"`
}

// convertConsumerInfo convert *nats.ConsumerInfo into APIRestRespConsumerInfo
func convertConsumerInfo(original *nats.ConsumerInfo) APIRestRespConsumerInfo {
	return APIRestRespConsumerInfo{
		Stream:  original.Stream,
		Name:    original.Name,
		Created: original.Created,
		Config: APIRestRespConsumerConfig{
			Description:    original.Config.Description,
			DeliverSubject: original.Config.DeliverSubject,
			DeliverGroup:   original.Config.DeliverGroup,
			MaxDeliver:     original.Config.MaxDeliver,
			AckWait:        original.Config.AckWait,
			FilterSubject:  original.Config.FilterSubject,
			MaxWaiting:     original.Config.MaxWaiting,
			MaxAckPending:  original.Config.MaxAckPending,
		},
		Delivered: APIRestRespSequenceInfo{
			Consumer: original.Delivered.Consumer,
			Stream:   original.Delivered.Stream,
			Last:     original.Delivered.Last,
		},
		AckFloor: APIRestRespSequenceInfo{
			Consumer: original.AckFloor.Consumer,
			Stream:   original.AckFloor.Stream,
			Last:     original.AckFloor.Last,
		},
		NumAckPending:  original.NumAckPending,
		NumRedelivered: original.NumRedelivered,
		NumWaiting:     original.NumWaiting,
		NumPending:     original.NumPending,
	}
}

// =======================================================================
// Stream related management

// -----------------------------------------------------------------------

// CreateStream godoc
// @Summary Define new stream
// @Description Define new JetStream stream
// @tags Management
// @Accept json
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param setting body management.JSStreamParam true "JetStream stream setting"
// @Success 200 {object} goutils.RestAPIBaseResponse "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream [post]
func (h APIRestJetStreamManagementHandler) CreateStream(w http.ResponseWriter, r *http.Request) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	// Parse the parameters
	var params management.JSStreamParam
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	if err := h.core.CreateStream(r.Context(), params); err != nil {
		msg := "Failed to create new stream"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusInternalServerError
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
		return
	}

	respCode = http.StatusOK
	respBody = h.GetStdRESTSuccessMsg(r.Context())
}

// CreateStreamHandler Wrapper around CreateStream
func (h APIRestJetStreamManagementHandler) CreateStreamHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.CreateStream(w, r)
	}
}

// -----------------------------------------------------------------------

// APIRestRespAllJetStreams response for listing all streams
type APIRestRespAllJetStreams struct {
	goutils.RestAPIBaseResponse
	// Streams the set of stream details mapped against its names
	Streams map[string]APIRestRespStreamInfo `json:"streams"`
}

// GetAllStreams godoc
// @Summary Query for info on all streams
// @Description Query for the details of all streams
// @tags Management
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Success 200 {object} APIRestRespAllJetStreams "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream [get]
func (h APIRestJetStreamManagementHandler) GetAllStreams(w http.ResponseWriter, r *http.Request) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	allInfo := h.core.GetAllStreams(r.Context())
	convertedInfo := make(map[string]APIRestRespStreamInfo)
	for streamName, streamInfo := range allInfo {
		convertedInfo[streamName] = convertStreamInfo(streamInfo)
	}
	resp := APIRestRespAllJetStreams{
		RestAPIBaseResponse: goutils.RestAPIBaseResponse{
			Success: true, RequestID: h.ReadRequestIDFromContext(r.Context()),
		}, Streams: convertedInfo,
	}

	if err := h.WriteRESTResponse(w, http.StatusOK, resp, nil); err != nil {
		log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
	}
}

// GetAllStreamsHandler Wrapper around GetAllStreams
func (h APIRestJetStreamManagementHandler) GetAllStreamsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.GetAllStreams(w, r)
	}
}

// -----------------------------------------------------------------------

// APIRestRespOneJetStream response for listing one stream
type APIRestRespOneJetStream struct {
	goutils.RestAPIBaseResponse
	// Stream the details for this stream
	Stream APIRestRespStreamInfo `json:"stream"`
}

// GetStream godoc
// @Summary Query for info on one stream
// @Description Query for the details of one stream
// @tags Management
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Success 200 {object} APIRestRespOneJetStream "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream/{streamName} [get]
func (h APIRestJetStreamManagementHandler) GetStream(w http.ResponseWriter, r *http.Request) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	streamInfo, err := h.core.GetStream(r.Context(), streamName)
	if err != nil {
		msg := fmt.Sprintf("Unable fetch stream %s info", streamName)
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusInternalServerError
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
		return
	}
	resp := APIRestRespOneJetStream{
		RestAPIBaseResponse: goutils.RestAPIBaseResponse{
			Success: true, RequestID: h.ReadRequestIDFromContext(r.Context()),
		},
		Stream: convertStreamInfo(streamInfo),
	}

	respCode = http.StatusOK
	respBody = resp
}

// GetStreamHandler Wrapper around GetStream
func (h APIRestJetStreamManagementHandler) GetStreamHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.GetStream(w, r)
	}
}

// -----------------------------------------------------------------------

// APIRestReqStreamSubjects subject change parameters
type APIRestReqStreamSubjects struct {
	// Subjects the list of new subject this stream will listen to
	Subjects []string `json:"subjects" validate:"required,min=1"`
}

// ChangeStreamSubjects godoc
// @Summary Change subjects of a stream
// @Description Change the list of subjects of interest for a stream
// @tags Management
// @Accept json
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Param subjects body APIRestReqStreamSubjects true "List of new subjects"
// @Success 200 {object} goutils.RestAPIBaseResponse "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream/{streamName}/subject [put]
func (h APIRestJetStreamManagementHandler) ChangeStreamSubjects(
	w http.ResponseWriter, r *http.Request,
) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	var subjects APIRestReqStreamSubjects
	if err := json.NewDecoder(r.Body).Decode(&subjects); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	if err := h.validate.Struct(&subjects); err != nil {
		msg := "Bad request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	if err := h.core.ChangeStreamSubjects(r.Context(), streamName, subjects.Subjects); err != nil {
		msg := fmt.Sprintf("Failed to change stream %s subjects", streamName)
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusInternalServerError
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
		return
	}

	respCode = http.StatusOK
	respBody = h.GetStdRESTSuccessMsg(r.Context())
}

// ChangeStreamSubjectsHandler Wrapper around ChangeStreamSubjects
func (h APIRestJetStreamManagementHandler) ChangeStreamSubjectsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.ChangeStreamSubjects(w, r)
	}
}

// -----------------------------------------------------------------------

// UpdateStreamLimits godoc
// @Summary Change limits a stream
// @Description Change the data retention limits of a stream
// @tags Management
// @Accept json
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Param limits body management.JSStreamLimits true "New stream limits"
// @Success 200 {object} goutils.RestAPIBaseResponse "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream/{streamName}/limit [put]
func (h APIRestJetStreamManagementHandler) UpdateStreamLimits(
	w http.ResponseWriter, r *http.Request,
) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	var limits management.JSStreamLimits
	if err := json.NewDecoder(r.Body).Decode(&limits); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	if err := h.core.UpdateStreamLimits(r.Context(), streamName, limits); err != nil {
		msg := fmt.Sprintf("Failed to change stream %s limits", streamName)
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusInternalServerError
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
		return
	}

	respCode = http.StatusOK
	respBody = h.GetStdRESTSuccessMsg(r.Context())
}

// UpdateStreamLimitsHandler Wrapper around UpdateStreamLimits
func (h APIRestJetStreamManagementHandler) UpdateStreamLimitsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.UpdateStreamLimits(w, r)
	}
}

// -----------------------------------------------------------------------

// DeleteStream godoc
// @Summary Delete a stream
// @Description Delete a stream
// @tags Management
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Success 200 {object} goutils.RestAPIBaseResponse "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream/{streamName} [delete]
func (h APIRestJetStreamManagementHandler) DeleteStream(w http.ResponseWriter, r *http.Request) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	if err := h.core.DeleteStream(r.Context(), streamName); err != nil {
		msg := fmt.Sprintf("Failed to delete stream %s", streamName)
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusInternalServerError
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
		return
	}

	respCode = http.StatusOK
	respBody = h.GetStdRESTSuccessMsg(r.Context())
}

// DeleteStreamHandler Wrapper around DeleteStream
func (h APIRestJetStreamManagementHandler) DeleteStreamHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.DeleteStream(w, r)
	}
}

// =======================================================================
// Consumer related management

// -----------------------------------------------------------------------

// CreateConsumer godoc
// @Summary Create a consumer on a stream
// @Description Create a new consumer on a stream. The stream must already be defined.
// @tags Management
// @Accept json
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Param consumerParam body management.JetStreamConsumerParam true "Consumer parameters"
// @Success 200 {object} goutils.RestAPIBaseResponse "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream/{streamName}/consumer [post]
func (h APIRestJetStreamManagementHandler) CreateConsumer(w http.ResponseWriter, r *http.Request) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	var params management.JetStreamConsumerParam
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	if err := h.core.CreateConsumerForStream(r.Context(), streamName, params); err != nil {
		msg := fmt.Sprintf("Failed to create consumer on stream %s", streamName)
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusInternalServerError
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
		return
	}

	respCode = http.StatusOK
	respBody = h.GetStdRESTSuccessMsg(r.Context())
}

// CreateConsumerHandler Wrapper around CreateConsumer
func (h APIRestJetStreamManagementHandler) CreateConsumerHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.CreateConsumer(w, r)
	}
}

// -----------------------------------------------------------------------

// APIRestRespAllJetStreamConsumers response for listing all consumers
type APIRestRespAllJetStreamConsumers struct {
	goutils.RestAPIBaseResponse
	// Consumers the set of consumer details mapped against consumer name
	Consumers map[string]APIRestRespConsumerInfo `json:"consumers"`
}

// GetAllConsumers godoc
// @Summary Get all consumers of a stream
// @Description Query for the details of all consumers of a stream
// @tags Management
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Success 200 {object} APIRestRespAllJetStreamConsumers "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream/{streamName}/consumer [get]
func (h APIRestJetStreamManagementHandler) GetAllConsumers(
	w http.ResponseWriter, r *http.Request,
) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	consumers := h.core.GetAllConsumersForStream(r.Context(), streamName)
	converted := make(map[string]APIRestRespConsumerInfo)
	for consumerName, consumerInfo := range consumers {
		converted[consumerName] = convertConsumerInfo(consumerInfo)
	}
	resp := APIRestRespAllJetStreamConsumers{
		RestAPIBaseResponse: goutils.RestAPIBaseResponse{
			Success: true, RequestID: h.ReadRequestIDFromContext(r.Context()),
		},
		Consumers: converted,
	}
	respCode = http.StatusOK
	respBody = resp
}

// GetAllConsumersHandler Wrapper around GetAllConsumers
func (h APIRestJetStreamManagementHandler) GetAllConsumersHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.GetAllConsumers(w, r)
	}
}

// -----------------------------------------------------------------------

// APIRestRespOneJetStreamConsumer response for listing one consumer
type APIRestRespOneJetStreamConsumer struct {
	goutils.RestAPIBaseResponse
	// Consumer the details regarding this consumer
	Consumer APIRestRespConsumerInfo `json:"consumer"`
}

// GetConsumer godoc
// @Summary Get one consumer of a stream
// @Description Query for the details of a consumer on a stream
// @tags Management
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Param consumerName path string true "JetStream consumer name"
// @Success 200 {object} APIRestRespOneJetStreamConsumer "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream/{streamName}/consumer/{consumerName} [get]
func (h APIRestJetStreamManagementHandler) GetConsumer(w http.ResponseWriter, r *http.Request) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}
	consumerName, ok := vars["consumerName"]
	if !ok {
		msg := "No consumer name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(consumerName, h.validate); err != nil {
		msg := "Invalid consumer name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	info, err := h.core.GetConsumerForStream(r.Context(), streamName, consumerName)
	if err != nil {
		msg := fmt.Sprintf("Failed to read consumer %s on stream %s", consumerName, streamName)
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusInternalServerError
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
		return
	}

	resp := APIRestRespOneJetStreamConsumer{
		RestAPIBaseResponse: goutils.RestAPIBaseResponse{
			Success: true, RequestID: h.ReadRequestIDFromContext(r.Context()),
		},
		Consumer: convertConsumerInfo(info),
	}
	respCode = http.StatusOK
	respBody = resp
}

// GetConsumerHandler Wrapper around GetConsumer
func (h APIRestJetStreamManagementHandler) GetConsumerHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.GetConsumer(w, r)
	}
}

// -----------------------------------------------------------------------

// DeleteConsumer godoc
// @Summary Delete one consumer of a stream
// @Description Delete one consumer of a stream
// @tags Management
// @Produce json
// @Param Httpmq-Request-ID header string false "User provided request ID to match against logs"
// @Param streamName path string true "JetStream stream name"
// @Param consumerName path string true "JetStream consumer name"
// @Success 200 {object} goutils.RestAPIBaseResponse "success"
// @Failure 400 {object} goutils.RestAPIBaseResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Header 200,400,500 {string} Httpmq-Request-ID "Request ID to match against logs"
// @Router /v1/admin/stream/{streamName}/consumer/{consumerName} [delete]
func (h APIRestJetStreamManagementHandler) DeleteConsumer(w http.ResponseWriter, r *http.Request) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	vars := mux.Vars(r)
	streamName, ok := vars["streamName"]
	if !ok {
		msg := "No stream name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(streamName, h.validate); err != nil {
		msg := "Invalid stream name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}
	consumerName, ok := vars["consumerName"]
	if !ok {
		msg := "No consumer name provided"
		log.WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, msg)
		return
	}
	if err := common.ValidateTopLevelEntityName(consumerName, h.validate); err != nil {
		msg := "Invalid consumer name"
		log.WithError(err).WithFields(localLogTags).Errorf(msg)
		respCode = http.StatusBadRequest
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusBadRequest, msg, err.Error())
		return
	}

	if err := h.core.DeleteConsumerOnStream(r.Context(), streamName, consumerName); err != nil {
		msg := fmt.Sprintf("Failed to delete consumer %s on stream %s", consumerName, streamName)
		log.WithError(err).WithFields(localLogTags).Error(msg)
		respCode = http.StatusInternalServerError
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
		return
	}

	respCode = http.StatusOK
	respBody = h.GetStdRESTSuccessMsg(r.Context())
}

// DeleteConsumerHandler Wrapper around DeleteConsumer
func (h APIRestJetStreamManagementHandler) DeleteConsumerHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.DeleteConsumer(w, r)
	}
}

// =======================================================================
// Health Checks

// -----------------------------------------------------------------------

// Alive godoc
// @Summary For management REST API liveness check
// @Description Will return success to indicate management REST API module is live
// @tags Management
// @Produce json
// @Success 200 {object} goutils.RestAPIBaseResponse "success"
// @Failure 400 {string} string "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Router /v1/admin/alive [get]
func (h APIRestJetStreamManagementHandler) Alive(w http.ResponseWriter, r *http.Request) {
	localLogTags := h.GetLogTagsForContext(r.Context())
	if err := h.WriteRESTResponse(
		w, http.StatusOK, h.GetStdRESTSuccessMsg(r.Context()), nil,
	); err != nil {
		log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
	}
}

// AliveHandler Wrapper around Alive
func (h APIRestJetStreamManagementHandler) AliveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.Alive(w, r)
	}
}

// -----------------------------------------------------------------------

// Ready godoc
// @Summary For management REST API readiness check
// @Description Will return success if management REST API module is ready for use
// @tags Management
// @Produce json
// @Success 200 {object} goutils.RestAPIBaseResponse "success"
// @Failure 400 {string} string "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} goutils.RestAPIBaseResponse "error"
// @Router /v1/admin/ready [get]
func (h APIRestJetStreamManagementHandler) Ready(w http.ResponseWriter, r *http.Request) {
	msg := "not ready"
	localLogTags := h.GetLogTagsForContext(r.Context())
	var respCode int
	var respBody interface{}
	defer func() {
		if err := h.WriteRESTResponse(w, respCode, respBody, nil); err != nil {
			log.WithError(err).WithFields(localLogTags).Error("Failed to form response")
		}
	}()

	if ready, err := h.core.Ready(); err != nil {
		respCode = http.StatusInternalServerError
		respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
	} else {
		if ready {
			respCode = http.StatusOK
			respBody = h.GetStdRESTSuccessMsg(r.Context())
		} else {
			respCode = http.StatusInternalServerError
			respBody = h.GetStdRESTErrorMsg(r.Context(), http.StatusInternalServerError, msg, err.Error())
		}
	}
}

// ReadyHandler Wrapper around Alive
func (h APIRestJetStreamManagementHandler) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.Ready(w, r)
	}
}
