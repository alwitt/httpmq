package apis

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/management"
)

// APIRestJetStreamManagementHandler REST handler for management of JetStream
type APIRestJetStreamManagementHandler struct {
	APIRestHandler
	core     management.JetStreamController
	validate *validator.Validate
}

// GetAPIRestJetStreamManagementHandler define APIRestJetStreamManagementHandler
func GetAPIRestJetStreamManagementHandler(
	core management.JetStreamController,
) (APIRestJetStreamManagementHandler, error) {
	logTags := log.Fields{
		"module":    "rest",
		"component": "jetstream-management",
	}
	return APIRestJetStreamManagementHandler{
		APIRestHandler: APIRestHandler{
			Component: common.Component{LogTags: logTags},
		}, core: core, validate: validator.New(),
	}, nil
}

// APIRestRespStreamConfig Adhoc structure for persenting nats.StreamConfig
type APIRestRespStreamConfig struct {
	Name              string        `json:"name"`
	Description       string        `json:"description,omitempty"`
	Subjects          []string      `json:"subjects,omitempty"`
	MaxConsumers      int           `json:"max_consumers"`
	MaxMsgs           int64         `json:"max_msgs"`
	MaxBytes          int64         `json:"max_bytes"`
	MaxAge            time.Duration `json:"max_age" swaggertype:"primitive,integer"`
	MaxMsgsPerSubject int64         `json:"max_msgs_per_subject"`
	MaxMsgSize        int32         `json:"max_msg_size,omitempty"`
}

// APIRestRespStreamState Adhoc structure for persenting nats.StreamState
type APIRestRespStreamState struct {
	Msgs      uint64    `json:"messages"`
	Bytes     uint64    `json:"bytes"`
	FirstSeq  uint64    `json:"first_seq"`
	FirstTime time.Time `json:"first_ts"`
	LastSeq   uint64    `json:"last_seq"`
	LastTime  time.Time `json:"last_ts"`
	Consumers int       `json:"consumer_count"`
}

// APIRestRespStreamInfo Adhoc structure for persenting nats.StreamInfo
type APIRestRespStreamInfo struct {
	Config  APIRestRespStreamConfig `json:"config"`
	Created time.Time               `json:"created"`
	State   APIRestRespStreamState  `json:"state"`
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

// APIRestRespConsumerConfig Adhoc structure for persenting nats.ConsumerConfig
type APIRestRespConsumerConfig struct {
	Description    string        `json:"notes,omitempty"`
	DeliverSubject string        `json:"deliver_subject,omitempty"`
	DeliverGroup   string        `json:"deliver_group,omitempty"`
	MaxDeliver     int           `json:"max_deliver,omitempty"`
	AckWait        time.Duration `json:"ack_wait" swaggertype:"primitive,integer"`
	FilterSubject  string        `json:"filter_subject,omitempty"`
	MaxWaiting     int           `json:"max_waiting,omitempty"`
	MaxAckPending  int           `json:"max_ack_pending,omitempty"`
}

// APIRestRespSequenceInfo Adhoc structure for persenting nats.SequenceInfo
type APIRestRespSequenceInfo struct {
	Consumer uint64     `json:"consumer_seq"`
	Stream   uint64     `json:"stream_seq"`
	Last     *time.Time `json:"last_active,omitempty"`
}

// APIRestRespConsumerInfo Adhoc structure for persenting nats.ConsumerInfo
type APIRestRespConsumerInfo struct {
	Stream         string                    `json:"stream_name"`
	Name           string                    `json:"name"`
	Created        time.Time                 `json:"created"`
	Config         APIRestRespConsumerConfig `json:"config"`
	Delivered      APIRestRespSequenceInfo   `json:"delivered"`
	AckFloor       APIRestRespSequenceInfo   `json:"ack_floor"`
	NumAckPending  int                       `json:"num_ack_pending"`
	NumRedelivered int                       `json:"num_redelivered"`
	NumWaiting     int                       `json:"num_waiting"`
	NumPending     uint64                    `json:"num_pending"`
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
// @tags management,post,stream
// @Accept json
// @Produce json
// @Param setting body management.JSStreamParam true "JetStream stream setting"
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream [post]
func (h APIRestJetStreamManagementHandler) CreateStream(w http.ResponseWriter, r *http.Request) {
	restCall := "POST /v1/admin/stream"

	localLogTags, err := common.UpdateLogTags(h.LogTags, r.Context())
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

	// Parse the parameters
	var params management.JSStreamParam
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	if err := h.core.CreateStream(params, r.Context()); err != nil {
		msg := "Failed to create new stream"
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

// CreateStreamHandler Wrapper around CreateStream
func (h APIRestJetStreamManagementHandler) CreateStreamHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.CreateStream(w, r)
	})
}

// -----------------------------------------------------------------------

// APIRestRespAllJetStreams response for listing all streams
type APIRestRespAllJetStreams struct {
	StandardResponse
	Streams map[string]APIRestRespStreamInfo `json:"streams,omitempty"`
}

// GetAllStreams godoc
// @Summary Query for info on all streams
// @Description Query for info on all streams
// @tags management,get,stream
// @Produce json
// @Param setting body management.JSStreamParam true "JetStream stream setting"
// @Success 200 {object} APIRestRespAllJetStreams "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream [get]
func (h APIRestJetStreamManagementHandler) GetAllStreams(w http.ResponseWriter, r *http.Request) {
	restCall := "GET /v1/admin/stream"

	allInfo := h.core.GetAllStreams(r.Context())
	convertedInfo := make(map[string]APIRestRespStreamInfo)
	for streamName, streamInfo := range allInfo {
		convertedInfo[streamName] = convertStreamInfo(streamInfo)
	}
	resp := APIRestRespAllJetStreams{
		StandardResponse: StandardResponse{Success: true}, Streams: convertedInfo,
	}

	h.reply(w, http.StatusOK, resp, restCall, r)
}

// GetAllStreamsHandler Wrapper around GetAllStreams
func (h APIRestJetStreamManagementHandler) GetAllStreamsHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.GetAllStreams(w, r)
	})
}

// -----------------------------------------------------------------------

// APIRestRespOneJetStream response for listing one stream
type APIRestRespOneJetStream struct {
	StandardResponse
	Stream APIRestRespStreamInfo `json:"stream,omitempty"`
}

// GetStream godoc
// @Summary Query for info on one stream
// @Description Query for info on one stream
// @tags management,get,stream
// @Produce json
// @Param streamName path string true "JetStream stream name"
// @Success 200 {object} APIRestRespOneJetStream "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream/{streamName} [get]
func (h APIRestJetStreamManagementHandler) GetStream(w http.ResponseWriter, r *http.Request) {
	restCall := "GET /v1/admin/stream/{streamName}"

	localLogTags, err := common.UpdateLogTags(h.LogTags, r.Context())
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

	streamInfo, err := h.core.GetStream(streamName, r.Context())
	if err != nil {
		msg := fmt.Sprintf("Unable fetch stream %s info", streamName)
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
		return
	}
	resp := APIRestRespOneJetStream{
		StandardResponse: StandardResponse{Success: true},
		Stream:           convertStreamInfo(streamInfo),
	}

	h.reply(w, http.StatusOK, resp, restCall, r)
}

// GetStreamHandler Wrapper around GetStream
func (h APIRestJetStreamManagementHandler) GetStreamHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.GetStream(w, r)
	})
}

// -----------------------------------------------------------------------

// APIRestReqStreamSubjects subject change parameters
type APIRestReqStreamSubjects struct {
	Subjects []string `json:"subjects" validate:"required,min=1"`
}

// ChangeStreamSubjects godoc
// @Summary Change subjects of a stream
// @Description Change subjects of a stream
// @tags management,put,stream
// @Accept json
// @Produce json
// @Param streamName path string true "JetStream stream name"
// @Param subjects body APIRestReqStreamSubjects true "List of new subjects"
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream/{streamName}/subject [put]
func (h APIRestJetStreamManagementHandler) ChangeStreamSubjects(
	w http.ResponseWriter, r *http.Request,
) {
	restCall := "PUT /v1/admin/stream/{streamName}/subject"

	localLogTags, err := common.UpdateLogTags(h.LogTags, r.Context())
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

	var subjects APIRestReqStreamSubjects
	if err := json.NewDecoder(r.Body).Decode(&subjects); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	if err := h.validate.Struct(&subjects); err != nil {
		msg := "Bad request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	if err := h.core.ChangeStreamSubjects(streamName, subjects.Subjects, r.Context()); err != nil {
		msg := fmt.Sprintf("Failed to change stream %s subjects", streamName)
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

// ChangeStreamSubjectsHandler Wrapper around ChangeStreamSubjects
func (h APIRestJetStreamManagementHandler) ChangeStreamSubjectsHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.ChangeStreamSubjects(w, r)
	})
}

// -----------------------------------------------------------------------

// UpdateStreamLimits godoc
// @Summary Change limits a stream
// @Description Change limits of a stream
// @tags management,put,stream
// @Accept json
// @Produce json
// @Param streamName path string true "JetStream stream name"
// @Param limits body management.JSStreamLimits true "New stream limits"
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream/{streamName}/limit [put]
func (h APIRestJetStreamManagementHandler) UpdateStreamLimits(
	w http.ResponseWriter, r *http.Request,
) {
	restCall := "PUT /v1/admin/stream/{streamName}/limit"

	localLogTags, err := common.UpdateLogTags(h.LogTags, r.Context())
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

	var limits management.JSStreamLimits
	if err := json.NewDecoder(r.Body).Decode(&limits); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	if err := h.core.UpdateStreamLimits(streamName, limits, r.Context()); err != nil {
		msg := fmt.Sprintf("Failed to change stream %s limits", streamName)
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

// UpdateStreamLimitsHandler Wrapper around UpdateStreamLimits
func (h APIRestJetStreamManagementHandler) UpdateStreamLimitsHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.UpdateStreamLimits(w, r)
	})
}

// -----------------------------------------------------------------------

// DeleteStream godoc
// @Summary Delete a stream
// @Description Delete of a stream
// @tags management,delete,stream
// @Produce json
// @Param streamName path string true "JetStream stream name"
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream/{streamName} [delete]
func (h APIRestJetStreamManagementHandler) DeleteStream(w http.ResponseWriter, r *http.Request) {
	restCall := "DELETE /v1/admin/stream/{streamName}"

	localLogTags, err := common.UpdateLogTags(h.LogTags, r.Context())
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

	if err := h.core.DeleteStream(streamName, r.Context()); err != nil {
		msg := fmt.Sprintf("Failed to delete stream %s", streamName)
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

// DeleteStreamHandler Wrapper around DeleteStream
func (h APIRestJetStreamManagementHandler) DeleteStreamHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.DeleteStream(w, r)
	})
}

// =======================================================================
// Consumer related management

// -----------------------------------------------------------------------

// CreateConsumer godoc
// @Summary Create a consumer on a stream
// @Description Create a consumer on a stream
// @tags management,post,consumer
// @Accept json
// @Produce json
// @Param streamName path string true "JetStream stream name"
// @Param consumerParam body management.JetStreamConsumerParam true "Consumer parameters"
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream/{streamName}/consumer [post]
func (h APIRestJetStreamManagementHandler) CreateConsumer(w http.ResponseWriter, r *http.Request) {
	restCall := "POST /v1/admin/stream/{streamName}/consumer"

	localLogTags, err := common.UpdateLogTags(h.LogTags, r.Context())
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

	var params management.JetStreamConsumerParam
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		msg := "Unable to parse request body"
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	if err := h.core.CreateConsumerForStream(streamName, params, r.Context()); err != nil {
		msg := fmt.Sprintf("Failed to create consumer on stream %s", streamName)
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

// CreateConsumerHandler Wrapper around CreateConsumer
func (h APIRestJetStreamManagementHandler) CreateConsumerHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.CreateConsumer(w, r)
	})
}

// -----------------------------------------------------------------------

// APIRestRespAllJetStreamConsumers response for listing all consumers
type APIRestRespAllJetStreamConsumers struct {
	StandardResponse
	Consumers map[string]APIRestRespConsumerInfo `json:"consumers,omitempty"`
}

// GetAllConsumers godoc
// @Summary Get all consumers of a stream
// @Description Get all consumers of a stream
// @tags management,get,consumer
// @Produce json
// @Param streamName path string true "JetStream stream name"
// @Success 200 {object} APIRestRespAllJetStreamConsumers "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream/{streamName}/consumer [get]
func (h APIRestJetStreamManagementHandler) GetAllConsumers(
	w http.ResponseWriter, r *http.Request,
) {
	restCall := "GET /v1/admin/stream/{streamName}/consumer"

	localLogTags, err := common.UpdateLogTags(h.LogTags, r.Context())
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

	consumers := h.core.GetAllConsumersForStream(streamName, r.Context())
	converted := make(map[string]APIRestRespConsumerInfo)
	for consumerName, consumerInfo := range consumers {
		converted[consumerName] = convertConsumerInfo(consumerInfo)
	}
	resp := APIRestRespAllJetStreamConsumers{
		StandardResponse: StandardResponse{Success: true},
		Consumers:        converted,
	}
	h.reply(w, http.StatusOK, resp, restCall, r)
}

// GetAllConsumersHandler Wrapper around GetAllConsumers
func (h APIRestJetStreamManagementHandler) GetAllConsumersHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.GetAllConsumers(w, r)
	})
}

// -----------------------------------------------------------------------

// APIRestRespOneJetStreamConsumer response for listing one consumer
type APIRestRespOneJetStreamConsumer struct {
	StandardResponse
	Consumer APIRestRespConsumerInfo `json:"consumer,omitempty"`
}

// GetConsumer godoc
// @Summary Get one consumer of a stream
// @Description Get one consumer of a stream
// @tags management,get,consumer
// @Produce json
// @Param streamName path string true "JetStream stream name"
// @Param consumerName path string true "JetStream consumer name"
// @Success 200 {object} APIRestRespOneJetStreamConsumer "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream/{streamName}/consumer/{consumerName} [get]
func (h APIRestJetStreamManagementHandler) GetConsumer(w http.ResponseWriter, r *http.Request) {
	restCall := "GET /v1/admin/stream/{streamName}/consumer/{consumerName}"

	localLogTags, err := common.UpdateLogTags(h.LogTags, r.Context())
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
	consumerName, ok := vars["consumerName"]
	if !ok {
		msg := "No consumer name provided"
		log.WithFields(localLogTags).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	info, err := h.core.GetConsumerForStream(streamName, consumerName, r.Context())
	if err != nil {
		msg := fmt.Sprintf("Failed to read consumer %s on stream %s", consumerName, streamName)
		log.WithError(err).WithFields(localLogTags).Error(msg)
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
		return
	}

	resp := APIRestRespOneJetStreamConsumer{
		StandardResponse: StandardResponse{Success: true},
		Consumer:         convertConsumerInfo(info),
	}
	h.reply(w, http.StatusOK, resp, restCall, r)
}

// GetConsumerHandler Wrapper around GetConsumer
func (h APIRestJetStreamManagementHandler) GetConsumerHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.GetConsumer(w, r)
	})
}

// -----------------------------------------------------------------------

// DeleteConsumer godoc
// @Summary Delete one consumer of a stream
// @Description Delete one consumer of a stream
// @tags management,delete,consumer
// @Produce json
// @Param streamName path string true "JetStream stream name"
// @Param consumerName path string true "JetStream consumer name"
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {object} StandardResponse "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /v1/admin/stream/{streamName}/consumer/{consumerName} [delete]
func (h APIRestJetStreamManagementHandler) DeleteConsumer(w http.ResponseWriter, r *http.Request) {
	restCall := "DELETE /v1/admin/stream/{streamName}/consumer/{consumerName}"

	localLogTags, err := common.UpdateLogTags(h.LogTags, r.Context())
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
	consumerName, ok := vars["consumerName"]
	if !ok {
		msg := "No consumer name provided"
		log.WithFields(localLogTags).Errorf(msg)
		h.reply(w, http.StatusBadRequest, getStdRESTErrorMsg(http.StatusBadRequest, &msg), restCall, r)
		return
	}

	if err := h.core.DeleteConsumerOnStream(streamName, consumerName, r.Context()); err != nil {
		msg := fmt.Sprintf("Failed to delete consumer %s on stream %s", consumerName, streamName)
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

// DeleteConsumerHandler Wrapper around DeleteConsumer
func (h APIRestJetStreamManagementHandler) DeleteConsumerHandler() http.HandlerFunc {
	return h.attachRequestID(func(w http.ResponseWriter, r *http.Request) {
		h.DeleteConsumer(w, r)
	})
}

// =======================================================================
// Health Checks

// -----------------------------------------------------------------------

// Alive godoc
// @Summary For liveness check
// @Description For liveness check
// @tags management,get,health
// @Produce json
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {string} string "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /alive [get]
func (h APIRestJetStreamManagementHandler) Alive(w http.ResponseWriter, r *http.Request) {
	restCall := "GET /alive"
	h.reply(w, http.StatusOK, getStdRESTSuccessMsg(), restCall, r)
}

// AliveHandler Wrapper around Alive
func (h APIRestJetStreamManagementHandler) AliveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.Alive(w, r)
	}
}

// -----------------------------------------------------------------------

// Ready godoc
// @Summary For readiness check
// @Description For readiness check
// @tags management,get,health
// @Produce json
// @Success 200 {object} StandardResponse "success"
// @Failure 400 {string} string "error"
// @Failure 404 {string} string "error"
// @Failure 500 {object} StandardResponse "error"
// @Router /ready [get]
func (h APIRestJetStreamManagementHandler) Ready(w http.ResponseWriter, r *http.Request) {
	restCall := "GET /ready"
	msg := "not ready"
	if ready, err := h.core.Ready(); err != nil {
		h.reply(
			w, http.StatusInternalServerError, getStdRESTErrorMsg(
				http.StatusInternalServerError, &msg,
			), restCall, r,
		)
	} else {
		if ready {
			h.reply(w, http.StatusOK, getStdRESTSuccessMsg(), restCall, r)
		} else {
			h.reply(
				w, http.StatusInternalServerError, getStdRESTErrorMsg(
					http.StatusInternalServerError, &msg,
				), restCall, r,
			)
		}
	}
}

// ReadyHandler Wrapper around Alive
func (h APIRestJetStreamManagementHandler) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.Ready(w, r)
	}
}
