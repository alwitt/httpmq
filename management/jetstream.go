package management

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/core"
)

// JSStreamLimits list stream data retention settings
type JSStreamLimits struct {
	MaxConsumers      *int           `json:"max_consumers,omitempty"`
	MaxMsgs           *int64         `json:"max_msgs,omitempty"`
	MaxBytes          *int64         `json:"max_bytes,omitempty"`
	MaxAge            *time.Duration `json:"max_age,omitempty"`
	MaxMsgsPerSubject *int64         `json:"max_msgs_per_subject,omitempty"`
	MaxMsgSize        *int32         `json:"max_msg_size,omitempty"`
}

// JSStreamParam list parameters for defining a stream
type JSStreamParam struct {
	// Name is the stream name
	Name     string   `json:"name" validate:"required"`
	Subjects []string `json:"subjects,omitempty"`
	JSStreamLimits
}

// JetStreamConsumerParam list parameters for defining a consumer
type JetStreamConsumerParam struct {
	Name  string `json:"name" validate:"required"`
	Notes string `json:"notes,omitempty"`
	// Set the consumer to filter for subjects matching this regex string
	FilterSubject *string `json:"filter_subject,omitempty"`
	// DeliveryGroup a consumer with non-empty group value can be used
	// by multiple client instances. For a subject a group listens to,
	// the messages will shared amongst the members
	DeliveryGroup *string `json:"delivery_group,omitempty"`
	// MaxInflight max number of un-ACKed message permitted in-flight
	MaxInflight int `json:"max_inflight" validate:"required,gte=1"`
	// Mode whether the consumer is push or pull consumer
	Mode string `json:"mode" validate:"required,oneof=push pull"`
}

// JetStreamController manage JetStream
type JetStreamController interface {
	// ========================================================
	// Stream related management
	// CreateStream create a new JS stream given parameters
	CreateStream(param JSStreamParam) error
	// GetAllStreams query for info on all available JS stream
	GetAllStreams(ctxt context.Context) map[string]*nats.StreamInfo
	// GetStream query for info on one JS stream by name
	GetStream(name string) (*nats.StreamInfo, error)
	// ChangeStreamSubjects changes the target subjects of a JS stream
	ChangeStreamSubjects(stream string, newSubjects []string) error
	// UpdateStreamLimits change the data retention limits of the JS stream
	UpdateStreamLimits(stream string, newLimits JSStreamLimits) error
	// Deletestream delete a JS stream by name
	DeleteStream(name string) error
	// ========================================================
	// Consumer related management
	// CreateConsumerForStream create a new consumer on a JS stream
	CreateConsumerForStream(stream string, param JetStreamConsumerParam) error
	// GetAllConsumersForStream query for info on all consumers of a JS stream
	GetAllConsumersForStream(stream string, ctxt context.Context) map[string]*nats.ConsumerInfo
	// GetConsumerForStream query for info of a consumer of a JS stream
	GetConsumerForStream(stream, consumerName string) (*nats.ConsumerInfo, error)
	// DeleteConsumerOnStream delete consumer of a JS stream
	DeleteConsumerOnStream(stream, consumerName string) error
}

// jetStreamControllerImpl manage JetStream
type jetStreamControllerImpl struct {
	common.Component
	core     *core.NatsClient
	validate *validator.Validate
}

// GetJetStreamController define JetStreamController
func GetJetStreamController(
	natsCore *core.NatsClient, instance string,
) (JetStreamController, error) {
	logTags := log.Fields{
		"module":    "management",
		"component": "jetstream",
		"instance":  instance,
	}
	return jetStreamControllerImpl{
		Component: common.Component{LogTags: logTags},
		core:      natsCore,
		validate:  validator.New(),
	}, nil
}

// =======================================================================
// Stream related controls

// GetAllStreams fetch the list of all known stream
func (js jetStreamControllerImpl) GetAllStreams(ctxt context.Context) map[string]*nats.StreamInfo {
	readChan := js.core.JetStream().StreamsInfo()
	knownStreams := map[string]*nats.StreamInfo{}
	readAll := false
	for !readAll {
		select {
		case info, ok := <-readChan:
			if !ok || info == nil {
				log.WithFields(js.LogTags).Errorf("Stream info chan read failure")
				readAll = true
				break
			}
			if _, ok := knownStreams[info.Config.Name]; ok {
				log.WithFields(js.LogTags).Errorf(
					"Stream info contain multiple entry of %s", info.Config.Name,
				)
			}
			knownStreams[info.Config.Name] = info
		case <-ctxt.Done():
			// out of time
			readAll = true
		}
	}
	return knownStreams
}

// GetStream get info on one stream
func (js jetStreamControllerImpl) GetStream(name string) (*nats.StreamInfo, error) {
	info, err := js.core.JetStream().StreamInfo(name)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to get stream %s info", name)
	}
	return info, err
}

func applyStreamLimits(targetLimit *JSStreamLimits, param *nats.StreamConfig) {
	if targetLimit.MaxConsumers != nil {
		param.MaxConsumers = *targetLimit.MaxConsumers
	}
	if targetLimit.MaxMsgs != nil {
		param.MaxMsgs = *targetLimit.MaxMsgs
	}
	if targetLimit.MaxBytes != nil {
		param.MaxBytes = *targetLimit.MaxBytes
	}
	if targetLimit.MaxAge != nil {
		param.MaxAge = *targetLimit.MaxAge
	}
	if targetLimit.MaxMsgsPerSubject != nil {
		param.MaxMsgsPerSubject = *targetLimit.MaxMsgsPerSubject
	}
	if targetLimit.MaxMsgSize != nil {
		param.MaxMsgSize = *targetLimit.MaxMsgSize
	}
}

// CreateStream define a new stream
func (js jetStreamControllerImpl) CreateStream(param JSStreamParam) error {
	// Convert to JetStream structure
	jsParams := nats.StreamConfig{
		Name:     param.Name,
		Subjects: param.Subjects,
	}
	applyStreamLimits(&param.JSStreamLimits, &jsParams)
	if _, err := js.core.JetStream().AddStream(&jsParams); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new stream %s", param.Name,
		)
		return err
	}
	log.WithFields(js.LogTags).Infof("Defined new stream %s", param.Name)
	return nil
}

// DeleteStream delete an existing stream
func (js jetStreamControllerImpl) DeleteStream(name string) error {
	if err := js.core.JetStream().DeleteStream(name); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to delete stream %s", name)
		return err
	}
	log.WithFields(js.LogTags).Infof("Deleted stream %s", name)
	return nil
}

// ChangeStreamSubjects change the set of subjects the stream collects for
func (js jetStreamControllerImpl) ChangeStreamSubjects(stream string, newSubjects []string) error {
	info, err := js.core.JetStream().StreamInfo(stream)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to get stream %s info", stream)
		return err
	}
	currentConfig := info.Config
	currentConfig.Subjects = newSubjects
	_, err = js.core.JetStream().UpdateStream(&currentConfig)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to change stream %s subjects", stream,
		)
	} else {
		t, _ := json.Marshal(newSubjects)
		log.WithFields(js.LogTags).Errorf("Change stream %s subjects to %s", stream, t)
	}
	return err
}

// UpdateStreamLimits update a stream's data retention limits
func (js jetStreamControllerImpl) UpdateStreamLimits(
	stream string, newLimits JSStreamLimits,
) error {
	info, err := js.core.JetStream().StreamInfo(stream)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to get stream %s info", stream)
		return err
	}
	currentConfig := info.Config
	applyStreamLimits(&newLimits, &currentConfig)
	if _, err := js.core.JetStream().UpdateStream(&currentConfig); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Failed to update stream %s retention limits", stream,
		)
		return err
	}
	log.WithFields(js.LogTags).Errorf("Updated stream %s retention limits", stream)
	return nil
}

// =======================================================================
// Consumer related controls

// GetAllConsumersForStream fetch the list of all consumers known consumer of a stream
func (js jetStreamControllerImpl) GetAllConsumersForStream(
	stream string, ctxt context.Context,
) map[string]*nats.ConsumerInfo {
	readChan := js.core.JetStream().ConsumersInfo(stream)
	knownConsumer := map[string]*nats.ConsumerInfo{}
	readAll := false
	for !readAll {
		select {
		case info, ok := <-readChan:
			if !ok || info == nil {
				log.WithFields(js.LogTags).Errorf("Consumer info chan read failure")
				readAll = true
				break
			}
			knownConsumer[info.Name] = info
		case <-ctxt.Done():
			// out of time
			readAll = true
		}
	}
	return knownConsumer
}

// GetConsumerForStream get info on one consumer of a stream
func (js jetStreamControllerImpl) GetConsumerForStream(
	stream, consumerName string,
) (*nats.ConsumerInfo, error) {
	info, err := js.core.JetStream().ConsumerInfo(stream, consumerName)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to get consumer %s of stream %s info", consumerName, stream,
		)
	}
	return info, err
}

// CreateConsumerForStream define a new consumer for a stream
func (js jetStreamControllerImpl) CreateConsumerForStream(
	stream string, param JetStreamConsumerParam,
) error {
	// Verify the parameters are acceptable
	if err := js.validate.Struct(&param); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new consumer %s for stream %s", param.Name, stream,
		)
		return err
	}
	// Convert to JetStream structure
	jsParams := nats.ConsumerConfig{
		Durable:       param.Name,
		Description:   param.Notes,
		MaxDeliver:    param.MaxInflight,
		MaxAckPending: param.MaxInflight,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
	}
	// Verify the configuration made sense
	if param.Mode == "pull" && param.DeliveryGroup != nil {
		err := fmt.Errorf("pull consumer can't use delivery group")
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new consumer %s for stream %s", param.Name, stream,
		)
		return err
	}
	// Set the delivery group
	if param.DeliveryGroup != nil {
		jsParams.DeliverGroup = *param.DeliveryGroup
	}
	// To insure that PUSH mode is used, a subject is needed
	if param.Mode == "push" {
		jsParams.DeliverSubject = nats.NewInbox()
	}
	// Filter for specific subjects
	if param.FilterSubject != nil {
		jsParams.FilterSubject = *param.FilterSubject
	}
	// Define the consumer
	if _, err := js.core.JetStream().AddConsumer(stream, &jsParams); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new consumer %s for stream %s", param.Name, stream,
		)
		return err
	}
	log.WithFields(js.LogTags).Infof(
		"Defined new consumer %s for stream %s", param.Name, stream,
	)
	return nil
}

// DeleteConsumerOnStream delete consumer from a stream
func (js jetStreamControllerImpl) DeleteConsumerOnStream(stream, consumerName string) error {
	if err := js.core.JetStream().DeleteConsumer(stream, consumerName); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to delete consumer %s from stream %s", consumerName, stream,
		)
		return err
	}
	log.WithFields(js.LogTags).Infof("Deleted consumer %s from stream %s", consumerName, stream)
	return nil
}
