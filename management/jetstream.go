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

package management

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/core"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
)

// JSStreamLimits is the set of stream data retention settings
type JSStreamLimits struct {
	// MaxConsumers is the max number of consumers allowed on the stream
	MaxConsumers *int `json:"max_consumers,omitempty" validate:"omitempty,gte=-1"`
	// MaxMsgs is the max number of messages the stream will store.
	//
	// Oldest messages are removed once limit breached.
	MaxMsgs *int64 `json:"max_msgs,omitempty" validate:"omitempty,gte=-1"`
	// MaxBytes is the max number of message bytes the stream will store.
	//
	// Oldest messages are removed once limit breached.
	MaxBytes *int64 `json:"max_bytes,omitempty" validate:"omitempty,gte=-1"`
	// MaxAge is the max duration (ns) the stream will store a message
	//
	// Messages breaching the limit will be removed.
	MaxAge *time.Duration `json:"max_age,omitempty" swaggertype:"primitive,integer"`
	// MaxMsgsPerSubject is the maximum number of subjects allowed on this stream
	MaxMsgsPerSubject *int64 `json:"max_msgs_per_subject,omitempty" validate:"omitempty,gte=-1"`
	// MaxMsgSize is the max size of a message allowed in this stream
	MaxMsgSize *int32 `json:"max_msg_size,omitempty" validate:"omitempty,gte=-1"`
}

// JSStreamParam are the parameters for defining a stream
type JSStreamParam struct {
	// Name is the stream name
	Name string `json:"name" validate:"required,alphaunicode|uuid"`
	// Subjects is the list of subjects of interest for this stream
	Subjects []string `json:"subjects,omitempty"`
	// JSStreamLimits stream data retention limits
	JSStreamLimits
}

// JetStreamConsumerParam are the parameters for defining a consumer on a stream
type JetStreamConsumerParam struct {
	// Name is the consumer name
	Name string `json:"name" validate:"required,alphaunicode|uuid"`
	// Notes are descriptions regarding this consumer
	Notes string `json:"notes,omitempty"`
	// FilterSubject sets the consumer to filter for subjects matching this NATs subject string
	//
	// See https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/naming
	FilterSubject *string `json:"filter_subject,omitempty"`
	// DeliveryGroup creates a consumer using a delivery group name.
	//
	// A consumer using delivery group allows multiple clients to subscribe under the same consumer
	// and group name tuple. For subjects this consumer listens to, the messages will be shared
	// amongst the connected clients.
	DeliveryGroup *string `json:"delivery_group,omitempty" validate:"omitempty,alphaunicode|uuid"`
	// MaxInflight is max number of un-ACKed message permitted in-flight (must be >= 1)
	MaxInflight int `json:"max_inflight" validate:"required,gte=1"`
	// MaxRetry max number of times an un-ACKed message is resent (-1: infinite)
	MaxRetry *int `json:"max_retry,omitempty" validate:"omitempty,gte=-1"`
	// AckWait when specified, the number of ns to wait for ACK before retry
	AckWait *time.Duration `json:"ack_wait,omitempty" swaggertype:"primitive,integer"`
	// Mode whether the consumer is push or pull consumer
	Mode string `json:"mode" validate:"required,oneof=push pull"`
}

// JetStreamController is a JetStream controller instance. It proxes the commands to JetStream.
type JetStreamController interface {
	// Ready indicates whether the system is considered ready
	Ready() (bool, error)
	// ========================================================
	// Stream related management

	// CreateStream creates a new stream given parameters
	CreateStream(ctxt context.Context, param JSStreamParam) error
	// GetAllStreams queries for info on all available streams
	GetAllStreams(ctxt context.Context) map[string]*nats.StreamInfo
	// GetStream queries for info on one stream by name
	GetStream(ctxt context.Context, name string) (*nats.StreamInfo, error)
	// ChangeStreamSubjects changes the target subjects of a stream
	ChangeStreamSubjects(ctxt context.Context, stream string, newSubjects []string) error
	// UpdateStreamLimits changes the data retention limits of the stream
	UpdateStreamLimits(ctxt context.Context, stream string, newLimits JSStreamLimits) error
	// Deletestream deletes a stream by name
	DeleteStream(ctxt context.Context, name string) error

	// ========================================================
	// Consumer related management

	// CreateConsumerForStream creates a new consumer for a stream
	CreateConsumerForStream(ctxt context.Context, stream string, param JetStreamConsumerParam) error
	// GetAllConsumersForStream queries for info on all consumers of a stream
	GetAllConsumersForStream(ctxt context.Context, stream string) map[string]*nats.ConsumerInfo
	// GetConsumerForStream queries for info of one consumer of a stream
	GetConsumerForStream(
		ctxt context.Context, stream, consumerName string,
	) (*nats.ConsumerInfo, error)
	// DeleteConsumerOnStream deletes one consumer of a stream
	DeleteConsumerOnStream(ctxt context.Context, stream, consumerName string) error
}

// jetStreamControllerImpl implements JetStreamController
type jetStreamControllerImpl struct {
	common.Component
	core     *core.NatsClient
	validate *validator.Validate
}

// GetJetStreamController define a jetStreamControllerImpl
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

// Ready indicate whether the system is considered ready
func (js jetStreamControllerImpl) Ready() (bool, error) {
	return js.core.NATs().Status() == nats.CONNECTED, nil
}

// =======================================================================
// Stream related controls

// GetAllStreams queries for info on all available streams
func (js jetStreamControllerImpl) GetAllStreams(ctxt context.Context) map[string]*nats.StreamInfo {
	localLogTags, _ := common.UpdateLogTags(ctxt, js.LogTags)
	readChan := js.core.JetStream().StreamsInfo()
	knownStreams := map[string]*nats.StreamInfo{}
	readAll := false
	for !readAll {
		select {
		case info, ok := <-readChan:
			if !ok || info == nil {
				log.WithFields(localLogTags).Errorf("Stream info chan read failure")
				readAll = true
				break
			}
			if _, ok := knownStreams[info.Config.Name]; ok {
				log.WithFields(localLogTags).Errorf(
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

// GetStream queries for info on one stream by name
func (js jetStreamControllerImpl) GetStream(
	ctxt context.Context, name string,
) (*nats.StreamInfo, error) {
	localLogTags, err := common.UpdateLogTags(ctxt, js.LogTags)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Failed to update logtags")
	}
	if err := common.ValidateTopLevelEntityName(name, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to query for stream '%s'", name,
		)
		return nil, err
	}
	info, err := js.core.JetStream().StreamInfo(name)
	if err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf("Unable to get stream %s info", name)
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
		param.Duplicates = *targetLimit.MaxAge
	}
	if targetLimit.MaxMsgsPerSubject != nil {
		param.MaxMsgsPerSubject = *targetLimit.MaxMsgsPerSubject
	}
	if targetLimit.MaxMsgSize != nil {
		param.MaxMsgSize = *targetLimit.MaxMsgSize
	}
}

// CreateStream creates a new stream given parameters
func (js jetStreamControllerImpl) CreateStream(ctxt context.Context, param JSStreamParam) error {
	localLogTags, err := common.UpdateLogTags(ctxt, js.LogTags)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Failed to update logtags")
	}

	if err := js.validate.Struct(&param); err != nil {
		log.WithError(err).WithFields(localLogTags).Error("Stream create parameters invalid")
		return err
	}
	for _, subject := range param.Subjects {
		if err := common.ValidateSubjectName(subject); err != nil {
			log.WithError(err).WithFields(localLogTags).Errorf(
				"Unable to define new stream %s", param.Name,
			)
			return err
		}
	}

	// Convert to JetStream structure
	jsParams := nats.StreamConfig{
		Name:     param.Name,
		Subjects: param.Subjects,
	}
	applyStreamLimits(&param.JSStreamLimits, &jsParams)
	if _, err := js.core.JetStream().AddStream(&jsParams); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to define new stream %s", param.Name,
		)
		return err
	}
	log.WithFields(localLogTags).Infof("Defined new stream %s", param.Name)
	return nil
}

// Deletestream deletes a stream by name
func (js jetStreamControllerImpl) DeleteStream(ctxt context.Context, name string) error {
	localLogTags, err := common.UpdateLogTags(ctxt, js.LogTags)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Failed to update logtags")
	}
	if err := common.ValidateTopLevelEntityName(name, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to delete stream '%s'", name,
		)
		return err
	}
	if err := js.core.JetStream().DeleteStream(name); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf("Unable to delete stream %s", name)
		return err
	}
	log.WithFields(localLogTags).Infof("Deleted stream %s", name)
	return nil
}

// ChangeStreamSubjects changes the target subjects of a stream
func (js jetStreamControllerImpl) ChangeStreamSubjects(
	ctxt context.Context, stream string, newSubjects []string,
) error {
	localLogTags, err := common.UpdateLogTags(ctxt, js.LogTags)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Failed to update logtags")
	}
	if err := common.ValidateTopLevelEntityName(stream, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to update stream '%s' subjects", stream,
		)
		return err
	}
	for _, subject := range newSubjects {
		if err := common.ValidateSubjectName(subject); err != nil {
			log.WithError(err).WithFields(localLogTags).Errorf(
				"Unable to update stream %s target subjects", stream,
			)
			return err
		}
	}
	info, err := js.core.JetStream().StreamInfo(stream)
	if err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf("Unable to get stream %s info", stream)
		return err
	}
	currentConfig := info.Config
	currentConfig.Subjects = newSubjects
	_, err = js.core.JetStream().UpdateStream(&currentConfig)
	if err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to change stream %s subjects", stream,
		)
	} else {
		t, _ := json.Marshal(newSubjects)
		log.WithFields(localLogTags).Errorf("Change stream %s subjects to %s", stream, t)
	}
	return err
}

// UpdateStreamLimits changes the data retention limits of the stream
func (js jetStreamControllerImpl) UpdateStreamLimits(
	ctxt context.Context, stream string, newLimits JSStreamLimits,
) error {
	localLogTags, err := common.UpdateLogTags(ctxt, js.LogTags)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Failed to update logtags")
	}
	if err := common.ValidateTopLevelEntityName(stream, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to update stream '%s' retention limits", stream,
		)
		return err
	}
	if err := js.validate.Struct(&newLimits); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to update stream '%s' retention limits", stream,
		)
		return err
	}
	info, err := js.core.JetStream().StreamInfo(stream)
	if err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf("Unable to get stream %s info", stream)
		return err
	}
	currentConfig := info.Config
	applyStreamLimits(&newLimits, &currentConfig)
	if _, err := js.core.JetStream().UpdateStream(&currentConfig); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Failed to update stream %s retention limits", stream,
		)
		return err
	}
	log.WithFields(localLogTags).Errorf("Updated stream %s retention limits", stream)
	return nil
}

// =======================================================================
// Consumer related controls

// GetAllConsumersForStream queries for info on all consumers of a stream
func (js jetStreamControllerImpl) GetAllConsumersForStream(
	ctxt context.Context, stream string,
) map[string]*nats.ConsumerInfo {
	localLogTags, _ := common.UpdateLogTags(ctxt, js.LogTags)
	if err := common.ValidateTopLevelEntityName(stream, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to list consumers of stream '%s'", stream,
		)
		return nil
	}
	readChan := js.core.JetStream().ConsumersInfo(stream)
	knownConsumer := map[string]*nats.ConsumerInfo{}
	readAll := false
	for !readAll {
		select {
		case info, ok := <-readChan:
			if !ok || info == nil {
				log.WithFields(localLogTags).Errorf("Consumer info chan read failure")
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

// GetConsumerForStream queries for info of one consumer of a stream
func (js jetStreamControllerImpl) GetConsumerForStream(
	ctxt context.Context, stream, consumerName string,
) (*nats.ConsumerInfo, error) {
	localLogTags, err := common.UpdateLogTags(ctxt, js.LogTags)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Failed to update logtags")
	}
	if err := common.ValidateTopLevelEntityName(stream, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to list consumers of stream '%s'", stream,
		)
		return nil, err
	}
	if err := common.ValidateTopLevelEntityName(consumerName, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to fetch consumer '%s' of stream '%s'", consumerName, stream,
		)
		return nil, err
	}
	info, err := js.core.JetStream().ConsumerInfo(stream, consumerName)
	if err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to get consumer %s of stream %s info", consumerName, stream,
		)
	}
	return info, err
}

// CreateConsumerForStream creates a new consumer for a stream
func (js jetStreamControllerImpl) CreateConsumerForStream(
	ctxt context.Context, stream string, param JetStreamConsumerParam,
) error {
	localLogTags, err := common.UpdateLogTags(ctxt, js.LogTags)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Failed to update logtags")
	}
	// Verify the parameters are acceptable
	if err := js.validate.Struct(&param); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to define new consumer %s for stream %s", param.Name, stream,
		)
		return err
	}
	if err := common.ValidateTopLevelEntityName(stream, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to define consumers on stream '%s'", stream,
		)
		return err
	}
	if err := common.ValidateTopLevelEntityName(param.Name, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to define consumer '%s' on stream '%s'", param.Name, stream,
		)
		return err
	}
	// Convert to JetStream structure
	jsParams := nats.ConsumerConfig{
		Durable:       param.Name,
		Description:   param.Notes,
		MaxAckPending: param.MaxInflight,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
	}
	// Redeliver settings
	if param.MaxRetry != nil {
		jsParams.MaxDeliver = *param.MaxRetry
	}
	if param.AckWait != nil {
		jsParams.AckWait = *param.AckWait
	}
	// Verify the configuration made sense
	if param.Mode == "pull" && param.DeliveryGroup != nil {
		err := fmt.Errorf("pull consumer can't use delivery group")
		log.WithError(err).WithFields(localLogTags).Errorf(
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
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to define new consumer %s for stream %s", param.Name, stream,
		)
		return err
	}
	log.WithFields(localLogTags).Infof(
		"Defined new consumer %s for stream %s", param.Name, stream,
	)
	return nil
}

// DeleteConsumerOnStream deletes one consumer of a stream
func (js jetStreamControllerImpl) DeleteConsumerOnStream(
	ctxt context.Context, stream, consumerName string,
) error {
	localLogTags, err := common.UpdateLogTags(ctxt, js.LogTags)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Failed to update logtags")
	}
	if err := common.ValidateTopLevelEntityName(stream, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to delete consumers on stream '%s'", stream,
		)
		return err
	}
	if err := common.ValidateTopLevelEntityName(consumerName, js.validate); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to delete consumer '%s' on stream '%s'", consumerName, stream,
		)
		return err
	}
	if err := js.core.JetStream().DeleteConsumer(stream, consumerName); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
			"Unable to delete consumer %s from stream %s", consumerName, stream,
		)
		return err
	}
	log.WithFields(localLogTags).Infof("Deleted consumer %s from stream %s", consumerName, stream)
	return nil
}
