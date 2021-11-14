package management

import (
	"context"
	"encoding/json"
	"time"

	"github.com/apex/log"
	"github.com/nats-io/nats.go"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/core"
)

// JetStreamQueueLimits list queue limits
type JetStreamQueueLimits struct {
	MaxConsumers      *int           `json:"max_consumers,omitempty"`
	MaxMsgs           *int64         `json:"max_msgs,omitempty"`
	MaxBytes          *int64         `json:"max_bytes,omitempty"`
	MaxAge            *time.Duration `json:"max_age,omitempty"`
	MaxMsgsPerSubject *int64         `json:"max_msgs_per_subject,omitempty"`
	MaxMsgSize        *int32         `json:"max_msg_size,omitempty"`
}

// JetStreamQueueParam list parameters for defining a queue
type JetStreamQueueParam struct {
	// Name is the queue name
	Name     string   `json:"name" validate:"required"`
	Subjects []string `json:"subjects,omitempty"`
	JetStreamQueueLimits
}

// JetStreamController manage JetStream
type JetStreamController interface {
	CreateQueue(param JetStreamQueueParam) error
	GetAllQueues(ctxt context.Context) map[string]*nats.StreamInfo
	GetQueue(name string) (*nats.StreamInfo, error)
	ChangeQueueSubjects(queue string, newSubjects []string) error
	UpdateQueueLimits(
		queue string, newLimits JetStreamQueueLimits,
	) error
	DeleteQueue(name string) error
}

// jetStreamControllerImpl manage JetStream
type jetStreamControllerImpl struct {
	common.Component
	core core.JetStream
}

// GetJetStreamController define JetStreamController
func GetJetStreamController(
	jsCore core.JetStream, instance string,
) (JetStreamController, error) {
	logTags := log.Fields{
		"module":    "management",
		"component": "jetstream",
		"instance":  instance,
	}
	return jetStreamControllerImpl{
		Component: common.Component{LogTags: logTags},
		core:      jsCore,
	}, nil
}

// =======================================================================
// Queue related controls

// GetAllQueues fetch the list of all known queue
func (js jetStreamControllerImpl) GetAllQueues(ctxt context.Context) map[string]*nats.StreamInfo {
	readChan := js.core.JetStream().StreamsInfo()
	knownQueues := map[string]*nats.StreamInfo{}
	readAll := false
	for !readAll {
		select {
		case info, ok := <-readChan:
			if !ok || info == nil {
				log.WithFields(js.LogTags).Errorf("Stream info chan read failure")
				readAll = true
				break
			}
			if _, ok := knownQueues[info.Config.Name]; ok {
				log.WithFields(js.LogTags).Errorf(
					"Stream info contain multiple entry of %s", info.Config.Name,
				)
			}
			knownQueues[info.Config.Name] = info
		case <-ctxt.Done():
			// out of time
			readAll = true
		}
	}
	return knownQueues
}

// GetQueue get info on one queue
func (js jetStreamControllerImpl) GetQueue(name string) (*nats.StreamInfo, error) {
	info, err := js.core.JetStream().StreamInfo(name)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to get queue %s info", name)
	}
	return info, err
}

func applyStreamLimits(targetLimit *JetStreamQueueLimits, param *nats.StreamConfig) {
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

// CreateQueue define a new queue
func (js jetStreamControllerImpl) CreateQueue(param JetStreamQueueParam) error {
	// Convert to JetStream structure
	jsParams := nats.StreamConfig{
		Name:     param.Name,
		Subjects: param.Subjects,
	}
	applyStreamLimits(&param.JetStreamQueueLimits, &jsParams)
	if _, err := js.core.JetStream().AddStream(&jsParams); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new queue %s", param.Name,
		)
		return err
	}
	log.WithFields(js.LogTags).Infof("Defined new queue %s", param.Name)
	return nil
}

// DeleteQueue delete an existing queue
func (js jetStreamControllerImpl) DeleteQueue(name string) error {
	if err := js.core.JetStream().DeleteStream(name); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to delete queue %s", name)
		return err
	}
	log.WithFields(js.LogTags).Infof("Deleted queue %s", name)
	return nil
}

// ChangeQueueSubjects change the set of subjects the queue collects for
func (js jetStreamControllerImpl) ChangeQueueSubjects(queue string, newSubjects []string) error {
	info, err := js.core.JetStream().StreamInfo(queue)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to get queue %s info", queue)
		return err
	}
	currentConfig := info.Config
	currentConfig.Subjects = newSubjects
	_, err = js.core.JetStream().UpdateStream(&currentConfig)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to change queue %s subjects", queue,
		)
	} else {
		t, _ := json.Marshal(newSubjects)
		log.WithFields(js.LogTags).Errorf("Change queue %s subjects to %s", queue, t)
	}
	return err
}

// UpdateQueueLimits update a queue's data retention limits
func (js jetStreamControllerImpl) UpdateQueueLimits(
	queue string, newLimits JetStreamQueueLimits,
) error {
	info, err := js.core.JetStream().StreamInfo(queue)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to get queue %s info", queue)
		return err
	}
	currentConfig := info.Config
	applyStreamLimits(&newLimits, &currentConfig)
	if _, err := js.core.JetStream().UpdateStream(&currentConfig); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Failed to update queue %s retention limits", queue,
		)
		return err
	}
	log.WithFields(js.LogTags).Errorf("Updated queue %s retention limits", queue)
	return nil
}

// =======================================================================
// Consumer related controls

// GetAllConsumersOfQueue fetch the list of all consumers known consumer of a queue
func (js jetStreamControllerImpl) GetAllConsumersOfQueue(
	queue string, ctxt context.Context,
) map[string]*nats.ConsumerInfo {
	readChan := js.core.JetStream().ConsumersInfo(queue)
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

// GetConsumerOfQueue get info on one consumer of a queue
func (js jetStreamControllerImpl) GetConsumerOfQueue(queue, consumer string) (
	*nats.ConsumerInfo, error,
) {
	info, err := js.core.JetStream().ConsumerInfo(queue, consumer)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to get consumer %s of queue %s info", consumer, queue,
		)
	}
	return info, err
}

// CreateConsumer define a new consumer for a queue
func (js jetStreamControllerImpl) CreateConsumerForQueue(
	queue string, param nats.ConsumerConfig,
) error {
	if _, err := js.core.JetStream().AddConsumer(queue, &param); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new consumer %s for queue %s", param.Durable, queue,
		)
		return err
	}
	log.WithFields(js.LogTags).Infof("Defined new consumer %s for queue %s", param.Durable, queue)
	return nil
}

// DeleteConsumerFromQueue delete consumer from a queue
func (js jetStreamControllerImpl) DeleteConsumerFromQueue(queue, consumer string) error {
	if err := js.core.JetStream().DeleteConsumer(queue, consumer); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to delete consumer %s from queue %s", consumer, queue,
		)
		return err
	}
	log.WithFields(js.LogTags).Infof("Deleted consumer %s from queue %s", consumer, queue)
	return nil
}
