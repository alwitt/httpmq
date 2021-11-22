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

// JetStreamQueueLimits list queue data retention settings
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

// JetStreamConsumerParam list parameters for defining a consumer
type JetStreamConsumerParam struct {
	Name  string `json:"name" validate:"required"`
	Notes string `json:"notes,omitempty"`
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
	// Queue related management
	// CreateQueue create a new JetStream queue given parameters
	CreateQueue(param JetStreamQueueParam) error
	// GetAllQueues query for info on all available JetStream queue
	GetAllQueues(ctxt context.Context) map[string]*nats.StreamInfo
	// GetQueue query for info on one JetStream queue by name
	GetQueue(name string) (*nats.StreamInfo, error)
	// ChangeQueueSubjects changes the target subjects of a JetStream queue
	ChangeQueueSubjects(queue string, newSubjects []string) error
	// UpdateQueueLimits change the data retention limits of the JetStream queue
	UpdateQueueLimits(
		queue string, newLimits JetStreamQueueLimits,
	) error
	// DeleteQueue delete a JetStream queue by name
	DeleteQueue(name string) error
	// ========================================================
	// Consumer related management
	// CreateConsumerForQueue create a new consumer on a JetStream queue
	CreateConsumerForQueue(queue string, param JetStreamConsumerParam) error
	// GetAllConsumersForQueue query for info on all consumers of a JetStream queue
	GetAllConsumersForQueue(
		queue string, ctxt context.Context,
	) map[string]*nats.ConsumerInfo
	// GetConsumerForQueue query for info of a consumer of a JetStream queue
	GetConsumerForQueue(queue, consumerName string) (*nats.ConsumerInfo, error)
	// DeleteConsumerOnQueue delete consumer of a JetSteam queue
	DeleteConsumerOnQueue(queue, consumerName string) error
}

// jetStreamControllerImpl manage JetStream
type jetStreamControllerImpl struct {
	common.Component
	core     core.NatsClient
	validate *validator.Validate
}

// GetJetStreamController define JetStreamController
func GetJetStreamController(
	natsCore core.NatsClient, instance string,
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

// GetAllConsumersForQueue fetch the list of all consumers known consumer of a queue
func (js jetStreamControllerImpl) GetAllConsumersForQueue(
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

// GetConsumerForQueue get info on one consumer of a queue
func (js jetStreamControllerImpl) GetConsumerForQueue(
	queue, consumerName string,
) (*nats.ConsumerInfo, error) {
	info, err := js.core.JetStream().ConsumerInfo(queue, consumerName)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to get consumer %s of queue %s info", consumerName, queue,
		)
	}
	return info, err
}

// CreateConsumerForQueue define a new consumer for a queue
func (js jetStreamControllerImpl) CreateConsumerForQueue(
	queue string, param JetStreamConsumerParam,
) error {
	// Verify the parameters are acceptable
	if err := js.validate.Struct(&param); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new consumer %s for queue %s", param.Name, queue,
		)
		return err
	}
	// Convert to JetStream structure
	jsParams := nats.ConsumerConfig{
		Durable:       param.Name,
		Description:   param.Notes,
		MaxDeliver:    param.MaxInflight,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
	}
	// Verify the configuration made sense
	if param.Mode == "pull" && param.DeliveryGroup != nil {
		err := fmt.Errorf("pull consumer can't use delivery group")
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new consumer %s for queue %s", param.Name, queue,
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
	// Define the consumer
	if _, err := js.core.JetStream().AddConsumer(queue, &jsParams); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new consumer %s for queue %s", param.Name, queue,
		)
		return err
	}
	log.WithFields(js.LogTags).Infof(
		"Defined new consumer %s for queue %s", param.Name, queue,
	)
	return nil
}

// DeleteConsumerOnQueue delete consumer from a queue
func (js jetStreamControllerImpl) DeleteConsumerOnQueue(queue, consumerName string) error {
	if err := js.core.JetStream().DeleteConsumer(queue, consumerName); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to delete consumer %s from queue %s", consumerName, queue,
		)
		return err
	}
	log.WithFields(js.LogTags).Infof("Deleted consumer %s from queue %s", consumerName, queue)
	return nil
}
