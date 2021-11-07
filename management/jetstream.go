package management

import (
	"context"
	"time"

	"github.com/apex/log"
	"github.com/nats-io/nats.go"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/core"
)

// JetStreamController manage JetStream
type JetStreamController struct {
	common.Component
	core core.JetStream
}

// GetJetStreamController define GetJetStreamController
func GetJetStreamController(
	jsCore core.JetStream, instance string,
) (JetStreamController, error) {
	logTags := log.Fields{
		"module":    "management",
		"component": "jetstream",
		"instance":  instance,
	}
	return JetStreamController{
		Component: common.Component{LogTags: logTags},
		core:      jsCore,
	}, nil
}

// GetAllQueues fetch the list all known queue
func (js JetStreamController) GetAllQueues(ctxt context.Context) map[string]*nats.StreamInfo {
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
			knownQueues[info.Config.Name] = info
		case <-ctxt.Done():
			// out of time
			readAll = true
		}
	}
	return knownQueues
}

// GetQueue get info on one queue
func (js JetStreamController) GetQueue(name string) (*nats.StreamInfo, error) {
	info, err := js.core.JetStream().StreamInfo(name)
	if err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to get queue %s info", name)
	}
	return info, err
}

// GetBasicQueueParam generate a basic queue setting object
func (js JetStreamController) GetBasicQueueParam(
	name string, topics []string, maxMessageAge time.Duration,
) nats.StreamConfig {
	return nats.StreamConfig{Name: name, Subjects: topics, MaxAge: maxMessageAge}
}

// CreateQueue define a new queue
func (js JetStreamController) CreateQueue(param nats.StreamConfig) error {
	if _, err := js.core.JetStream().AddStream(&param); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Unable to define new queue %s", param.Name,
		)
		return err
	}
	log.WithFields(js.LogTags).Infof("Defined new queue %s", param.Name)
	return nil
}

// DeleteQueue delete an existing queue
func (js JetStreamController) DeleteQueue(name string) error {
	if err := js.core.JetStream().DeleteStream(name); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("Unable to delete queue %s", name)
		return err
	}
	log.WithFields(js.LogTags).Infof("Deleted queue %s", name)
	return nil
}

// UpdateQueueSetting update a queue's setting
func (js JetStreamController) UpdateQueueSetting(param nats.StreamConfig) error {
	if _, err := js.core.JetStream().UpdateStream(&param); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf(
			"Failed to update queue %s params", param.Name,
		)
		return err
	}
	log.WithFields(js.LogTags).Errorf("Updated queue %s params", param.Name)
	return nil
}
