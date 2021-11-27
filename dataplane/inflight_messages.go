package dataplane

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apex/log"
	"github.com/nats-io/nats.go"
	"gitlab.com/project-nan/httpmq/common"
)

// JetStreamInflightMsgProcessor handle inflight jetstream messages
type JetStreamInflightMsgProcessor interface {
	RecordInflightMessage(msg *nats.Msg, blocking bool, callCtxt context.Context) error
	HandlerMsgACK(ack AckIndication, blocking bool, callCtxt context.Context) error
}

// perConsumerInflightMessages set of messages awaiting ACK for a consumer
type perConsumerInflightMessages struct {
	inflight map[uint64]*nats.Msg
}

// perQueueInflightMessages set of perConsumerInflightMessages for each consumer
type perQueueInflightMessages struct {
	consumers map[string]*perConsumerInflightMessages
}

// jetStreamInflightMsgProcessorImpl implements JetStreamInflightMsgProcessor
type jetStreamInflightMsgProcessorImpl struct {
	common.Component
	subject, consumer string
	tp                common.TaskProcessor
	inflightPerQueue  map[string]*perQueueInflightMessages
}

// GetJetStreamInflightMsgProcessor define new JetStreamInflightMsgProcessor
func GetJetStreamInflightMsgProcessor(
	tp common.TaskProcessor, subject, consumer string,
) (JetStreamInflightMsgProcessor, error) {
	logTags := log.Fields{
		"module":    "dataplane",
		"component": "js-inflight-msg-holdling",
		"instance":  fmt.Sprintf("%s@%s", consumer, subject),
	}
	instance := jetStreamInflightMsgProcessorImpl{
		Component:        common.Component{LogTags: logTags},
		subject:          subject,
		consumer:         consumer,
		tp:               tp,
		inflightPerQueue: make(map[string]*perQueueInflightMessages),
	}
	// Add handlers
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(jsInflightCtrlRecordNewMsg{}),
		instance.processInflightMessage,
	); err != nil {
		return nil, err
	}
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(jsInflightCtrlRecordACK{}),
		instance.processMsgACK,
	); err != nil {
		return nil, err
	}
	return &instance, nil
}

// =========================================================================

type jsInflightCtrlRecordNewMsg struct {
	timestamp time.Time
	message   *nats.Msg
	resultCB  func(err error)
}

// RecordInflightMessage record new inflight messages
func (c *jetStreamInflightMsgProcessorImpl) RecordInflightMessage(
	msg *nats.Msg, blocking bool, callCtxt context.Context,
) error {
	resultChan := make(chan error)
	handler := func(err error) {
		resultChan <- err
	}

	request := jsInflightCtrlRecordNewMsg{
		timestamp: time.Now(),
		message:   msg,
		resultCB:  handler,
	}

	if err := c.tp.Submit(request, callCtxt); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf("Failed to submit new inflight message")
		return err
	}

	// Don't wait for a response
	if !blocking {
		return nil
	}

	var err error
	// Wait for the response or timeout
	select {
	case result, ok := <-resultChan:
		if !ok {
			err = fmt.Errorf("response to new inflight message is invalid")
		} else {
			err = result
		}
	case <-callCtxt.Done():
		err = callCtxt.Err()
	}

	if err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf("Processing new inflight message failed")
	}
	return err
}

// processInflightMessage support TaskProcessor, handle jsInflightCtrlRecordNewMsg
func (c *jetStreamInflightMsgProcessorImpl) processInflightMessage(param interface{}) error {
	request, ok := param.(jsInflightCtrlRecordNewMsg)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for record inflight message",
			reflect.TypeOf(param),
		)
	}
	err := c.ProcessInflightMessage(request.message)
	request.resultCB(err)
	return err
}

// ProcessInflightMessage record new inflight messages
func (c *jetStreamInflightMsgProcessorImpl) ProcessInflightMessage(msg *nats.Msg) error {
	// Store the message based on per-consumer sequence number of the JetStream message
	meta, err := msg.Metadata()
	if err != nil {
		log.WithError(err).WithFields(c.LogTags).Error("Unable to record message")
		return err
	}
	// Sanity check the consumer name match
	if c.consumer != meta.Consumer {
		err := fmt.Errorf(
			"message expected for %s, but meta says %s", c.consumer, meta.Consumer,
		)
		log.WithError(err).WithFields(c.LogTags).Error("Unable to record message")
		return err
	}

	// Fetch the per queue records
	perQueueRecords, ok := c.inflightPerQueue[meta.Stream]
	if !ok {
		c.inflightPerQueue[meta.Stream] = &perQueueInflightMessages{
			consumers: make(map[string]*perConsumerInflightMessages),
		}
		perQueueRecords = c.inflightPerQueue[meta.Stream]
	}
	// Fetch the per consumer records
	perConsumerRecords, ok := perQueueRecords.consumers[c.consumer]
	if !ok {
		perQueueRecords.consumers[c.consumer] = &perConsumerInflightMessages{
			inflight: make(map[uint64]*nats.Msg),
		}
		perConsumerRecords = perQueueRecords.consumers[c.consumer]
	}

	perConsumerRecords.inflight[meta.Sequence.Consumer] = msg
	log.WithFields(c.LogTags).Debugf("Recorded message [%d]", meta.Sequence.Consumer)
	return nil
}

// =========================================================================

type jsInflightCtrlRecordACK struct {
	timestamp time.Time
	ack       AckIndication
	resultCB  func(err error)
}

// HandlerMsgACK handler JetStream message ACK
func (c *jetStreamInflightMsgProcessorImpl) HandlerMsgACK(
	ack AckIndication, blocking bool, callCtxt context.Context,
) error {
	resultChan := make(chan error)
	handler := func(err error) {
		resultChan <- err
	}

	request := jsInflightCtrlRecordACK{
		timestamp: time.Now(),
		ack:       ack,
		resultCB:  handler,
	}

	if err := c.tp.Submit(request, callCtxt); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf("Failed to submit msg ACK")
		return err
	}

	// Don't wait for a response
	if !blocking {
		return nil
	}

	var err error
	// Wait for the response or timeout
	select {
	case result, ok := <-resultChan:
		if !ok {
			err = fmt.Errorf("response to msg ACK is invalid")
		} else {
			err = result
		}
	case <-callCtxt.Done():
		err = callCtxt.Err()
	}

	if err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf("Processing msg ACK failed")
	}
	return err
}

// processMsgACK support TaskProcessor, handle jsInflightCtrlRecordACK
func (c *jetStreamInflightMsgProcessorImpl) processMsgACK(param interface{}) error {
	request, ok := param.(jsInflightCtrlRecordACK)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for handle new ACK message",
			reflect.TypeOf(param),
		)
	}
	err := c.ProcessMsgACK(request.ack)
	request.resultCB(err)
	return err
}

// ProcessMsgACK handler JetStream message ACK
func (c *jetStreamInflightMsgProcessorImpl) ProcessMsgACK(ack AckIndication) error {
	// Fetch the per queue records
	perQueueRecords, ok := c.inflightPerQueue[ack.Queue]
	if !ok {
		err := fmt.Errorf("no records related to queue %s", ack.Queue)
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to process %s", ack.String())
		return err
	}
	// Fetch the per consumer records
	perConsumerRecords, ok := perQueueRecords.consumers[ack.Consumer]
	if !ok {
		err := fmt.Errorf("no records related to consumer %s on queue %s", ack.Consumer, ack.Queue)
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to process %s", ack.String())
		return err
	}

	// ACK the stored message
	msg, ok := perConsumerRecords.inflight[ack.SeqNum.Consumer]
	if !ok {
		err := fmt.Errorf(
			"no records related message [%d] for %s@%s", ack.SeqNum.Consumer, ack.Consumer, ack.Queue,
		)
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to process %s", ack.String())
		return err
	}
	if err := msg.AckSync(); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to process %s", ack.String())
		return err
	}
	delete(perConsumerRecords.inflight, ack.SeqNum.Consumer)
	return nil
}
