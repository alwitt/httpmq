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

// perStreamInflightMessages set of perConsumerInflightMessages for each consumer
type perStreamInflightMessages struct {
	consumers map[string]*perConsumerInflightMessages
}

// jetStreamInflightMsgProcessorImpl implements JetStreamInflightMsgProcessor
type jetStreamInflightMsgProcessorImpl struct {
	common.Component
	subject, consumer string
	tp                common.TaskProcessor
	inflightPerStream map[string]*perStreamInflightMessages
}

// getJetStreamInflightMsgProcessor define new JetStreamInflightMsgProcessor
func getJetStreamInflightMsgProcessor(
	tp common.TaskProcessor, stream, subject, consumer string, ctxt context.Context,
) (JetStreamInflightMsgProcessor, error) {
	logTags := log.Fields{
		"module":    "dataplane",
		"component": "js-inflight-msg-holdling",
		"stream":    stream,
		"subject":   subject,
		"consumer":  consumer,
	}
	if ctxt.Value(common.RequestID{}) != nil {
		logTags["request"] = ctxt.Value(common.RequestID{}).(string)
	}
	instance := jetStreamInflightMsgProcessorImpl{
		Component:         common.Component{LogTags: logTags},
		subject:           subject,
		consumer:          consumer,
		tp:                tp,
		inflightPerStream: make(map[string]*perStreamInflightMessages),
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
	blocking  bool
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
		blocking:  blocking,
		message:   msg,
		resultCB:  handler,
	}

	if err := c.tp.Submit(request, callCtxt); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf("Failed to submit %s", msgToString(msg))
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
		log.WithError(err).WithFields(c.LogTags).Errorf("Processing %s failed", msgToString(msg))
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
	if request.blocking {
		request.resultCB(err)
	}
	return err
}

// ProcessInflightMessage record new inflight messages
func (c *jetStreamInflightMsgProcessorImpl) ProcessInflightMessage(msg *nats.Msg) error {
	// Store the message based on per-consumer sequence number of the JetStream message
	meta, err := msg.Metadata()
	if err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to record %s", msgToString(msg))
		return err
	}
	// Sanity check the consumer name match
	if c.consumer != meta.Consumer {
		err := fmt.Errorf(
			"message expected for %s, but meta says %s", c.consumer, meta.Consumer,
		)
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to record %s", msgToString(msg))
		return err
	}

	// Fetch the per stream records
	perStreamRecords, ok := c.inflightPerStream[meta.Stream]
	if !ok {
		c.inflightPerStream[meta.Stream] = &perStreamInflightMessages{
			consumers: make(map[string]*perConsumerInflightMessages),
		}
		perStreamRecords = c.inflightPerStream[meta.Stream]
	}
	// Fetch the per consumer records
	perConsumerRecords, ok := perStreamRecords.consumers[c.consumer]
	if !ok {
		perStreamRecords.consumers[c.consumer] = &perConsumerInflightMessages{
			inflight: make(map[uint64]*nats.Msg),
		}
		perConsumerRecords = perStreamRecords.consumers[c.consumer]
	}

	perConsumerRecords.inflight[meta.Sequence.Consumer] = msg
	log.WithFields(c.LogTags).Debugf("Recorded %s", msgToString(msg))
	return nil
}

// =========================================================================

type jsInflightCtrlRecordACK struct {
	timestamp time.Time
	blocking  bool
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
		blocking:  blocking,
		ack:       ack,
		resultCB:  handler,
	}

	if err := c.tp.Submit(request, callCtxt); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf("Failed to submit %s", ack.String())
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
		log.WithError(err).WithFields(c.LogTags).Errorf("Processing %s failed", ack.String())
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
	if request.blocking {
		request.resultCB(err)
	}
	return err
}

// ProcessMsgACK handler JetStream message ACK
func (c *jetStreamInflightMsgProcessorImpl) ProcessMsgACK(ack AckIndication) error {
	// Fetch the per stream records
	perStreamRecords, ok := c.inflightPerStream[ack.Stream]
	if !ok {
		err := fmt.Errorf("no records related to stream %s", ack.Stream)
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to process %s", ack.String())
		return err
	}
	// Fetch the per consumer records
	perConsumerRecords, ok := perStreamRecords.consumers[ack.Consumer]
	if !ok {
		err := fmt.Errorf("no records related to consumer %s on stream %s", ack.Consumer, ack.Stream)
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to process %s", ack.String())
		return err
	}

	// ACK the stored message
	msg, ok := perConsumerRecords.inflight[ack.SeqNum.Consumer]
	if !ok {
		err := fmt.Errorf(
			"no records related message [%d] for %s@%s", ack.SeqNum.Consumer, ack.Consumer, ack.Stream,
		)
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to process %s", ack.String())
		return err
	}
	if err := msg.AckSync(); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf("Unable to process %s", ack.String())
		return err
	}
	delete(perConsumerRecords.inflight, ack.SeqNum.Consumer)
	log.WithFields(c.LogTags).Debugf("Cleaned up based on %s", ack.String())
	return nil
}
