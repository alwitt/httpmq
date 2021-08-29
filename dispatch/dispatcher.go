package dispatch

import (
	"fmt"
	"reflect"

	"github.com/apex/log"
	"gitlab.com/project-nan/httpmq/common"
)

// ========================================================================================
// MessageDispatch dispatch messages from back-end store while accounting for flow control
type MessageDispatch interface {
	ProcessMessages() error
	SubmitMessageACK(ackNum int64) error
	SubmitRetransmit(msg MessageInFlight) error
	SubmitMessageToDeliver(msg MessageInFlight) error
}

// messageDispatchImpl implements MessageDispatch
type messageDispatchImpl struct {
	common.Component
	queueName        string
	tp               common.TaskProcessor
	ackQueue         chan int64
	retry            chan MessageInFlight
	deliver          chan MessageInFlight
	inflightMsgs     int
	maxInflight      int
	forwardMsg       SubmitMessage
	registerInflight registerInflightMessage
}

// DefineMessageDispatch create new message dispatch module
func DefineMessageDispatch(
	queueName string,
	tp common.TaskProcessor,
	ack chan int64,
	retry chan MessageInFlight,
	deliver chan MessageInFlight,
	alreadyInflightMsgs int,
	maxInflightMsgs int,
	forwardCB SubmitMessage,
	registerMsgCB registerInflightMessage,
) (MessageDispatch, error) {
	logTags := log.Fields{
		"module": "dispatch", "component": "message-dispatch", "instance": queueName,
	}
	initialInflight := 0
	if alreadyInflightMsgs > 0 {
		initialInflight = alreadyInflightMsgs
	}
	instance := messageDispatchImpl{
		Component:        common.Component{LogTags: logTags},
		queueName:        queueName,
		tp:               tp,
		ackQueue:         ack,
		retry:            retry,
		deliver:          deliver,
		inflightMsgs:     initialInflight,
		maxInflight:      maxInflightMsgs,
		forwardMsg:       forwardCB,
		registerInflight: registerMsgCB,
	}
	// Add handlers
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(msgDispatcherProcessReq{}), instance.processProcessRequest,
	); err != nil {
		return nil, err
	}
	return &instance, nil
}

// ----------------------------------------------------------------------------------------

type msgDispatcherProcessReq struct{}

// ProcessMessages trigger the event processing loop for the dispatch
func (d *messageDispatchImpl) ProcessMessages() error {
	if err := d.tp.Submit(msgDispatcherProcessReq{}); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to submit process request")
		return err
	}
	return nil
}

func (d *messageDispatchImpl) processProcessRequest(param interface{}) error {
	_, ok := param.(msgDispatcherProcessReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for process request",
			reflect.TypeOf(param),
		)
	}
	return d.ProcessProcessRequest()
}

// ProcessProcessRequest trigger the event processing loop for the dispatch
func (d *messageDispatchImpl) ProcessProcessRequest() error {
	// Processing sequence
	// * Check msg ACKs
	// * Process retransmission if there is room left for in-flight messages
	// * Process normal message if there is room left for in-flight messages
	//   * Signal that these message were just sent

	// Check through all the ACKs
	{
		readAll := false
		for !readAll {
			select {
			case ackMsg := <-d.ackQueue:
				if d.inflightMsgs > 0 {
					d.inflightMsgs -= 1
				}
				log.WithFields(d.LogTags).Debugf("Message %d is ACKed", ackMsg)
			default:
				readAll = true
			}
		}
		log.WithFields(d.LogTags).Debugf("Processed all available ACK messages")
	}

	log.WithFields(d.LogTags).Debugf("Messages inflight for %s: %d", d.queueName, d.inflightMsgs)

	// Process retransmission if possible
	{
		readAll := false
		for !readAll {
			select {
			case msg := <-d.retry:
				if err := d.forwardMsg(msg); err != nil {
					log.WithError(err).WithFields(d.LogTags).Errorf(
						"Failed to re-transmit %s@%d", d.queueName, msg.Index,
					)
				} else {
					log.WithFields(d.LogTags).Debugf("Re-transmitted %s@%d", d.queueName, msg.Index)
				}
			default:
				readAll = true
			}
		}
		log.WithFields(d.LogTags).Debug("Handled re-transmit messages")
	}

	log.WithFields(d.LogTags).Debugf("Messages inflight for %s: %d", d.queueName, d.inflightMsgs)

	// Process new message delivery if possible
	{
		readAll := false
		for !readAll && d.inflightMsgs < d.maxInflight {
			select {
			case msg := <-d.deliver:
				if err := d.forwardMsg(msg); err != nil {
					log.WithError(err).WithFields(d.LogTags).Errorf(
						"Failed to send %s@%d", d.queueName, msg.Index,
					)
				} else {
					log.WithFields(d.LogTags).Debugf("Sent %s@%d", d.queueName, msg.Index)
				}
				d.inflightMsgs += 1
				if err := d.registerInflight(msg.Index); err != nil {
					log.WithError(err).WithFields(d.LogTags).Errorf(
						"Unable to register transmit of %s@%d", d.queueName, msg.Index,
					)
					return err
				}
			default:
				readAll = true
			}
		}
		log.WithFields(d.LogTags).Debug("Handled new messages")
	}

	log.WithFields(d.LogTags).Debugf("Messages inflight for %s: %d", d.queueName, d.inflightMsgs)

	return nil
}

// ----------------------------------------------------------------------------------------

// SubmitMessageACK submit a message ID which was just ACKed
func (d *messageDispatchImpl) SubmitMessageACK(ackNum int64) error {
	d.ackQueue <- ackNum
	return d.ProcessMessages()
}

// ----------------------------------------------------------------------------------------

// SubmitRetransmit submit a message for retransmission
func (d *messageDispatchImpl) SubmitRetransmit(msg MessageInFlight) error {
	msg.Redelivery = true
	d.retry <- msg
	return d.ProcessMessages()
}

// ----------------------------------------------------------------------------------------

// SubmitMessageToDeliver submit a message for delivery
func (d *messageDispatchImpl) SubmitMessageToDeliver(msg MessageInFlight) error {
	msg.Redelivery = false
	d.deliver <- msg
	return d.ProcessMessages()
}
