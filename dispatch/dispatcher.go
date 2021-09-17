package dispatch

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apex/log"
	"gitlab.com/project-nan/httpmq/common"
)

// ========================================================================================
// MessageDispatch dispatch messages from back-end store while accounting for flow control
type MessageDispatch interface {
	ProcessMessages(useContext context.Context) error
	StopOperation() error
	SubmitMessageACK(ackIndexes []int64, useContext context.Context) error
	SubmitRetransmit(msg MessageInFlight, useContext context.Context) error
	SubmitMessageToDeliver(msg MessageInFlight, useContext context.Context) error
}

// messageDispatchImpl implements MessageDispatch
type messageDispatchImpl struct {
	common.Component
	queueName        string
	tp               common.TaskProcessor
	ackQueue         chan []int64
	retry            chan MessageInFlight
	deliver          chan MessageInFlight
	inflightMsgs     int
	maxInflight      int
	forwardMsg       SubmitMessage
	stopOperation    bool
	operationContext context.Context
	contextCancel    context.CancelFunc
	registerInflight RegisterInflightMessage
}

// DefineMessageDispatch create new message dispatch module
func DefineMessageDispatch(
	queueName string,
	tp common.TaskProcessor,
	ack chan []int64,
	retry chan MessageInFlight,
	deliver chan MessageInFlight,
	alreadyInflightMsgs int,
	maxInflightMsgs int,
	forwardCB SubmitMessage,
	registerMsgCB RegisterInflightMessage,
	rootCtxt context.Context,
) (MessageDispatch, error) {
	logTags := log.Fields{
		"module": "dispatch", "component": "message-dispatch", "queue": queueName,
	}
	initialInflight := 0
	if alreadyInflightMsgs > 0 {
		initialInflight = alreadyInflightMsgs
	}
	ctxt, cancel := context.WithCancel(rootCtxt)
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
		stopOperation:    false,
		operationContext: ctxt,
		contextCancel:    cancel,
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

// StopOperation stop forwarding message
func (d *messageDispatchImpl) StopOperation() error {
	d.stopOperation = true
	d.contextCancel()
	return nil
}

// ----------------------------------------------------------------------------------------

type msgDispatcherProcessReq struct{}

// ProcessMessages trigger the event processing loop for the dispatch
func (d *messageDispatchImpl) ProcessMessages(useContext context.Context) error {
	if err := d.tp.Submit(msgDispatcherProcessReq{}, useContext); err != nil {
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

	currentTime := time.Now()

	// Check through all the ACKs
	{
		readAll := false
		for !readAll {
			select {
			case ackMsg := <-d.ackQueue:
				d.inflightMsgs -= len(ackMsg)
				if d.inflightMsgs < 0 {
					d.inflightMsgs = 0
				}
				log.WithFields(d.LogTags).Debugf("Message %s@%v is ACKed", d.queueName, ackMsg)
			default:
				readAll = true
			}
		}
	}

	// Process retransmission if possible
	{
		readAll := false
		for !readAll {
			select {
			case msg := <-d.retry:
				if err := d.forwardMsg(msg, d.operationContext); err != nil {
					log.WithError(err).WithFields(d.LogTags).Errorf(
						"Failed to re-transmit %s@%d", d.queueName, msg.Index,
					)
					if d.stopOperation {
						log.WithFields(d.LogTags).Info("Stopping all message forwarding")
						return err
					}
				}
				log.WithFields(d.LogTags).Debugf("Re-transmitted %s@%d", d.queueName, msg.Index)
			default:
				readAll = true
			}
		}
	}

	// Process new message delivery if possible
	{
		readAll := false
		for !readAll && d.inflightMsgs < d.maxInflight {
			select {
			case msg := <-d.deliver:
				if err := d.forwardMsg(msg, d.operationContext); err != nil {
					log.WithError(err).WithFields(d.LogTags).Errorf(
						"Failed to send %s@%d", d.queueName, msg.Index,
					)
					if d.stopOperation {
						log.WithFields(d.LogTags).Info("Stopping all message forwarding")
						return err
					}
				}
				log.WithFields(d.LogTags).Debugf("Sent %s@%d", d.queueName, msg.Index)
				d.inflightMsgs += 1
				if err := d.registerInflight(msg.Index, currentTime, d.operationContext); err != nil {
					log.WithError(err).WithFields(d.LogTags).Errorf(
						"Unable to register transmit of %s@%d", d.queueName, msg.Index,
					)
					if d.stopOperation {
						log.WithFields(d.LogTags).Info("Stopping all message forwarding")
						return err
					}
				}
			default:
				readAll = true
			}
		}
	}

	return nil
}

// ----------------------------------------------------------------------------------------

// SubmitMessageACK submit a message ID which was just ACKed
func (d *messageDispatchImpl) SubmitMessageACK(
	ackNum []int64, useContext context.Context,
) error {
	select {
	case d.ackQueue <- ackNum:
		break
	case <-useContext.Done():
		return useContext.Err()
	}
	return d.ProcessMessages(useContext)
}

// ----------------------------------------------------------------------------------------

// SubmitRetransmit submit a message for retransmission
func (d *messageDispatchImpl) SubmitRetransmit(
	msg MessageInFlight, useContext context.Context,
) error {
	msg.Redelivery = true
	select {
	case d.retry <- msg:
		break
	case <-useContext.Done():
		return useContext.Err()
	}
	return d.ProcessMessages(useContext)
}

// ----------------------------------------------------------------------------------------

// SubmitMessageToDeliver submit a message for delivery
func (d *messageDispatchImpl) SubmitMessageToDeliver(
	msg MessageInFlight, useContext context.Context,
) error {
	msg.Redelivery = false
	select {
	case d.deliver <- msg:
		break
	case <-useContext.Done():
		return useContext.Err()
	}
	return d.ProcessMessages(useContext)
}
