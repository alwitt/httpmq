package dispatch

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apex/log"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/storage"
)

// ========================================================================================
// MessageRetransmit retransmit older messages from a queue
type MessageRetransmit interface {
	StopOperation() error
	RetransmitMessages(msgIndexes []int64, ctxt context.Context) error
	ReceivedACKs(msgIndexes []int64, ctxt context.Context) error
}

// messageRetransmitImpl implements MessageRetransmit
type messageRetransmitImpl struct {
	common.Component
	queueName        string
	tp               common.TaskProcessor
	storage          storage.MessageQueues
	readTimeout      time.Duration
	forwardMsg       SubmitMessage
	operationContext context.Context
	contextCancel    context.CancelFunc
	msgCache         map[int64]MessageInFlight
}

// DefineMessageRetransmit create new message retransmit module
func DefineMessageRetransmit(
	queueName string,
	tp common.TaskProcessor,
	storage storage.MessageQueues,
	storeReadTO time.Duration,
	forwardCB SubmitMessage,
	rootCtxt context.Context,
) (MessageRetransmit, error) {
	logTags := log.Fields{
		"module": "dispatch", "component": "message-retransmit", "queue": queueName,
	}
	ctxt, cancel := context.WithCancel(rootCtxt)
	instance := messageRetransmitImpl{
		Component:        common.Component{LogTags: logTags},
		queueName:        queueName,
		tp:               tp,
		storage:          storage,
		readTimeout:      storeReadTO,
		forwardMsg:       forwardCB,
		operationContext: ctxt,
		contextCancel:    cancel,
		msgCache:         make(map[int64]MessageInFlight),
	}
	// Add handlers
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(msgRetransmitterRetransMsgReq{}),
		instance.processRetransmitRequest,
	); err != nil {
		return nil, err
	}
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(msgRetransmitterACKMsgReq{}),
		instance.processACKMsgRequest,
	); err != nil {
		return nil, err
	}
	return &instance, nil
}

// StopOperation stop forwarding message
func (d *messageRetransmitImpl) StopOperation() error {
	d.contextCancel()
	return nil
}

// ----------------------------------------------------------------------------------------

type msgRetransmitterRetransMsgReq struct {
	indexes  []int64
	resultCB func(error)
}

// RetransmitMessages retransmit the messages requested
func (r *messageRetransmitImpl) RetransmitMessages(
	msgIndexes []int64, ctxt context.Context,
) error {
	complete := make(chan bool, 1)
	var processError error
	handler := func(err error) {
		processError = err
		complete <- true
	}

	// Make the request
	request := msgRetransmitterRetransMsgReq{
		indexes:  msgIndexes,
		resultCB: handler,
	}

	if err := r.tp.Submit(request, ctxt); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to submit retransmit-messages request",
		)
		return err
	}

	// Wait for completion
	select {
	case <-complete:
		break
	case <-ctxt.Done():
		return ctxt.Err()
	}

	return processError
}

func (r *messageRetransmitImpl) processRetransmitRequest(param interface{}) error {
	request, ok := param.(msgRetransmitterRetransMsgReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for retransmit request",
			reflect.TypeOf(param),
		)
	}
	err := r.ProcessRetransmitRequest(request.indexes)
	request.resultCB(err)
	return err
}

// ProcessRetransmitRequest retransmit the messages requested
func (r *messageRetransmitImpl) ProcessRetransmitRequest(msgIndexes []int64) error {
	// Re-transmission can will messages from cache first, if not pull from storage

	for _, msgIndex := range msgIndexes {
		msg, ok := r.msgCache[msgIndex]
		if ok {
			// Sending from cache
			log.WithFields(r.LogTags).Debugf(
				"Using cached %s@%d for retransmit", r.queueName, msgIndex,
			)
		} else {
			// Pull from storage
			useContext, cancel := context.WithTimeout(context.Background(), r.readTimeout)
			defer cancel()
			coreMsg, err := r.storage.Read(r.queueName, msgIndex, useContext)
			if err != nil {
				log.WithError(err).WithFields(r.LogTags).Errorf(
					"Failed to read %s@%d from storage", r.queueName, msgIndex,
				)
				continue
			}
			msg = MessageInFlight{
				Message:    coreMsg,
				Index:      msgIndex,
				Redelivery: true,
			}
			r.msgCache[msgIndex] = msg
			log.WithFields(r.LogTags).Debugf(
				"Read %s@%d for retransmit", r.queueName, msgIndex,
			)
		}
		if err := r.forwardMsg(msg, r.operationContext); err != nil {
			log.WithError(err).WithFields(r.LogTags).Errorf(
				"Failed to submit %s@%d for retransmit", r.queueName, msgIndex,
			)
			return err
		} else {
			log.WithFields(r.LogTags).Infof("Retransmitted %s@%d", r.queueName, msgIndex)
		}
	}

	return nil
}

// ----------------------------------------------------------------------------------------

type msgRetransmitterACKMsgReq struct {
	indexes  []int64
	resultCB func(error)
}

// ReceivedACKs received ACK for certain messages
func (r *messageRetransmitImpl) ReceivedACKs(msgIndexes []int64, ctxt context.Context) error {
	complete := make(chan bool, 1)
	var processError error
	handler := func(err error) {
		processError = err
		complete <- true
	}

	// Make the request
	request := msgRetransmitterACKMsgReq{
		indexes:  msgIndexes,
		resultCB: handler,
	}

	if err := r.tp.Submit(request, ctxt); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to submit acked-messages request",
		)
		return err
	}

	// Wait for completion
	select {
	case <-complete:
		break
	case <-ctxt.Done():
		return ctxt.Err()
	}

	return processError
}

func (r *messageRetransmitImpl) processACKMsgRequest(param interface{}) error {
	request, ok := param.(msgRetransmitterACKMsgReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for ACK msg request",
			reflect.TypeOf(param),
		)
	}
	err := r.ProcessACKMsgRequest(request.indexes)
	request.resultCB(err)
	return err
}

// ReceivedACKs received ACK for certain messages
func (r *messageRetransmitImpl) ProcessACKMsgRequest(msgIndexes []int64) error {
	// For the message which were ACKed, clear them from cache

	for _, msgIndex := range msgIndexes {
		delete(r.msgCache, msgIndex)
		log.WithFields(r.LogTags).Infof(
			"Delete from cache %s@%d. Its ACKed", r.queueName, msgIndex,
		)
	}

	return nil
}
