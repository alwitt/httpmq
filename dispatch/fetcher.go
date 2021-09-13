package dispatch

import (
	"context"
	"sync"

	"github.com/apex/log"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/storage"
)

// ========================================================================================
// MessageFetch process message from queue
type MessageFetch interface {
	StartReading(startIndex int64) error
	StopOperation() error
}

// messageFetchImpl implements MessageFetcher
type messageFetchImpl struct {
	common.Component
	queueName        string
	wg               *sync.WaitGroup
	rootContext      context.Context
	operationContext context.Context
	contextCancel    context.CancelFunc
	fetchLoopRunning bool
	storage          storage.MessageQueues
	forwardMsg       SubmitMessage
}

// DefineMessageFetcher create new message fetch module
func DefineMessageFetcher(
	queueName string,
	wg *sync.WaitGroup,
	storage storage.MessageQueues,
	forwardCB SubmitMessage,
	rootCtxt context.Context,
) (MessageFetch, error) {
	logTags := log.Fields{
		"module": "dispatch", "component": "message-fetch", "instance": queueName,
	}
	instance := messageFetchImpl{
		Component:        common.Component{LogTags: logTags},
		queueName:        queueName,
		wg:               wg,
		rootContext:      rootCtxt,
		fetchLoopRunning: false,
		storage:          storage,
		forwardMsg:       forwardCB,
	}
	return &instance, nil
}

// ----------------------------------------------------------------------------------------

// StartReading starts the background queue reading loop
func (f *messageFetchImpl) StartReading(startIndex int64) error {
	// Start reading from the queue data stream
	f.wg.Add(1)
	f.fetchLoopRunning = true
	ctxt, cancel := context.WithCancel(f.rootContext)
	f.operationContext = ctxt
	f.contextCancel = cancel
	go func() {
		defer f.wg.Done()
		readParam := storage.ReadStreamParam{
			TargetQueue: f.queueName, StartIndex: startIndex, Handler: f.processMessage,
		}
		log.WithFields(f.LogTags).Infof("Start fetching messages from queue %s", f.queueName)
		if err := f.storage.ReadStream(readParam, f.operationContext); err != nil {
			log.WithError(err).WithFields(f.LogTags).Errorf("Reading queue %s failed", f.queueName)
		}
		log.WithFields(f.LogTags).Infof("Stop fetching messages from queue %s", f.queueName)
	}()
	return nil
}

// processMessage support function for dealing with the messages read from a queue
func (f *messageFetchImpl) processMessage(msg common.Message, index int64) (bool, error) {
	// Repackage the message
	wrapped := MessageInFlight{Message: msg, Index: index, Redelivery: false}
	// Send it
	return f.fetchLoopRunning, f.forwardMsg(wrapped, f.operationContext)
}

// ----------------------------------------------------------------------------------------

// StopOperation stop reading from queue and forwarding
func (f *messageFetchImpl) StopOperation() error {
	f.fetchLoopRunning = false
	f.contextCancel()
	return nil
}
