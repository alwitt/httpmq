package dispatch

import (
	"context"
	"sync"
	"time"

	"github.com/apex/log"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/storage"
)

// ========================================================================================
// MessageFetch process message from queue
type MessageFetch interface {
	StartReading(startIndex int64, maxRetries int) error
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
		"module": "dispatch", "component": "message-fetch", "queue": queueName,
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
func (f *messageFetchImpl) StartReading(startIndex int64, maxRetries int) error {
	// Start reading from the queue data stream
	f.wg.Add(1)
	f.fetchLoopRunning = true
	ctxt, cancel := context.WithCancel(f.rootContext)
	f.operationContext = ctxt
	f.contextCancel = cancel
	go func() {
		defer f.wg.Done()
		log.WithFields(f.LogTags).Infof(
			"Start fetching messages from queue %s@%d", f.queueName, startIndex,
		)
		// Keep trying to read from stream unless context is cancelled
		readFromIndex := startIndex
		for itr := 0; itr < maxRetries && f.operationContext.Err() == nil; itr++ {
			readParam := storage.ReadStreamParam{
				TargetQueue: f.queueName, StartIndex: readFromIndex, Handler: f.processMessage,
			}
			log.WithFields(f.LogTags).Debugf("Calling ReadStream")
			nextIdx, err := f.storage.ReadStream(readParam, f.operationContext)
			log.WithFields(f.LogTags).Debugf("Reading queue ended before processing %d", nextIdx)
			if err != nil {
				log.WithError(err).WithFields(f.LogTags).Errorf("Reading queue %s failed", f.queueName)
				break
			}
			readFromIndex = nextIdx
			time.Sleep(time.Millisecond * 50)
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
	log.WithFields(f.LogTags).Debugf("Sending onward %s", wrapped)
	return f.fetchLoopRunning, f.forwardMsg(wrapped, f.operationContext)
}

// ----------------------------------------------------------------------------------------

// StopOperation stop reading from queue and forwarding
func (f *messageFetchImpl) StopOperation() error {
	log.WithFields(f.LogTags).Info("Signal fetch loop to exit")
	f.fetchLoopRunning = false
	f.contextCancel()
	return nil
}
