package dispatch

import (
	"context"
	"fmt"
	"sync"
	"time"

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
// TODO: Clean up the names
type messageFetchImpl struct {
	common.Component
	queueName        string
	wg               *sync.WaitGroup
	rootContext      context.Context
	operationContext context.Context
	contextCancel    context.CancelFunc
	fetchLoopRunning bool
	storage          storage.MessageQueues
	readMaxRetry     int
	retryIntSeq      common.Sequencer
	forwardMsg       SubmitMessage
	reportCritical   ReportCriticalFailure
}

// DefineMessageFetcher create new message fetch module
func DefineMessageFetcher(
	queueName string,
	wg *sync.WaitGroup,
	storage storage.MessageQueues,
	storageReadMaxRetry int,
	storageReadRetryIntSeq common.Sequencer,
	forwardCB SubmitMessage,
	reportErr ReportCriticalFailure,
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
		readMaxRetry:     storageReadMaxRetry,
		retryIntSeq:      storageReadRetryIntSeq,
		forwardMsg:       forwardCB,
		reportCritical:   reportErr,
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
		log.WithFields(f.LogTags).Infof("Start fetching messages at %d", startIndex)
		defer f.wg.Done()
		defer log.WithFields(f.LogTags).Info("Stop fetching messages")
		// Keep trying to read from stream unless context is cancelled
		readFromIndex := startIndex
		var itr int
		for itr = 0; itr < f.readMaxRetry && f.operationContext.Err() == nil; itr++ {
			readParam := storage.ReadStreamParam{
				TargetQueue: f.queueName, StartIndex: readFromIndex, Handler: f.processMessage,
			}
			// Perform read
			log.WithFields(f.LogTags).Debug("Calling ReadStream")
			nextIdx, err := f.storage.ReadStream(readParam, f.operationContext)
			log.WithFields(f.LogTags).Debugf("Reading queue ended before processing %d", nextIdx)
			if err != nil {
				log.WithError(err).WithFields(f.LogTags).Errorf("Reading queue %s failed", f.queueName)
			}
			readFromIndex = nextIdx
			// Unexpected termination of queue read
			if f.operationContext.Err() == nil {
				timeout := time.Duration(f.retryIntSeq.NextValue())
				log.WithFields(f.LogTags).Errorf(
					"Read ended unexpectedly. Will attempt read again from %d after %s.",
					nextIdx,
					timeout.String(),
				)
				time.Sleep(timeout)
			}
		}
		// Something gone wrong
		if itr >= f.readMaxRetry {
			_ = f.reportCritical(
				fmt.Errorf("exhausted retries while attempting to read from queue %s", f.queueName),
				fmt.Sprintf("dispatch.message-fetch.%s", f.queueName),
			)
		}
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
