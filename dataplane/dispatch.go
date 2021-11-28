package dataplane

import (
	"context"
	"fmt"
	"sync"

	"github.com/apex/log"
	"github.com/nats-io/nats.go"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/core"
)

// MessageDispatcher process a consumer subscription request and dispatch
// messages to the consumer
type MessageDispatcher interface {
	Start(msgOutput ForwardMessageHandlerCB) error
}

// pushMessageDispatcher implements MessageDispatcher for a push consumer
type pushMessageDispatcher struct {
	common.Component
	nats       *core.NatsClient
	optContext context.Context
	wg         *sync.WaitGroup
	lock       *sync.Mutex
	started    bool
	// msgTracking monitors the set of inflight messages
	msgTracking   JetStreamInflightMsgProcessor
	msgTrackingTP common.TaskProcessor
	// ackWatcher monitors for ACK being received
	ackWatcher JetStreamACKReceiver
	// subscriber connected to JetStream to receive messages
	subscriber JetStreamPushSubscriber
}

// GetPushMessageDispatcher get a new push MessageDispatcher
func GetPushMessageDispatcher(
	natsClient *core.NatsClient,
	stream, subject, consumer string,
	deliveryGroup *string,
	maxInflightMsgs int,
	wg *sync.WaitGroup,
	ctxt context.Context,
) (MessageDispatcher, error) {
	instance := fmt.Sprintf("%s@%s/%s", consumer, stream, subject)
	logTags := log.Fields{
		"module":    "dataplane",
		"component": "push-msg-dispatcher",
		"stream":    stream,
		"subject":   subject,
		"consumer":  consumer,
	}

	// Define components
	ackReceiver, err := getJetStreamACKReceiver(natsClient, stream, subject, consumer)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define ACK receiver")
		return nil, err
	}
	msgTrackingTP, err := common.GetNewTaskProcessorInstance(instance, maxInflightMsgs*4, ctxt)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define task processor")
		return nil, err
	}
	msgTracking, err := getJetStreamInflightMsgProcessor(msgTrackingTP, stream, subject, consumer)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define MSG tracker")
		return nil, err
	}
	subscriber, err := getJetStreamPushSubscriber(
		natsClient, stream, subject, consumer, deliveryGroup,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define MSG subscriber")
		return nil, err
	}

	return &pushMessageDispatcher{
		Component:     common.Component{LogTags: logTags},
		nats:          natsClient,
		optContext:    ctxt,
		wg:            wg,
		lock:          &sync.Mutex{},
		started:       false,
		msgTracking:   msgTracking,
		msgTrackingTP: msgTrackingTP,
		ackWatcher:    ackReceiver,
		subscriber:    subscriber,
	}, nil
}

// Start starts the push message dispatcher operation
func (d *pushMessageDispatcher) Start(msgOutput ForwardMessageHandlerCB) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.started {
		return fmt.Errorf("already started")
	}

	// Start message tracking TP
	if err := d.msgTrackingTP.StartEventLoop(d.wg); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to start MSG tracker task processor")
		return err
	}

	// Start ACK receiver
	if err := d.ackWatcher.SubscribeForACKs(
		d.wg, d.optContext, func(ai AckIndication, ctxt context.Context) {
			log.WithFields(d.LogTags).Debugf("Processing %s", ai.String())
			// Pass to message tracker in non-blocking mode
			if err := d.msgTracking.HandlerMsgACK(ai, false, ctxt); err != nil {
				log.WithError(err).WithFields(d.LogTags).Errorf("Failed to submit %s", ai.String())
			}
		},
	); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to start ACK receiver")
		return err
	}

	// Start subscriber
	if err := d.subscriber.StartReading(func(msg *nats.Msg, ctxt context.Context) error {
		msgName := msgToString(msg)
		log.WithFields(d.LogTags).Debugf("Processing %s", msgName)
		// Forward the message toward consumer
		if err := msgOutput(msg, ctxt); err != nil {
			log.WithError(err).WithFields(d.LogTags).Errorf("Unable to forward %s", msgName)
			return err
		}
		// Pass to message tracker in non-blocking mode
		if err := d.msgTracking.RecordInflightMessage(msg, false, ctxt); err != nil {
			log.WithError(err).WithFields(d.LogTags).Errorf("Unable to record %s", msgName)
			return err
		}
		return nil
	}, d.wg, d.optContext); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to start MSG subscriber")
		return err
	}

	d.started = true
	return nil
}
