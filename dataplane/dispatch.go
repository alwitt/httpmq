// Copyright 2021-2022 The httpmq Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataplane

import (
	"context"
	"fmt"
	"sync"

	"github.com/alwitt/goutils"
	"github.com/alwitt/httpmq/core"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
)

// MessageDispatcher process a consumer subscription request from a client and dispatch
// messages to that client
type MessageDispatcher interface {
	// Start starts operations
	Start(msgOutput ForwardMessageHandlerCB, errorCB AlertOnErrorCB) error
}

// pushMessageDispatcher implements MessageDispatcher for a push consumer
type pushMessageDispatcher struct {
	goutils.Component
	nats       *core.NatsClient
	optContext context.Context
	wg         *sync.WaitGroup
	lock       *sync.Mutex
	started    bool
	// msgTracking monitors the set of inflight messages
	msgTracking   JetStreamInflightMsgProcessor
	msgTrackingTP goutils.TaskProcessor
	// ackWatcher monitors for ACK being received
	ackWatcher JetStreamACKReceiver
	// subscriber connected to JetStream to receive messages
	subscriber JetStreamPushSubscriber
}

// GetPushMessageDispatcher get a new push MessageDispatcher
func GetPushMessageDispatcher(
	ctxt context.Context,
	natsClient *core.NatsClient,
	stream, subject, consumer string,
	deliveryGroup *string,
	maxInflightMsgs int,
	wg *sync.WaitGroup,
) (MessageDispatcher, error) {
	instance := fmt.Sprintf("%s@%s/%s", consumer, stream, subject)
	logTags := log.Fields{
		"module":    "dataplane",
		"component": "push-msg-dispatcher",
		"stream":    stream,
		"subject":   subject,
		"consumer":  consumer,
	}
	goutils.ModifyLogMetadataByRestRequestParam(ctxt, logTags)

	// Initial validation
	{
		validate := validator.New()
		t := wrapperForValidation{
			Stream: stream, Consumer: consumer, DeliveryGroup: deliveryGroup, Subject: &subject,
		}
		if err := t.Validate(validate); err != nil {
			log.WithError(err).WithFields(logTags).Error(
				"Unable to define dispatch due to parameter errors",
			)
			return nil, err
		}
	}

	// Define components
	ackReceiver, err := getJetStreamACKReceiver(ctxt, natsClient, stream, subject, consumer)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define ACK receiver")
		return nil, err
	}
	tpLogTags := log.Fields{
		"module":    "dataplane",
		"component": "task-processor",
		"instance":  instance,
		"stream":    stream,
		"subject":   subject,
		"consumer":  consumer,
	}
	goutils.ModifyLogMetadataByRestRequestParam(ctxt, logTags)
	msgTrackingTP, err := goutils.GetNewTaskProcessorInstance(
		ctxt, instance, maxInflightMsgs*4, tpLogTags,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define task processor")
		return nil, err
	}
	msgTracking, err := getJetStreamInflightMsgProcessor(
		ctxt, msgTrackingTP, stream, subject, consumer,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define MSG tracker")
		return nil, err
	}
	subscriber, err := getJetStreamPushSubscriber(
		ctxt, natsClient, stream, subject, consumer, deliveryGroup,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define MSG subscriber")
		return nil, err
	}

	return &pushMessageDispatcher{
		Component:     goutils.Component{LogTags: logTags},
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
func (d *pushMessageDispatcher) Start(
	msgOutput ForwardMessageHandlerCB, errorCB AlertOnErrorCB,
) error {
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
		d.wg, func(ctxt context.Context, ai AckIndication) {
			log.WithFields(d.LogTags).Debugf("Processing %s", ai.String())
			// Pass to message tracker in non-blocking mode
			if err := d.msgTracking.HandlerMsgACK(ctxt, ai, false); err != nil {
				log.WithError(err).WithFields(d.LogTags).Errorf("Failed to submit %s", ai.String())
			}
		},
	); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to start ACK receiver")
		return err
	}

	// Start subscriber
	if err := d.subscriber.StartReading(
		func(ctxt context.Context, msg *nats.Msg) error {
			msgName := msgToString(msg)
			log.WithFields(d.LogTags).Debugf("Processing %s", msgName)
			// Forward the message toward consumer
			if err := msgOutput(ctxt, msg); err != nil {
				log.WithError(err).WithFields(d.LogTags).Errorf("Unable to forward %s", msgName)
				return err
			}
			// Pass to message tracker in non-blocking mode
			if err := d.msgTracking.RecordInflightMessage(ctxt, msg, false); err != nil {
				log.WithError(err).WithFields(d.LogTags).Errorf("Unable to record %s", msgName)
				return err
			}
			return nil
		}, errorCB, d.wg); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to start MSG subscriber")
		return err
	}

	d.started = true
	return nil
}
