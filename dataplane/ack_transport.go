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
	"encoding/json"
	"fmt"
	"sync"

	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/core"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
)

// AckSeqNum are the sequence numbers of the NATs JetStream message
type AckSeqNum struct {
	// Stream is the JetStream message sequence number for this stream
	Stream uint64 `json:"stream" validate:"required,gte=0"`
	// Consumer is the JetStream message sequence number for this consumer
	Consumer uint64 `json:"consumer" validate:"required,gte=0"`
}

// AckIndication is the ACK of a NATs JetStream message which contains its key parameters
type AckIndication struct {
	// Stream is the name of the stream
	Stream string `json:"stream" validate:"required,alphaunicode|uuid"`
	// Consumer is the name of the consumer
	Consumer string `json:"consumer" validate:"required,alphaunicode|uuid"`
	// SeqNum is the sequence number of the JetStream message
	SeqNum AckSeqNum `json:"seq_num" validate:"required,dive"`
}

// String toString for ackIndication
func (m AckIndication) String() string {
	return fmt.Sprintf(
		"%s@%s:ACK[S:%d, C:%d]", m.Consumer, m.Stream, m.SeqNum.Stream, m.SeqNum.Consumer,
	)
}

// defineACKBroadcastSubject helper function to define a NATs subject based on stream and consumer
func defineACKBroadcastSubject(stream, consumer string) string {
	return fmt.Sprintf("ack-rx.%s.%s", stream, consumer)
}

// JetStreamAckHandler is the function signature for callback processing a JetStream ACK
type JetStreamAckHandler func(context.Context, AckIndication)

// JetStreamACKReceiver processes JetStream message ACKs being broadcast through NATs subjects
type JetStreamACKReceiver interface {
	// SubscribeForACKs start receiving JetStream message ACKs
	SubscribeForACKs(wg *sync.WaitGroup, handler JetStreamAckHandler) error
}

// jetStreamACKReceiverImpl implements JetStreamACKReceiver
type jetStreamACKReceiverImpl struct {
	common.Component
	ackSubject      string
	nats            *core.NatsClient
	subscribed      bool
	ackSubscription *nats.Subscription
	lock            *sync.Mutex
	validate        *validator.Validate
	ctxt            context.Context
}

// getJetStreamACKReceiver define JetStreamACKReceiver
func getJetStreamACKReceiver(
	opContext context.Context, natsClient *core.NatsClient, stream, subject, consumer string,
) (JetStreamACKReceiver, error) {
	ackSubject := defineACKBroadcastSubject(stream, consumer)
	logTags := log.Fields{
		"module":    "dataplane",
		"component": "js-ack-receiver",
		"stream":    stream,
		"subject":   subject,
		"consumer":  consumer,
	}
	logTags, err := common.UpdateLogTags(opContext, logTags)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Failed to update logtags")
		return nil, err
	}
	// Add context for log
	validate := validator.New()
	{
		t := wrapperForValidation{
			Stream: stream, Consumer: consumer, Subject: &subject,
		}
		if err := t.Validate(validate); err != nil {
			log.WithError(err).WithFields(logTags).Error("Unable to define ACK receiver")
			return nil, err
		}
	}
	return &jetStreamACKReceiverImpl{
		Component:       common.Component{LogTags: logTags},
		ackSubject:      ackSubject,
		nats:            natsClient,
		subscribed:      false,
		ackSubscription: nil,
		lock:            new(sync.Mutex),
		validate:        validate,
		ctxt:            opContext,
	}, nil
}

// SubscribeForACKs start receiving JetStream message ACKs
func (r *jetStreamACKReceiverImpl) SubscribeForACKs(
	wg *sync.WaitGroup, handler JetStreamAckHandler,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	// Already subscribed
	if r.subscribed {
		return fmt.Errorf("already instructed to subscribe to %s", r.ackSubject)
	}
	r.subscribed = true
	// Subscribe to the ACK channel for updates
	ackSub, err := r.nats.NATs().Subscribe(r.ackSubject, func(msg *nats.Msg) {
		// Process the message
		var ackInfo AckIndication
		if err := json.Unmarshal(msg.Data, &ackInfo); err != nil {
			log.WithError(err).WithFields(r.LogTags).Errorf(
				"Failed to read ACK message: %s", msg.Data,
			)
			return
		}
		// Validate the message
		if err := r.validate.Struct(&ackInfo); err != nil {
			log.WithError(err).WithFields(r.LogTags).Errorf(
				"Failed to validate ACK message: %s", msg.Data,
			)
			return
		}
		// Forward the message
		log.WithFields(r.LogTags).Debugf("Received %s", ackInfo.String())
		handler(r.ctxt, ackInfo)
	})
	if err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to subscribe to ACK channel %s", r.ackSubject,
		)
		return err
	}
	r.ackSubscription = ackSub
	// Handler to automatically un-subscribe once the context is over
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-r.ctxt.Done()
		log.WithFields(r.LogTags).Debugf("Unsubscribing from ACK channel %s", r.ackSubject)
		if err := r.ackSubscription.Unsubscribe(); err != nil {
			log.WithError(err).WithFields(r.LogTags).Errorf(
				"Error occurred when unsubscribing from ACK channel %s", r.ackSubject,
			)
		}
		log.WithFields(r.LogTags).Infof("Unsubscribed from ACK channel %s", r.ackSubject)
	}()
	return nil
}

// ==============================================================================

// JetStreamACKBroadcaster broadcasts JetStream message ACK through NATs subjects
type JetStreamACKBroadcaster interface {
	// BroadcastACK broadcast a JetStream message ACK
	BroadcastACK(ctxt context.Context, ack AckIndication) error
}

// jetStreamACKBroadcasterImpl implements JetStreamACKBroadcaster
type jetStreamACKBroadcasterImpl struct {
	common.Component
	nats     *core.NatsClient
	validate *validator.Validate
}

// GetJetStreamACKBroadcaster define JetStreamACKBroadcaster
func GetJetStreamACKBroadcaster(
	natsClient *core.NatsClient, instance string,
) (JetStreamACKBroadcaster, error) {
	logTags := log.Fields{
		"module":    "dataplane",
		"component": "js-ack-broadcaster",
		"instance":  instance,
	}
	return &jetStreamACKBroadcasterImpl{
		Component: common.Component{LogTags: logTags},
		nats:      natsClient,
		validate:  validator.New(),
	}, nil
}

// BroadcastACK broadcast a JetStream message ACK
func (t *jetStreamACKBroadcasterImpl) BroadcastACK(ctxt context.Context, ack AckIndication) error {
	localLogTags, err := common.UpdateLogTags(ctxt, t.LogTags)
	if err != nil {
		log.WithError(err).WithFields(t.LogTags).Errorf("Failed to update logtags")
		return err
	}
	if err := t.validate.Struct(&ack); err != nil {
		log.WithError(err).WithFields(localLogTags).Error("ACK parameter invalid")
		return err
	}
	subject := defineACKBroadcastSubject(ack.Stream, ack.Consumer)
	msg, err := json.Marshal(&ack)
	if err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf("Unable to serialize ACK %s", ack)
		return err
	}
	log.WithFields(localLogTags).Debugf("Sending %s on %s", ack, subject)
	if err := t.nats.NATs().Publish(subject, msg); err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf("Failed to send ACK %s on %s", ack, subject)
		return err
	}
	log.WithFields(localLogTags).Debugf("Sent %s on %s", ack, subject)
	return nil
}
