package dataplane

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/core"
)

// ackSeqNum sequence numbers of the ACK
type ackSeqNum struct {
	Stream   int64 `json:"stream" validate:"required"`
	Consumer int64 `json:"consumer" validate:"required"`
}

// AckIndication information regarding an ACK
type AckIndication struct {
	Stream   string    `json:"stream" validate:"required"`
	Consumer string    `json:"consumer" validate:"required"`
	SeqNum   ackSeqNum `json:"seq_num" validate:"required,dive"`
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

// jetStreamAckHandler function signature for processing a JetStream ACK
type jetStreamAckHandler func(AckIndication)

// JetStreamACKReceiver handle JetStream ACK being broadcast through NATs subjects
type JetStreamACKReceiver interface {
	SubscribeForACKs(
		wg *sync.WaitGroup, opContext context.Context, handler jetStreamAckHandler,
	) error
}

// jetStreamACKReceiverImpl implements JetStreamACKReceiver
type jetStreamACKReceiverImpl struct {
	common.Component
	ackSubject      string
	stream          string
	consumer        string
	nats            *core.NatsClient
	subscribed      bool
	ackSubscription *nats.Subscription
	lock            *sync.Mutex
	validate        *validator.Validate
}

// GetJetStreamACKReceiver define JetStreamACKReceiver
func GetJetStreamACKReceiver(
	natsClient *core.NatsClient, targetStream, targetConsumer string,
) (JetStreamACKReceiver, error) {
	ackSubject := defineACKBroadcastSubject(targetStream, targetConsumer)
	logTags := log.Fields{
		"module":    "dataplane",
		"component": "js-ack-receiver",
		"instance":  ackSubject,
	}
	return &jetStreamACKReceiverImpl{
		Component:       common.Component{LogTags: logTags},
		ackSubject:      ackSubject,
		stream:          targetStream,
		consumer:        targetConsumer,
		nats:            natsClient,
		subscribed:      false,
		ackSubscription: nil,
		lock:            new(sync.Mutex),
		validate:        validator.New(),
	}, nil
}

// SubscribeForACKs subscribe to NATs change for ACKs on (stream, consumer) tuple
func (r *jetStreamACKReceiverImpl) SubscribeForACKs(
	wg *sync.WaitGroup, opContext context.Context, handler jetStreamAckHandler,
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
		handler(ackInfo)
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
		<-opContext.Done()
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

// JetStreamACKBroadcaster broadcast JetStream ACK through NATs subjects
type JetStreamACKBroadcaster interface {
	BroadcastACK(ack AckIndication) error
}

// jetStreamACKBroadcasterImpl implements JetStreamACKBroadcaster
type jetStreamACKBroadcasterImpl struct {
	common.Component
	nats *core.NatsClient
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
	}, nil
}

// BroadcastACK broadcast the ACK
func (t *jetStreamACKBroadcasterImpl) BroadcastACK(ack AckIndication) error {
	subject := defineACKBroadcastSubject(ack.Stream, ack.Consumer)
	msg, err := json.Marshal(&ack)
	if err != nil {
		log.WithError(err).WithFields(t.LogTags).Errorf("Unable to serialize ACK %s", ack)
		return err
	}
	log.WithFields(t.LogTags).Debugf("Sending %s on %s", ack, subject)
	if err := t.nats.NATs().Publish(subject, msg); err != nil {
		log.WithError(err).WithFields(t.LogTags).Errorf("Failed to send ACK %s on %s", ack, subject)
		return err
	}
	log.WithFields(t.LogTags).Debugf("Sent %s on %s", ack, subject)
	return nil
}
