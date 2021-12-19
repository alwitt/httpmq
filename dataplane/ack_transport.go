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
	Stream uint64 `json:"stream" validate:"required"`
	// Consumer is the JetStream message sequence number for this consumer
	Consumer uint64 `json:"consumer" validate:"required"`
}

// AckIndication is the ACK of a NATs JetStream message which contains its key parameters
type AckIndication struct {
	// Stream is the name of the stream
	Stream string `json:"stream" validate:"required"`
	// Consumer is the name of the consumer
	Consumer string `json:"consumer" validate:"required"`
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
type JetStreamAckHandler func(AckIndication, context.Context)

// JetStreamACKReceiver processes JetStream message ACKs being broadcast through NATs subjects
type JetStreamACKReceiver interface {
	// SubscribeForACKs start receiving JetStream message ACKs
	SubscribeForACKs(
		wg *sync.WaitGroup, opContext context.Context, handler JetStreamAckHandler,
	) error
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
}

// getJetStreamACKReceiver define JetStreamACKReceiver
func getJetStreamACKReceiver(
	natsClient *core.NatsClient, stream, subject, consumer string,
) (JetStreamACKReceiver, error) {
	ackSubject := defineACKBroadcastSubject(stream, consumer)
	logTags := log.Fields{
		"module":    "dataplane",
		"component": "js-ack-receiver",
		"stream":    stream,
		"subject":   subject,
		"consumer":  consumer,
	}
	return &jetStreamACKReceiverImpl{
		Component:       common.Component{LogTags: logTags},
		ackSubject:      ackSubject,
		nats:            natsClient,
		subscribed:      false,
		ackSubscription: nil,
		lock:            new(sync.Mutex),
		validate:        validator.New(),
	}, nil
}

// SubscribeForACKs start receiving JetStream message ACKs
func (r *jetStreamACKReceiverImpl) SubscribeForACKs(
	wg *sync.WaitGroup, opContext context.Context, handler JetStreamAckHandler,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	// Already subscribed
	if r.subscribed {
		return fmt.Errorf("already instructed to subscribe to %s", r.ackSubject)
	}
	localLogTags, err := common.UpdateLogTags(r.LogTags, opContext)
	if err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf("Failed to update logtags")
		return err
	}
	r.subscribed = true
	// Subscribe to the ACK channel for updates
	ackSub, err := r.nats.NATs().Subscribe(r.ackSubject, func(msg *nats.Msg) {
		// Process the message
		var ackInfo AckIndication
		if err := json.Unmarshal(msg.Data, &ackInfo); err != nil {
			log.WithError(err).WithFields(localLogTags).Errorf(
				"Failed to read ACK message: %s", msg.Data,
			)
			return
		}
		// Validate the message
		if err := r.validate.Struct(&ackInfo); err != nil {
			log.WithError(err).WithFields(localLogTags).Errorf(
				"Failed to validate ACK message: %s", msg.Data,
			)
			return
		}
		// Forward the message
		log.WithFields(localLogTags).Debugf("Received %s", ackInfo.String())
		handler(ackInfo, opContext)
	})
	if err != nil {
		log.WithError(err).WithFields(localLogTags).Errorf(
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
		log.WithFields(localLogTags).Debugf("Unsubscribing from ACK channel %s", r.ackSubject)
		if err := r.ackSubscription.Unsubscribe(); err != nil {
			log.WithError(err).WithFields(localLogTags).Errorf(
				"Error occurred when unsubscribing from ACK channel %s", r.ackSubject,
			)
		}
		log.WithFields(localLogTags).Infof("Unsubscribed from ACK channel %s", r.ackSubject)
	}()
	return nil
}

// ==============================================================================

// JetStreamACKBroadcaster broadcasts JetStream message ACK through NATs subjects
type JetStreamACKBroadcaster interface {
	// BroadcastACK broadcast a JetStream message ACK
	BroadcastACK(ack AckIndication, ctxt context.Context) error
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
func (t *jetStreamACKBroadcasterImpl) BroadcastACK(ack AckIndication, ctxt context.Context) error {
	localLogTags, err := common.UpdateLogTags(t.LogTags, ctxt)
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
