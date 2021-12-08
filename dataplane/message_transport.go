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

// ForwardMessageHandlerCB callback used to forward new messages
type ForwardMessageHandlerCB func(msg *nats.Msg, ctxt context.Context) error

// AlertOnErrorCB callback used to expose internal error to an outer context for handling
type AlertOnErrorCB func(err error)

// JetStreamPushSubscriber component directly reading from JetStream with push consumer
type JetStreamPushSubscriber interface {
	StartReading(
		forwardCB ForwardMessageHandlerCB,
		errorCB AlertOnErrorCB,
		wg *sync.WaitGroup,
		ctxt context.Context,
	) error
}

// jetStreamPushSubscriberImpl implements JetStreamPushSubscriber
type jetStreamPushSubscriberImpl struct {
	common.Component
	nats       *core.NatsClient
	reading    bool
	sub        *nats.Subscription
	forwardMsg ForwardMessageHandlerCB
	errorCB    AlertOnErrorCB
	lock       *sync.Mutex
}

// getJetStreamPushSubscriber define new JetStreamPushSubscriber
func getJetStreamPushSubscriber(
	natsClient *core.NatsClient, stream, subject, consumer string, deliveryGroup *string,
) (JetStreamPushSubscriber, error) {
	logTags := log.Fields{
		"module":    "dataplane",
		"component": "js-push-reader",
		"stream":    stream,
		"subject":   subject,
		"consumer":  consumer,
	}
	// Create the subscription now
	var s *nats.Subscription
	var err error
	// Build the subscription based on whether deliveryGroup is defined
	if deliveryGroup != nil {
		s, err = natsClient.JetStream().QueueSubscribeSync(
			subject, *deliveryGroup, nats.Durable(consumer),
		)
	} else {
		s, err = natsClient.JetStream().SubscribeSync(subject, nats.Durable(consumer))
	}
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to define subscription")
		return nil, err
	}
	return &jetStreamPushSubscriberImpl{
		Component:  common.Component{LogTags: logTags},
		nats:       natsClient,
		sub:        s,
		forwardMsg: nil,
		errorCB:    nil,
		lock:       &sync.Mutex{},
	}, nil
}

// StartReading begin reading data from JetStream
func (r *jetStreamPushSubscriberImpl) StartReading(
	forwardCB ForwardMessageHandlerCB,
	errorCB AlertOnErrorCB,
	wg *sync.WaitGroup,
	ctxt context.Context,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	// Already reading
	if r.reading {
		err := fmt.Errorf("already reading")
		log.WithError(err).WithFields(r.LogTags).Error("Unable to start reading")
		return err
	}
	wg.Add(1)
	r.forwardMsg = forwardCB
	r.errorCB = errorCB
	r.reading = true
	// Start reading from JetStream
	go func() {
		defer wg.Done()
		log.WithFields(r.LogTags).Infof("Starting reading from JetStream")
		defer log.WithFields(r.LogTags).Infof("Stopping JetStream read loop")
		defer func() {
			if err := r.sub.Unsubscribe(); err != nil {
				log.WithError(err).WithFields(r.LogTags).Error("Unsubscribe failed")
			} else {
				log.WithFields(r.LogTags).Infof("Unsubscribed from subject")
			}
		}()
		defer func() {
			if err := r.sub.Drain(); err != nil {
				log.WithError(err).WithFields(r.LogTags).Error("Drain failed")
			} else {
				log.WithFields(r.LogTags).Infof("Drained subscription")
			}
		}()
		for {
			newMsg, err := r.sub.NextMsgWithContext(ctxt)
			if err != nil {
				log.WithError(err).WithFields(r.LogTags).Errorf("Read failure")
				r.errorCB(err)
				break
			}
			// Forward the message
			if newMsg != nil {
				log.WithFields(r.LogTags).Debugf("Received %s", msgToString(newMsg))
				if err := r.forwardMsg(newMsg, ctxt); err != nil {
					log.WithError(err).WithFields(r.LogTags).Errorf("Unable to forward messages")
					r.errorCB(err)
				}
			}
		}
	}()
	return nil
}

// ==============================================================================

// JetStreamPublisher send a new message into JetStream
type JetStreamPublisher interface {
	Publish(subject string, msg []byte, ctxt context.Context) error
}

// jetStreamPublisherImpl implements JetStreamPublisher
type jetStreamPublisherImpl struct {
	common.Component
	nats *core.NatsClient
}

// GetJetStreamPublisher get new JetStreamPublisher
func GetJetStreamPublisher(
	natsClient *core.NatsClient, instance string,
) (JetStreamPublisher, error) {
	logTags := log.Fields{
		"module": "dataplane", "component": "js-publisher", "instance": instance,
	}
	return &jetStreamPublisherImpl{
		Component: common.Component{LogTags: logTags}, nats: natsClient,
	}, nil
}

// Publish publish a message on a subject
func (s *jetStreamPublisherImpl) Publish(subject string, msg []byte, ctxt context.Context) error {
	ack, err := s.nats.JetStream().PublishAsync(subject, msg)
	if err != nil {
		log.WithError(err).WithFields(s.LogTags).Errorf("Unable to send message")
		return err
	}
	// Wait for success, failure, or timeout
	select {
	case goodSig, ok := <-ack.Ok():
		if !ok {
			err := fmt.Errorf("reading nats.PubAckFuture OK channel failure")
			log.WithError(err).WithFields(s.LogTags).Errorf("Message send failure")
			return err
		}
		log.WithFields(s.LogTags).Debugf(
			"Sent [%d] to %s/%s", goodSig.Sequence, goodSig.Stream, subject,
		)
		return nil
	case txErr, ok := <-ack.Err():
		if !ok {
			err := fmt.Errorf("reading nats.PubAckFuture error channel failure")
			log.WithError(err).WithFields(s.LogTags).Errorf("Message send failure")
			return err
		}
		return txErr
	case <-ctxt.Done():
		err := ctxt.Err()
		log.WithError(err).WithFields(s.LogTags).Errorf("Message send timed out")
		return err
	}
}
