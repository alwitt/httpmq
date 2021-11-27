package dataplane

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/core"
	"gitlab.com/project-nan/httpmq/management"
)

func TestInflightMessageHandling(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-js-inflight-handling"

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "dataplane_test",
		"component": "JetStreamInflightHandling",
		"instance":  "basic",
	}

	// Define NATS connection params
	natsParam := core.NATSConnectParams{
		ServerURI:           "nats://127.0.0.1:4222",
		ConnectTimeout:      time.Second,
		MaxReconnectAttempt: 0,
		ReconnectWait:       time.Second,
		OnDisconnectCallback: func(_ *nats.Conn, e error) {
			if e != nil {
				log.WithError(e).WithFields(logTags).Error(
					"Disconnect callback triggered with failure",
				)
			}
		},
		OnReconnectCallback: func(_ *nats.Conn) {
			log.WithFields(logTags).Debug("Reconnected with NATs server")
		},
		OnCloseCallback: func(_ *nats.Conn) {
			log.WithFields(logTags).Debug("Disconnected from NATs server")
		},
	}

	js, err := core.GetJetStream(natsParam)
	assert.Nil(err)
	defer js.Close(utCtxt)

	tp, err := common.GetNewTaskProcessorInstance(testName, 4, utCtxt)
	assert.Nil(err)

	jsCtrl, err := management.GetJetStreamController(js, testName)
	assert.Nil(err)

	// Clear out current streams in JetStream
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		existing := jsCtrl.GetAllStreams(ctxt)
		for stream := range existing {
			// Clear out any consumer attached to the stream
			consumers := jsCtrl.GetAllConsumersForStream(stream, ctxt)
			for consumer := range consumers {
				assert.Nil(jsCtrl.DeleteConsumerOnStream(stream, consumer))
			}
			assert.Nil(jsCtrl.DeleteStream(stream))
		}
	}

	// Define stream and consumers for testing
	stream1 := uuid.New().String()
	subjects1 := uuid.New().String()
	stream2 := uuid.New().String()
	subjects2 := uuid.New().String()
	{
		maxAge := time.Second * 10
		streamParam := management.JSStreamParam{
			Name:     stream1,
			Subjects: []string{subjects1},
			JSStreamLimits: management.JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(jsCtrl.CreateStream(streamParam))
		streamParam = management.JSStreamParam{
			Name:     stream2,
			Subjects: []string{subjects2},
			JSStreamLimits: management.JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(jsCtrl.CreateStream(streamParam))
	}
	consumer1 := uuid.New().String()
	var consumer1Sub1 *nats.Subscription
	var consumer1Sub2 *nats.Subscription
	{
		param := management.JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 2, Mode: "push",
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(stream1, param))
		assert.Nil(jsCtrl.CreateConsumerForStream(stream2, param))
		s, err := js.JetStream().SubscribeSync(subjects1, nats.Durable(consumer1))
		assert.Nil(err)
		consumer1Sub1 = s
		s, err = js.JetStream().SubscribeSync(subjects2, nats.Durable(consumer1))
		assert.Nil(err)
		consumer1Sub2 = s
	}
	log.Debug("============================= 1 =============================")

	uut, err := GetJetStreamInflightMsgProcessor(tp, subjects1, consumer1)
	assert.Nil(err)

	// Start the task processor
	assert.Nil(tp.StartEventLoop(&wg))

	// Case 0: ACK unknown stream
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.NotNil(
			uut.HandlerMsgACK(
				AckIndication{
					Stream:   uuid.New().String(),
					Consumer: testName,
					SeqNum:   ackSeqNum{Stream: 12, Consumer: 2},
				}, true, ctxt,
			),
		)
	}
	log.Debug("============================= 2 =============================")

	// Case 1: register an inflight message
	testMsg1 := []byte(fmt.Sprintf("Hello %s", uuid.New().String()))
	var testMsg1Seq nats.SequencePair
	{
		ack, err := js.JetStream().Publish(subjects1, testMsg1)
		assert.Nil(err)
		assert.Equal(stream1, ack.Stream)
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second*2)
		defer cancel()
		rxMsg, err := consumer1Sub1.NextMsgWithContext(ctxt)
		assert.Nil(err)
		assert.NotNil(rxMsg)
		meta, err := rxMsg.Metadata()
		assert.Nil(err)
		testMsg1Seq = meta.Sequence
		// Cache message for later ACK
		assert.Nil(uut.RecordInflightMessage(rxMsg, true, ctxt))
	}
	log.Debug("============================= 3 =============================")

	// Case 2: ACK the message
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(
			uut.HandlerMsgACK(
				AckIndication{
					Stream:   stream1,
					Consumer: consumer1,
					SeqNum:   ackSeqNum{Stream: testMsg1Seq.Stream, Consumer: testMsg1Seq.Consumer},
				}, true, ctxt,
			),
		)
	}
	log.Debug("============================= 4 =============================")

	// Case 3: register an inflight message
	testMsg3 := []byte(fmt.Sprintf("Hello %s", uuid.New().String()))
	var testMsg3Seq nats.SequencePair
	{
		ack, err := js.JetStream().Publish(subjects2, testMsg3)
		assert.Nil(err)
		assert.Equal(stream2, ack.Stream)
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second*2)
		defer cancel()
		rxMsg, err := consumer1Sub2.NextMsgWithContext(ctxt)
		assert.Nil(err)
		assert.NotNil(rxMsg)
		meta, err := rxMsg.Metadata()
		assert.Nil(err)
		testMsg3Seq = meta.Sequence
		// Cache message for later ACK
		assert.Nil(uut.RecordInflightMessage(rxMsg, true, ctxt))
	}
	log.Debug("============================= 5 =============================")

	// Case 3: ACK the message but with wrong seq number
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.NotNil(
			uut.HandlerMsgACK(
				AckIndication{
					Stream:   stream2,
					Consumer: consumer1,
					SeqNum:   ackSeqNum{Stream: testMsg3Seq.Stream, Consumer: testMsg3Seq.Consumer + 2},
				}, true, ctxt,
			),
		)
	}
	log.Debug("============================= 6 =============================")

	// Case 4: again, but with the correct seq number
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(
			uut.HandlerMsgACK(
				AckIndication{
					Stream:   stream2,
					Consumer: consumer1,
					SeqNum:   ackSeqNum{Stream: testMsg3Seq.Stream, Consumer: testMsg3Seq.Consumer},
				}, true, ctxt,
			),
		)
	}
	log.Debug("============================= 7 =============================")

	// Case 5: ACK the message again
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.NotNil(
			uut.HandlerMsgACK(
				AckIndication{
					Stream:   stream2,
					Consumer: consumer1,
					SeqNum:   ackSeqNum{Stream: testMsg3Seq.Stream, Consumer: testMsg3Seq.Consumer},
				}, true, ctxt,
			),
		)
	}
	log.Debug("============================= 8 =============================")
}
