package dataplane

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"gitlab.com/project-nan/httpmq/core"
)

func TestAckTransport(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-js-ack-transport"

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "dataplane_test",
		"component": "JetStreamACKTransport",
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

	testQueue := uuid.New().String()
	testConsumer1 := uuid.New().String()
	testConsumer2 := uuid.New().String()

	uutTX, err := GetJetStreamACKBroadcaster(js, testName)
	assert.Nil(err)
	uutRX1, err := GetJetStreamACKReceiver(js, testQueue, testConsumer1)
	assert.Nil(err)

	// Case 0: start subscription on uutRX1
	rxChan1 := make(chan AckIndication, 1)
	ackHandler1 := func(ack AckIndication) {
		rxChan1 <- ack
	}
	err = uutRX1.SubscribeForACKs(&wg, utCtxt, ackHandler1)
	assert.Nil(err)
	// subscribe again
	err = uutRX1.SubscribeForACKs(&wg, utCtxt, ackHandler1)
	assert.NotNil(err)

	// Case 1: send an ACK
	ack1 := AckIndication{
		Queue:    testQueue,
		Consumer: testConsumer1,
		SeqNum: ackSeqNum{
			Queue:    1,
			Consumer: 10,
		},
	}
	assert.Nil(uutTX.BroadcastACK(ack1))
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case ackMsg, ok := <-rxChan1:
			assert.True(ok)
			assert.EqualValues(ack1, ackMsg)
		case <-ctxt.Done():
			assert.False(true)
		}
	}

	uutRX2, err := GetJetStreamACKReceiver(js, testQueue, testConsumer1)
	assert.Nil(err)
	rxChan2 := make(chan AckIndication, 1)
	ackHandler2 := func(ack AckIndication) {
		rxChan2 <- ack
	}
	err = uutRX2.SubscribeForACKs(&wg, utCtxt, ackHandler2)
	assert.Nil(err)

	// Case 2: test with two instances
	ack2 := AckIndication{
		Queue:    testQueue,
		Consumer: testConsumer1,
		SeqNum: ackSeqNum{
			Queue:    2,
			Consumer: 12,
		},
	}
	assert.Nil(uutTX.BroadcastACK(ack2))
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case ackMsg, ok := <-rxChan1:
			assert.True(ok)
			assert.EqualValues(ack2, ackMsg)
		case <-ctxt.Done():
			assert.False(true)
		}
		select {
		case ackMsg, ok := <-rxChan2:
			assert.True(ok)
			assert.EqualValues(ack2, ackMsg)
		case <-ctxt.Done():
			assert.False(true)
		}
	}

	uutRX3, err := GetJetStreamACKReceiver(js, testQueue, testConsumer2)
	assert.Nil(err)
	rxChan3 := make(chan AckIndication, 1)
	ackHandler3 := func(ack AckIndication) {
		rxChan3 <- ack
	}
	err = uutRX3.SubscribeForACKs(&wg, utCtxt, ackHandler3)
	assert.Nil(err)

	// Case 3: test with different consumer
	ack3 := AckIndication{
		Queue:    testQueue,
		Consumer: testConsumer2,
		SeqNum: ackSeqNum{
			Queue:    3,
			Consumer: 32,
		},
	}
	assert.Nil(uutTX.BroadcastACK(ack3))
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Millisecond*150)
		defer cancel()
		select {
		case _, ok := <-rxChan1:
			assert.False(ok)
		case _, ok := <-rxChan2:
			assert.False(ok)
		case <-ctxt.Done():
			break
		}
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case ackMsg, ok := <-rxChan3:
			assert.True(ok)
			assert.EqualValues(ack3, ackMsg)
		case <-ctxt.Done():
			assert.False(true)
		}
	}
}
