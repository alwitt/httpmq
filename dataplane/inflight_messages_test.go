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
	"testing"
	"time"

	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/core"
	"github.com/alwitt/httpmq/management"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
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
		ServerURI:           common.GetUnitTestNatsURI(),
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

	tp, err := common.GetNewTaskProcessorInstance(utCtxt, testName, 4)
	assert.Nil(err)

	jsCtrl, err := management.GetJetStreamController(js, testName)
	assert.Nil(err)

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
		assert.Nil(jsCtrl.CreateStream(utCtxt, streamParam))
		streamParam = management.JSStreamParam{
			Name:     stream2,
			Subjects: []string{subjects2},
			JSStreamLimits: management.JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(jsCtrl.CreateStream(utCtxt, streamParam))
	}
	consumer1 := uuid.New().String()
	var consumer1Sub1 *nats.Subscription
	var consumer1Sub2 *nats.Subscription
	{
		param := management.JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 2, Mode: "push",
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(utCtxt, stream1, param))
		assert.Nil(jsCtrl.CreateConsumerForStream(utCtxt, stream2, param))
		s, err := js.JetStream().SubscribeSync(subjects1, nats.Durable(consumer1))
		assert.Nil(err)
		consumer1Sub1 = s
		s, err = js.JetStream().SubscribeSync(subjects2, nats.Durable(consumer1))
		assert.Nil(err)
		consumer1Sub2 = s
	}
	log.Debug("============================= 1 =============================")

	uut, err := getJetStreamInflightMsgProcessor(utCtxt, tp, stream1, subjects1, consumer1)
	assert.Nil(err)

	// Start the task processor
	assert.Nil(tp.StartEventLoop(&wg))

	// Case 0: ACK unknown stream
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.NotNil(
			uut.HandlerMsgACK(
				ctxt, AckIndication{
					Stream:   uuid.New().String(),
					Consumer: testName,
					SeqNum:   AckSeqNum{Stream: 12, Consumer: 2},
				}, true,
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
		assert.Nil(uut.RecordInflightMessage(ctxt, rxMsg, true))
	}
	log.Debug("============================= 3 =============================")

	// Case 2: ACK the message
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(
			uut.HandlerMsgACK(
				ctxt, AckIndication{
					Stream:   stream1,
					Consumer: consumer1,
					SeqNum:   AckSeqNum{Stream: testMsg1Seq.Stream, Consumer: testMsg1Seq.Consumer},
				}, true,
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
		assert.Nil(uut.RecordInflightMessage(ctxt, rxMsg, true))
	}
	log.Debug("============================= 5 =============================")

	// Case 3: ACK the message but with wrong seq number
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.NotNil(
			uut.HandlerMsgACK(
				ctxt, AckIndication{
					Stream:   stream2,
					Consumer: consumer1,
					SeqNum:   AckSeqNum{Stream: testMsg3Seq.Stream + 2, Consumer: testMsg3Seq.Consumer},
				}, true,
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
				ctxt, AckIndication{
					Stream:   stream2,
					Consumer: consumer1,
					SeqNum:   AckSeqNum{Stream: testMsg3Seq.Stream, Consumer: testMsg3Seq.Consumer},
				}, true,
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
				ctxt, AckIndication{
					Stream:   stream2,
					Consumer: consumer1,
					SeqNum:   AckSeqNum{Stream: testMsg3Seq.Stream, Consumer: testMsg3Seq.Consumer},
				}, true,
			),
		)
	}
	log.Debug("============================= 8 =============================")
}
