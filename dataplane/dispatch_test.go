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

func TestPushMessageDispatcher(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-push-dispatcher"

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "dataplane_test",
		"component": "MessageDispatcher",
		"instance":  "pushMessageDispatcher",
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

	jsCtrl, err := management.GetJetStreamController(js, testName)
	assert.Nil(err)

	// Define stream and consumers for testing
	stream1 := uuid.New().String()
	subject1 := fmt.Sprintf("%s.primary", uuid.New().String())
	{
		maxAge := time.Second * 10
		streamParam := management.JSStreamParam{
			Name:     stream1,
			Subjects: []string{subject1},
			JSStreamLimits: management.JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(jsCtrl.CreateStream(utCtxt, streamParam))
	}
	consumer1 := uuid.New().String()
	maxInflight := 2
	{
		param := management.JetStreamConsumerParam{
			Name: consumer1, MaxInflight: maxInflight, Mode: "push", FilterSubject: &subject1,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(utCtxt, stream1, param))
	}
	log.Debug("============================= 1 =============================")

	msgRxChan := make(chan *nats.Msg, maxInflight)
	msgHandler := func(_ context.Context, msg *nats.Msg) error {
		msgRxChan <- msg
		return nil
	}

	internalErrorHandler := func(err error) {
		assert.Equal("nats: connection closed", err.Error())
	}

	// Case 0: start a new dispatcher
	uut, err := GetPushMessageDispatcher(
		utCtxt, js, stream1, subject1, consumer1, nil, maxInflight, &wg,
	)
	assert.Nil(err)
	assert.Nil(uut.Start(msgHandler, internalErrorHandler))
	log.Debug("============================= 2 =============================")

	publisher, err := GetJetStreamPublisher(js, testName)
	assert.Nil(err)
	ackSend, err := GetJetStreamACKBroadcaster(js, testName)
	assert.Nil(err)

	// Case 1: send a message
	msg1 := []byte(uuid.New().String())
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(publisher.Publish(ctxt, subject1, msg1))
	}
	log.Debug("============================= 3 =============================")
	msg1SeqNum := AckSeqNum{}
	// verify reception
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case rxMsg, ok := <-msgRxChan:
			assert.True(ok)
			assert.Equal(msg1, rxMsg.Data)
			meta, err := rxMsg.Metadata()
			assert.Nil(err)
			msg1SeqNum.Consumer = meta.Sequence.Consumer
			msg1SeqNum.Stream = meta.Sequence.Stream
		case <-ctxt.Done():
			assert.False(true)
		}
	}
	log.Debug("============================= 4 =============================")

	// Case 2: send a message
	msg2 := []byte(uuid.New().String())
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(publisher.Publish(ctxt, subject1, msg2))
	}
	log.Debug("============================= 5 =============================")
	// verify reception
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case rxMsg, ok := <-msgRxChan:
			assert.True(ok)
			assert.Equal(msg2, rxMsg.Data)
		case <-ctxt.Done():
			assert.False(true)
		}
	}
	log.Debug("============================= 6 =============================")

	// Case 3: send a message, it should not be delivered
	msg3 := []byte(uuid.New().String())
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(publisher.Publish(ctxt, subject1, msg3))
	}
	log.Debug("============================= 7 =============================")
	// verify nothing came
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Millisecond*300)
		defer cancel()
		select {
		case <-msgRxChan:
			assert.True(false)
		case <-ctxt.Done():
			break
		}
	}
	log.Debug("============================= 8 =============================")

	// Case 4: ACK the first message
	assert.Nil(ackSend.BroadcastACK(
		utCtxt, AckIndication{Stream: stream1, Consumer: consumer1, SeqNum: msg1SeqNum},
	))
	log.Debug("============================= 9 =============================")
	// The third message should come now
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case rxMsg, ok := <-msgRxChan:
			assert.True(ok)
			assert.Equal(msg3, rxMsg.Data)
		case <-ctxt.Done():
			assert.False(true)
		}
	}
	log.Debug("============================= 10 =============================")
}
