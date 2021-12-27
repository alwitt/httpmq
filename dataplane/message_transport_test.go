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

func TestMessageTransportPushSub(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-js-msg-push-sub"

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "dataplane_test",
		"component": "JetStreamPushSubscriber",
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

	jsCtrl, err := management.GetJetStreamController(js, testName)
	assert.Nil(err)

	// Define stream and consumers for testing
	stream1 := uuid.New().String()
	base := uuid.New().String()
	subject1 := fmt.Sprintf("%s.primary", base)
	subject2 := fmt.Sprintf("%s.backup", base)
	subject3 := fmt.Sprintf("%s.*", base)
	{
		maxAge := time.Second * 10
		streamParam := management.JSStreamParam{
			Name:     stream1,
			Subjects: []string{subject1, subject2},
			JSStreamLimits: management.JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(jsCtrl.CreateStream(utCtxt, streamParam))
	}
	consumer1 := uuid.New().String()
	consumer2 := uuid.New().String()
	consumer3 := uuid.New().String()
	{
		param := management.JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push", FilterSubject: &subject1,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(utCtxt, stream1, param))
		param = management.JetStreamConsumerParam{
			Name: consumer2, MaxInflight: 1, Mode: "push", FilterSubject: &subject2,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(utCtxt, stream1, param))
		param = management.JetStreamConsumerParam{
			Name: consumer3, MaxInflight: 1, Mode: "push", FilterSubject: &subject3,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(utCtxt, stream1, param))
	}
	log.Debug("============================= 1 =============================")

	// Case 0: define new subscribers
	rxSub1, err := getJetStreamPushSubscriber(utCtxt, js, stream1, subject1, consumer1, nil)
	assert.Nil(err)
	rxSub2, err := getJetStreamPushSubscriber(utCtxt, js, stream1, subject2, consumer2, nil)
	assert.Nil(err)
	rxSub3, err := getJetStreamPushSubscriber(utCtxt, js, stream1, subject3, consumer3, nil)
	assert.Nil(err)
	log.Debug("============================= 2 =============================")

	internalErrorHandler := func(err error) {
		assert.Equal("nats: connection closed", err.Error())
	}

	// Case 1: start reading messages
	rxChan1 := make(chan *nats.Msg, 1)
	msgHandler1 := func(_ context.Context, msg *nats.Msg) error {
		rxChan1 <- msg
		return nil
	}
	rxChan2 := make(chan *nats.Msg, 1)
	msgHandler2 := func(_ context.Context, msg *nats.Msg) error {
		rxChan2 <- msg
		return nil
	}
	rxChan3 := make(chan *nats.Msg, 1)
	msgHandler3 := func(_ context.Context, msg *nats.Msg) error {
		rxChan3 <- msg
		return nil
	}
	assert.Nil(rxSub1.StartReading(msgHandler1, internalErrorHandler, &wg))
	assert.NotNil(rxSub1.StartReading(msgHandler1, internalErrorHandler, &wg))
	assert.Nil(rxSub2.StartReading(msgHandler2, internalErrorHandler, &wg))
	assert.Nil(rxSub3.StartReading(msgHandler3, internalErrorHandler, &wg))
	log.Debug("============================= 3 =============================")

	publisher, err := GetJetStreamPublisher(js, testName)
	assert.Nil(err)

	// Case 2: publish a message
	msg2 := []byte(uuid.New().String())
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(publisher.Publish(ctxt, subject1, msg2))
	}
	// verify receive
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case <-ctxt.Done():
			assert.False(true)
		case msg, ok := <-rxChan1:
			assert.True(ok)
			assert.EqualValues(msg2, msg.Data)
			assert.Nil(msg.AckSync())
		}
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Millisecond*200)
		defer cancel()
		select {
		case <-ctxt.Done():
			break
		case <-rxChan2:
			assert.False(true)
		}
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case <-ctxt.Done():
			assert.False(true)
		case msg, ok := <-rxChan3:
			assert.True(ok)
			assert.EqualValues(msg2, msg.Data)
			assert.Nil(msg.AckSync())
		}
	}
	log.Debug("============================= 4 =============================")

	// Case 3: publish a message
	msg3 := []byte(uuid.New().String())
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(publisher.Publish(ctxt, subject2, msg3))
	}
	// verify receive
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case <-ctxt.Done():
			assert.False(true)
		case msg, ok := <-rxChan2:
			assert.True(ok)
			assert.EqualValues(msg3, msg.Data)
			assert.Nil(msg.AckSync())
		}
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Millisecond*200)
		defer cancel()
		select {
		case <-ctxt.Done():
			break
		case <-rxChan1:
			assert.False(true)
		}
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case <-ctxt.Done():
			assert.False(true)
		case msg, ok := <-rxChan3:
			assert.True(ok)
			assert.EqualValues(msg3, msg.Data)
			assert.Nil(msg.AckSync())
		}
	}
	log.Debug("============================= 5 =============================")
}

func TestMessageTransportPushSubGroup(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-js-msg-push-sub-group"

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "dataplane_test",
		"component": "JetStreamPushSubscriber",
		"instance":  "delivery_group",
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

	js1, err := core.GetJetStream(natsParam)
	assert.Nil(err)
	defer js1.Close(utCtxt)
	js2, err := core.GetJetStream(natsParam)
	assert.Nil(err)
	defer js2.Close(utCtxt)

	jsCtrl, err := management.GetJetStreamController(js1, testName)
	assert.Nil(err)

	// Define stream and consumers for testing
	stream1 := uuid.New().String()
	subject1 := uuid.New().String()
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
	group1 := uuid.New().String()
	{
		param := management.JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push", DeliveryGroup: &group1,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(utCtxt, stream1, param))
	}
	log.Debug("============================= 1 =============================")

	// Case 0: define new subscribers
	rxSub1, err := getJetStreamPushSubscriber(utCtxt, js1, stream1, subject1, consumer1, &group1)
	assert.Nil(err)
	rxSub2, err := getJetStreamPushSubscriber(utCtxt, js2, stream1, subject1, consumer1, &group1)
	assert.Nil(err)
	log.Debug("============================= 2 =============================")

	internalErrorHandler := func(err error) {
		assert.Equal("nats: connection closed", err.Error())
	}

	// Case 1: start reading messages
	type msgTuple struct {
		msg *nats.Msg
		id  int
	}
	rxChan := make(chan msgTuple, 1)
	msgHandler1 := func(_ context.Context, msg *nats.Msg) error {
		rxChan <- msgTuple{msg: msg, id: 1}
		return nil
	}
	msgHandler2 := func(_ context.Context, msg *nats.Msg) error {
		rxChan <- msgTuple{msg: msg, id: 2}
		return nil
	}
	assert.Nil(rxSub1.StartReading(msgHandler1, internalErrorHandler, &wg))
	assert.Nil(rxSub2.StartReading(msgHandler2, internalErrorHandler, &wg))
	log.Debug("============================= 3 =============================")

	publisher, err := GetJetStreamPublisher(js1, testName)
	assert.Nil(err)

	// Case 2: send messages repeatedly
	rxID := make(map[int]int)
	for itr := 0; itr < 20; itr++ {
		testMsg := []byte(uuid.New().String())
		{
			ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
			defer cancel()
			assert.Nil(publisher.Publish(ctxt, subject1, testMsg))
		}
		{
			ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
			defer cancel()
			select {
			case <-ctxt.Done():
				assert.False(true)
			case msg, ok := <-rxChan:
				assert.True(ok)
				assert.EqualValues(testMsg, msg.msg.Data)
				assert.Nil(msg.msg.AckSync())
				count, ok := rxID[msg.id]
				if ok {
					rxID[msg.id] = count + 1
				} else {
					rxID[msg.id] = 1
				}
			}
			log.Debugf("============================= 4-%d =============================", itr)
		}
	}
	assert.Len(rxID, 2)
}

func TestMessageTranscoding(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-js-msg-transcode"

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "dataplane_test",
		"component": "JetStreamPushSubscriber",
		"instance":  "message_transcode",
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
	base := uuid.New().String()
	subject1 := fmt.Sprintf("%s.primary", base)
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
	{
		param := management.JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push", FilterSubject: &subject1,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(utCtxt, stream1, param))
	}
	log.Debug("============================= 1 =============================")

	// Case 0: define new subscribers
	rxSub1, err := getJetStreamPushSubscriber(utCtxt, js, stream1, subject1, consumer1, nil)
	assert.Nil(err)
	log.Debug("============================= 2 =============================")

	internalErrorHandler := func(err error) {
		assert.Equal("nats: connection closed", err.Error())
	}

	// Case 1: start reading messages
	rxChan1 := make(chan *nats.Msg, 1)
	msgHandler1 := func(_ context.Context, msg *nats.Msg) error {
		rxChan1 <- msg
		return nil
	}
	assert.Nil(rxSub1.StartReading(msgHandler1, internalErrorHandler, &wg))
	log.Debug("============================= 3 =============================")

	publisher, err := GetJetStreamPublisher(js, testName)
	assert.Nil(err)

	// Case 2: publish a message
	msg2 := []byte(uuid.New().String())
	var encoded2 []byte
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(publisher.Publish(ctxt, subject1, msg2))
	}
	// verify receive
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		select {
		case <-ctxt.Done():
			assert.False(true)
		case msg, ok := <-rxChan1:
			assert.True(ok)
			assert.EqualValues(msg2, msg.Data)
			assert.Nil(msg.AckSync())
			enc, err := ConvertJSMessageDeliver(subject1, msg)
			assert.Nil(err)
			t, err := json.Marshal(&enc)
			assert.Nil(err)
			encoded2 = t
		}
	}

	log.WithFields(logTags).Debugf("Encoded message %s", encoded2)

	// Case 3: verify the decoded message matches
	{
		var parsed MsgToDeliver
		assert.Nil(json.Unmarshal(encoded2, &parsed))
		assert.EqualValues(msg2, parsed.Message)
	}
}
