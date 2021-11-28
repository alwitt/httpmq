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
	"gitlab.com/project-nan/httpmq/core"
	"gitlab.com/project-nan/httpmq/management"
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
		assert.Nil(jsCtrl.CreateStream(streamParam))
	}
	consumer1 := uuid.New().String()
	consumer2 := uuid.New().String()
	consumer3 := uuid.New().String()
	{
		param := management.JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push", FilterSubject: &subject1,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(stream1, param))
		param = management.JetStreamConsumerParam{
			Name: consumer2, MaxInflight: 1, Mode: "push", FilterSubject: &subject2,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(stream1, param))
		param = management.JetStreamConsumerParam{
			Name: consumer3, MaxInflight: 1, Mode: "push", FilterSubject: &subject3,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(stream1, param))
	}
	log.Debug("============================= 1 =============================")

	// Case 0: define new subscribers
	rxSub1, err := getJetStreamPushSubscriber(js, stream1, subject1, consumer1, nil)
	assert.Nil(err)
	rxSub2, err := getJetStreamPushSubscriber(js, stream1, subject2, consumer2, nil)
	assert.Nil(err)
	rxSub3, err := getJetStreamPushSubscriber(js, stream1, subject3, consumer3, nil)
	assert.Nil(err)
	log.Debug("============================= 2 =============================")

	// Case 1: start reading messages
	rxChan1 := make(chan *nats.Msg, 1)
	msgHandler1 := func(msg *nats.Msg, _ context.Context) error {
		rxChan1 <- msg
		return nil
	}
	rxChan2 := make(chan *nats.Msg, 1)
	msgHandler2 := func(msg *nats.Msg, _ context.Context) error {
		rxChan2 <- msg
		return nil
	}
	rxChan3 := make(chan *nats.Msg, 1)
	msgHandler3 := func(msg *nats.Msg, _ context.Context) error {
		rxChan3 <- msg
		return nil
	}
	assert.Nil(rxSub1.StartReading(msgHandler1, &wg, utCtxt))
	assert.NotNil(rxSub1.StartReading(msgHandler1, &wg, utCtxt))
	assert.Nil(rxSub2.StartReading(msgHandler2, &wg, utCtxt))
	assert.Nil(rxSub3.StartReading(msgHandler3, &wg, utCtxt))
	log.Debug("============================= 3 =============================")

	publisher, err := GetJetStreamPublisher(js, testName)
	assert.Nil(err)

	// Case 2: publish a message
	msg2 := []byte(uuid.New().String())
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		assert.Nil(publisher.Publish(subject1, msg2, ctxt))
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
		assert.Nil(publisher.Publish(subject2, msg3, ctxt))
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
		assert.Nil(jsCtrl.CreateStream(streamParam))
	}
	consumer1 := uuid.New().String()
	group1 := uuid.New().String()
	{
		param := management.JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push", DeliveryGroup: &group1,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(stream1, param))
	}
	log.Debug("============================= 1 =============================")

	// Case 0: define new subscribers
	rxSub1, err := getJetStreamPushSubscriber(js1, stream1, subject1, consumer1, &group1)
	assert.Nil(err)
	rxSub2, err := getJetStreamPushSubscriber(js2, stream1, subject1, consumer1, &group1)
	assert.Nil(err)
	log.Debug("============================= 2 =============================")

	// Case 1: start reading messages
	type msgTuple struct {
		msg *nats.Msg
		id  int
	}
	rxChan := make(chan msgTuple, 1)
	msgHandler1 := func(msg *nats.Msg, _ context.Context) error {
		rxChan <- msgTuple{msg: msg, id: 1}
		return nil
	}
	msgHandler2 := func(msg *nats.Msg, _ context.Context) error {
		rxChan <- msgTuple{msg: msg, id: 2}
		return nil
	}
	assert.Nil(rxSub1.StartReading(msgHandler1, &wg, utCtxt))
	assert.Nil(rxSub2.StartReading(msgHandler2, &wg, utCtxt))
	log.Debug("============================= 3 =============================")

	publisher, err := GetJetStreamPublisher(js1, testName)
	assert.Nil(err)

	// Case 2: send messages repeatedly
	rxID := make(map[int]int)
	for itr := 0; itr < 10; itr++ {
		testMsg := []byte(uuid.New().String())
		{
			ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
			defer cancel()
			assert.Nil(publisher.Publish(subject1, testMsg, ctxt))
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
