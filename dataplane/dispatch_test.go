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
		assert.Nil(jsCtrl.CreateStream(streamParam))
	}
	consumer1 := uuid.New().String()
	maxInflight := 2
	{
		param := management.JetStreamConsumerParam{
			Name: consumer1, MaxInflight: maxInflight, Mode: "push", FilterSubject: &subject1,
		}
		assert.Nil(jsCtrl.CreateConsumerForStream(stream1, param))
	}
	log.Debug("============================= 1 =============================")

	msgRxChan := make(chan *nats.Msg, maxInflight)
	msgHandler := func(msg *nats.Msg, _ context.Context) error {
		msgRxChan <- msg
		return nil
	}

	// Case 0: start a new dispatcher
	uut, err := GetPushMessageDispatcher(
		js, stream1, subject1, consumer1, nil, maxInflight, &wg, utCtxt,
	)
	assert.Nil(err)
	assert.Nil(uut.Start(msgHandler))
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
		assert.Nil(publisher.Publish(subject1, msg1, ctxt))
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
		assert.Nil(publisher.Publish(subject1, msg2, ctxt))
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
		assert.Nil(publisher.Publish(subject1, msg3, ctxt))
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
		AckIndication{Stream: stream1, Consumer: consumer1, SeqNum: msg1SeqNum},
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
