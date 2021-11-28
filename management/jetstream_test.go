package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"gitlab.com/project-nan/httpmq/core"
)

func TestJetStreamControllerStreams(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := uuid.New().String()

	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "management_test",
		"component": "JetStreamController",
		"instance":  "streams",
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

	uut, err := GetJetStreamController(js, testName)
	assert.Nil(err)

	// Case 0: no streams in system
	{
		dummyStream := fmt.Sprintf("%s-00", testName)
		_, err := uut.GetStream(dummyStream)
		assert.NotNil(err)
		assert.NotNil(uut.DeleteStream(dummyStream))
	}

	// Case 1: create stream
	stream1 := fmt.Sprintf("%s-01", testName)
	subjects1 := []string{
		fmt.Sprintf("%s-1-0", testName),
		fmt.Sprintf("%s-1-1", testName),
		fmt.Sprintf("%s-1-2", testName),
	}
	{
		maxAge := time.Second
		streamParam := JSStreamParam{
			Name:     stream1,
			Subjects: subjects1,
			JSStreamLimits: JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateStream(streamParam))
		param, err := uut.GetStream(stream1)
		assert.Nil(err)
		assert.Equal(stream1, param.Config.Name)
		assert.EqualValues(subjects1, param.Config.Subjects)
	}
	// reuse the name with different param
	{
		subjects := []string{fmt.Sprintf("%s-1-0", testName), fmt.Sprintf("%s-1-1", testName)}
		streamParam := JSStreamParam{
			Name:     stream1,
			Subjects: subjects,
		}
		assert.NotNil(uut.CreateStream(streamParam))
	}

	// Case 2: delete stream
	assert.Nil(uut.DeleteStream(stream1))
	{
		_, err := uut.GetStream(stream1)
		assert.NotNil(err)
	}

	// Case 3: change stream param
	stream3 := fmt.Sprintf("%s-03", testName)
	subjects3 := []string{fmt.Sprintf("%s-3-0", testName), fmt.Sprintf("%s-3-1", testName)}
	{
		maxAge := time.Second * 30
		streamParam := JSStreamParam{
			Name:     stream3,
			Subjects: subjects3,
			JSStreamLimits: JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateStream(streamParam))
		streamInfo, err := uut.GetStream(stream3)
		assert.Nil(err)
		assert.Equal(stream3, streamInfo.Config.Name)
		assert.EqualValues(subjects3, streamInfo.Config.Subjects)
	}
	{
		subjects3 = append(subjects3, fmt.Sprintf("%s-3-3", testName))
		assert.Nil(uut.ChangeStreamSubjects(stream3, subjects3))
		maxMsgPerSub := int64(32)
		newParam := JSStreamLimits{MaxMsgsPerSubject: &maxMsgPerSub}
		assert.Nil(uut.UpdateStreamLimits(stream3, newParam))
	}
	{
		streamInfo, err := uut.GetStream(stream3)
		assert.Nil(err)
		assert.Equal(stream3, streamInfo.Config.Name)
		assert.Equal(int64(32), streamInfo.Config.MaxMsgsPerSubject)
		assert.EqualValues(subjects3, streamInfo.Config.Subjects)
	}

	// Case 4: delete stream
	assert.Nil(uut.DeleteStream(stream3))
	{
		_, err := uut.GetStream(stream3)
		assert.NotNil(err)
	}

	// Case 5: create multiple streams
	stream5s := []string{}
	for itr := 0; itr < 3; itr++ {
		stream5s = append(stream5s, fmt.Sprintf("%s-05-%d", testName, itr))
	}
	topics5s := map[string][]string{}
	for idx, streamName := range stream5s {
		topics5s[streamName] = []string{fmt.Sprintf("%s-05-%d", testName, idx)}
	}
	for _, streamName := range stream5s {
		subjs, ok := topics5s[streamName]
		assert.True(ok)
		maxAge := time.Second * 30
		streamParam := JSStreamParam{
			Name:     streamName,
			Subjects: subjs,
			JSStreamLimits: JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateStream(streamParam))
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		allStreams := uut.GetAllStreams(ctxt)
		for _, streamName := range stream5s {
			subjs, ok := topics5s[streamName]
			assert.True(ok)
			info, ok := allStreams[streamName]
			assert.True(ok)
			assert.Equal(streamName, info.Config.Name)
			assert.EqualValues(subjs, info.Config.Subjects)
		}
	}
}

func TestJetStreamControllerConsumers(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := uuid.New().String()

	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "management_test",
		"component": "JetStreamController",
		"instance":  "consumers",
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

	uut, err := GetJetStreamController(js, testName)
	assert.Nil(err)

	// Case 0: create consumer with no streams
	{
		consumerParam := JetStreamConsumerParam{
			Name: uuid.New().String(), MaxInflight: 1, Mode: "push",
		}
		assert.NotNil(uut.CreateConsumerForStream(uuid.New().String(), consumerParam))
	}

	// Define two streams for operating
	stream1 := fmt.Sprintf("%s-01", testName)
	subjects1 := []string{
		fmt.Sprintf("%s-1-0", testName),
		fmt.Sprintf("%s-1-1", testName),
		fmt.Sprintf("%s-1-2", testName),
	}
	stream2 := fmt.Sprintf("%s-02", testName)
	subjects2 := []string{fmt.Sprintf("%s-2-0", testName), fmt.Sprintf("%s-2-1", testName)}
	{
		maxAge := time.Second
		streamParam := JSStreamParam{
			Name:     stream1,
			Subjects: subjects1,
			JSStreamLimits: JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateStream(streamParam))
		streamParam = JSStreamParam{
			Name:     stream2,
			Subjects: subjects2,
			JSStreamLimits: JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateStream(streamParam))
	}

	// Case 1: create consumer
	consumer1 := uuid.New().String()
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push",
		}
		assert.Nil(uut.CreateConsumerForStream(stream1, param))
	}

	// Case 2: re-use the consumer again with the same param
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push",
		}
		assert.Nil(uut.CreateConsumerForStream(stream1, param))
	}
	// With different params
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 2, Mode: "push",
		}
		assert.NotNil(uut.CreateConsumerForStream(stream1, param))
	}

	// Case 3: re-use the consumer again on a different stream
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push",
		}
		assert.Nil(uut.CreateConsumerForStream(stream2, param))
	}

	// Case 4: verify the consumers are listed
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		stream1Consumers := uut.GetAllConsumersForStream(stream1, ctxt)
		assert.Len(stream1Consumers, 1)
		stream1Con1, ok := stream1Consumers[consumer1]
		assert.True(ok)
		assert.Equal(consumer1, stream1Con1.Config.Durable)
		stream2Consumers := uut.GetAllConsumersForStream(stream2, ctxt)
		assert.Len(stream2Consumers, 1)
		stream2Con1, ok := stream2Consumers[consumer1]
		assert.True(ok)
		assert.Equal(consumer1, stream2Con1.Config.Durable)
	}

	// Case 5: delete unknown consumer
	assert.NotNil(uut.DeleteConsumerOnStream(stream1, uuid.New().String()))

	// Case 6: delete consumer from stream1
	assert.Nil(uut.DeleteConsumerOnStream(stream1, consumer1))
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		stream1Consumers := uut.GetAllConsumersForStream(stream1, ctxt)
		assert.Empty(stream1Consumers)
		stream2Consumers := uut.GetAllConsumersForStream(stream2, ctxt)
		assert.Len(stream2Consumers, 1)
		stream2Con1, ok := stream2Consumers[consumer1]
		assert.True(ok)
		assert.Equal(consumer1, stream2Con1.Config.Durable)
	}

	// Case 7: create pull consumer
	consumer7 := uuid.New().String()
	{
		param := JetStreamConsumerParam{
			Name: consumer7, MaxInflight: 1, Mode: "pull",
		}
		assert.Nil(uut.CreateConsumerForStream(stream1, param))
	}

	// Case 8: create pull consumer, but with delivery group
	consumer8 := uuid.New().String()
	{
		group := "receiver-group"
		param := JetStreamConsumerParam{
			Name: consumer8, MaxInflight: 1, Mode: "pull", DeliveryGroup: &group,
		}
		assert.NotNil(uut.CreateConsumerForStream(stream2, param))
	}
}
