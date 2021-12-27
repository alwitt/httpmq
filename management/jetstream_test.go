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

package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/core"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
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

	uut, err := GetJetStreamController(js, testName)
	assert.Nil(err)

	// Case 0: no streams in system
	{
		dummyStream := uuid.New().String()
		_, err := uut.GetStream(utCtxt, dummyStream)
		assert.NotNil(err)
		assert.NotNil(uut.DeleteStream(utCtxt, dummyStream))
	}

	// Case 1: create stream
	stream1 := uuid.New().String()
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
		assert.Nil(uut.CreateStream(utCtxt, streamParam))
		param, err := uut.GetStream(utCtxt, stream1)
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
		assert.NotNil(uut.CreateStream(utCtxt, streamParam))
	}

	// Case 2: delete stream
	assert.Nil(uut.DeleteStream(utCtxt, stream1))
	{
		_, err := uut.GetStream(utCtxt, stream1)
		assert.NotNil(err)
	}

	// Case 3: change stream param
	stream3 := uuid.New().String()
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
		assert.Nil(uut.CreateStream(utCtxt, streamParam))
		streamInfo, err := uut.GetStream(utCtxt, stream3)
		assert.Nil(err)
		assert.Equal(stream3, streamInfo.Config.Name)
		assert.EqualValues(subjects3, streamInfo.Config.Subjects)
	}
	{
		subjects3 = append(subjects3, fmt.Sprintf("%s-3-3", testName))
		assert.Nil(uut.ChangeStreamSubjects(utCtxt, stream3, subjects3))
		maxMsgPerSub := int64(32)
		newParam := JSStreamLimits{MaxMsgsPerSubject: &maxMsgPerSub}
		assert.Nil(uut.UpdateStreamLimits(utCtxt, stream3, newParam))
	}
	{
		streamInfo, err := uut.GetStream(utCtxt, stream3)
		assert.Nil(err)
		assert.Equal(stream3, streamInfo.Config.Name)
		assert.Equal(int64(32), streamInfo.Config.MaxMsgsPerSubject)
		assert.EqualValues(subjects3, streamInfo.Config.Subjects)
	}

	// Case 4: delete stream
	assert.Nil(uut.DeleteStream(utCtxt, stream3))
	{
		_, err := uut.GetStream(utCtxt, stream3)
		assert.NotNil(err)
	}

	// Case 5: create multiple streams
	stream5s := []string{}
	for itr := 0; itr < 3; itr++ {
		stream5s = append(stream5s, uuid.New().String())
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
		assert.Nil(uut.CreateStream(utCtxt, streamParam))
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

	uut, err := GetJetStreamController(js, testName)
	assert.Nil(err)

	// Case 0: create consumer with no streams
	{
		consumerParam := JetStreamConsumerParam{
			Name: uuid.New().String(), MaxInflight: 1, Mode: "push",
		}
		assert.NotNil(uut.CreateConsumerForStream(utCtxt, uuid.New().String(), consumerParam))
	}

	// Define two streams for operating
	stream1 := uuid.New().String()
	subjects1 := []string{
		fmt.Sprintf("%s-1-0", testName),
		fmt.Sprintf("%s-1-1", testName),
		fmt.Sprintf("%s-1-2", testName),
	}
	stream2 := uuid.New().String()
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
		assert.Nil(uut.CreateStream(utCtxt, streamParam))
		streamParam = JSStreamParam{
			Name:     stream2,
			Subjects: subjects2,
			JSStreamLimits: JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateStream(utCtxt, streamParam))
	}

	// Case 1: create consumer
	consumer1 := uuid.New().String()
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push",
		}
		assert.Nil(uut.CreateConsumerForStream(utCtxt, stream1, param))
	}

	// Case 2: re-use the consumer again with the same param
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push",
		}
		assert.Nil(uut.CreateConsumerForStream(utCtxt, stream1, param))
	}
	// With different params
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 2, Mode: "push",
		}
		assert.NotNil(uut.CreateConsumerForStream(utCtxt, stream1, param))
	}

	// Case 3: re-use the consumer again on a different stream
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push",
		}
		assert.Nil(uut.CreateConsumerForStream(utCtxt, stream2, param))
	}

	// Case 4: verify the consumers are listed
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		stream1Consumers := uut.GetAllConsumersForStream(ctxt, stream1)
		assert.Len(stream1Consumers, 1)
		stream1Con1, ok := stream1Consumers[consumer1]
		assert.True(ok)
		assert.Equal(consumer1, stream1Con1.Config.Durable)
		stream2Consumers := uut.GetAllConsumersForStream(ctxt, stream2)
		assert.Len(stream2Consumers, 1)
		stream2Con1, ok := stream2Consumers[consumer1]
		assert.True(ok)
		assert.Equal(consumer1, stream2Con1.Config.Durable)
	}

	// Case 5: delete unknown consumer
	assert.NotNil(uut.DeleteConsumerOnStream(utCtxt, stream1, uuid.New().String()))

	// Case 6: delete consumer from stream1
	assert.Nil(uut.DeleteConsumerOnStream(utCtxt, stream1, consumer1))
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		stream1Consumers := uut.GetAllConsumersForStream(ctxt, stream1)
		assert.Empty(stream1Consumers)
		stream2Consumers := uut.GetAllConsumersForStream(ctxt, stream2)
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
		assert.Nil(uut.CreateConsumerForStream(utCtxt, stream1, param))
	}

	// Case 8: create pull consumer, but with delivery group
	consumer8 := uuid.New().String()
	{
		group := "receiver-group"
		param := JetStreamConsumerParam{
			Name: consumer8, MaxInflight: 1, Mode: "pull", DeliveryGroup: &group,
		}
		assert.NotNil(uut.CreateConsumerForStream(utCtxt, stream2, param))
	}
}

func TestNameWithNoneStandardCharacters(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := uuid.New().String()

	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "management_test",
		"component": "JetStreamController",
		"instance":  "name-character-check",
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

	uut, err := GetJetStreamController(js, testName)
	assert.Nil(err)

	// Case 0: define stream with " "
	stream0 := fmt.Sprintf("%s 00", testName)
	subjects0 := []string{fmt.Sprintf("%s-0-0", testName)}
	{
		maxAge := time.Second
		streamParam := JSStreamParam{
			Name:     stream0,
			Subjects: subjects0,
			JSStreamLimits: JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.NotNil(uut.CreateStream(utCtxt, streamParam))
	}

	// Case 1: define stream
	stream1 := uuid.New().String()
	subjects1 := []string{fmt.Sprintf("%s-1-0", testName)}
	{
		maxAge := time.Second
		streamParam := JSStreamParam{
			Name:     stream1,
			Subjects: subjects1,
			JSStreamLimits: JSStreamLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateStream(utCtxt, streamParam))
	}

	// Case 2: define consumer with " "
	consumer2 := fmt.Sprintf("%s 02", uuid.New().String())
	{
		param := JetStreamConsumerParam{
			Name: consumer2, MaxInflight: 1, Mode: "push",
		}
		assert.NotNil(uut.CreateConsumerForStream(utCtxt, stream1, param))
	}

	// Case 3: define consumer, but specify stream with " "
	consumer3 := uuid.New().String()
	{
		param := JetStreamConsumerParam{
			Name: consumer3, MaxInflight: 1, Mode: "push",
		}
		assert.NotNil(uut.CreateConsumerForStream(utCtxt, fmt.Sprintf("%s 01", testName), param))
	}
}
