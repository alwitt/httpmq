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

func TestJetStreamControllerQueues(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-js-queues"

	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "management_test",
		"component": "JetStreamController",
		"instance":  "queues",
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

	// Clear out current queues in JetStream
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		existing := uut.GetAllQueues(ctxt)
		for queue := range existing {
			assert.Nil(uut.DeleteQueue(queue))
		}
	}

	// Case 0: no queues in system
	{
		dummyQueue := fmt.Sprintf("%s-00", testName)
		_, err := uut.GetQueue(dummyQueue)
		assert.NotNil(err)
		assert.NotNil(uut.DeleteQueue(dummyQueue))
	}

	// Case 1: create queue
	queue1 := fmt.Sprintf("%s-01", testName)
	subjects1 := []string{"topic-1-0", "topic-1-1", "topic-1-2"}
	{
		maxAge := time.Second
		queueParam := JetStreamQueueParam{
			Name:     queue1,
			Subjects: subjects1,
			JetStreamQueueLimits: JetStreamQueueLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateQueue(queueParam))
		param, err := uut.GetQueue(queue1)
		assert.Nil(err)
		assert.Equal(queue1, param.Config.Name)
		assert.EqualValues(subjects1, param.Config.Subjects)
	}
	// reuse the name with different param
	{
		subjects := []string{"topic-1-0", "topic-1-1"}
		queueParam := JetStreamQueueParam{
			Name:     queue1,
			Subjects: subjects,
		}
		assert.NotNil(uut.CreateQueue(queueParam))
	}

	// Case 2: delete queue
	assert.Nil(uut.DeleteQueue(queue1))
	{
		_, err := uut.GetQueue(queue1)
		assert.NotNil(err)
	}

	// Case 3: change queue param
	queue3 := fmt.Sprintf("%s-03", testName)
	subjects3 := []string{"topic-3-0", "topic-3-1"}
	{
		maxAge := time.Second * 30
		queueParam := JetStreamQueueParam{
			Name:     queue3,
			Subjects: subjects3,
			JetStreamQueueLimits: JetStreamQueueLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateQueue(queueParam))
		queueInfo, err := uut.GetQueue(queue3)
		assert.Nil(err)
		assert.Equal(queue3, queueInfo.Config.Name)
		assert.EqualValues(subjects3, queueInfo.Config.Subjects)
	}
	{
		subjects3 = append(subjects3, "topic-3-3")
		assert.Nil(uut.ChangeQueueSubjects(queue3, subjects3))
		maxMsgPerSub := int64(32)
		newParam := JetStreamQueueLimits{MaxMsgsPerSubject: &maxMsgPerSub}
		assert.Nil(uut.UpdateQueueLimits(queue3, newParam))
	}
	{
		queueInfo, err := uut.GetQueue(queue3)
		assert.Nil(err)
		assert.Equal(queue3, queueInfo.Config.Name)
		assert.Equal(int64(32), queueInfo.Config.MaxMsgsPerSubject)
		assert.EqualValues(subjects3, queueInfo.Config.Subjects)
	}

	// Case 4: delete queue
	assert.Nil(uut.DeleteQueue(queue3))
	{
		_, err := uut.GetQueue(queue3)
		assert.NotNil(err)
	}

	// Case 5: create multiple queues
	queue5s := []string{}
	for itr := 0; itr < 3; itr++ {
		queue5s = append(queue5s, fmt.Sprintf("%s-05-%d", testName, itr))
	}
	topics5s := map[string][]string{}
	for idx, queueName := range queue5s {
		topics5s[queueName] = []string{fmt.Sprintf("%s-05-%d", testName, idx)}
	}
	for _, queueName := range queue5s {
		subjs, ok := topics5s[queueName]
		assert.True(ok)
		maxAge := time.Second * 30
		queueParam := JetStreamQueueParam{
			Name:     queueName,
			Subjects: subjs,
			JetStreamQueueLimits: JetStreamQueueLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateQueue(queueParam))
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		allQueues := uut.GetAllQueues(ctxt)
		for _, queueName := range queue5s {
			subjs, ok := topics5s[queueName]
			assert.True(ok)
			info, ok := allQueues[queueName]
			assert.True(ok)
			assert.Equal(queueName, info.Config.Name)
			assert.EqualValues(subjs, info.Config.Subjects)
		}
	}

	// Case 6: clean up all queues
	for _, queueName := range queue5s {
		assert.Nil(uut.DeleteQueue(queueName))
	}
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		allQueues := uut.GetAllQueues(ctxt)
		assert.Empty(allQueues)
	}
}

func TestJetStreamControllerConsumers(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-js-consumers"

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

	// Clear out current queues in JetStream
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		existing := uut.GetAllQueues(ctxt)
		for queue := range existing {
			// Clear out any consumer attached to the queue
			consumers := uut.GetAllConsumersForQueue(queue, ctxt)
			for consumer := range consumers {
				assert.Nil(uut.DeleteConsumerOnQueue(queue, consumer))
			}
			assert.Nil(uut.DeleteQueue(queue))
		}
	}

	// Case 0: create consumer with no queues
	{
		consumerParam := JetStreamConsumerParam{
			Name: uuid.New().String(), MaxInflight: 1, Mode: "push",
		}
		assert.NotNil(uut.CreateConsumerForQueue(uuid.New().String(), consumerParam))
	}

	// Define two queues for operating
	queue1 := fmt.Sprintf("%s-01", testName)
	subjects1 := []string{"topic-1-0", "topic-1-1", "topic-1-2"}
	queue2 := fmt.Sprintf("%s-02", testName)
	subjects2 := []string{"topic-2-0", "topic-2-1"}
	{
		maxAge := time.Second
		queueParam := JetStreamQueueParam{
			Name:     queue1,
			Subjects: subjects1,
			JetStreamQueueLimits: JetStreamQueueLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateQueue(queueParam))
		queueParam = JetStreamQueueParam{
			Name:     queue2,
			Subjects: subjects2,
			JetStreamQueueLimits: JetStreamQueueLimits{
				MaxAge: &maxAge,
			},
		}
		assert.Nil(uut.CreateQueue(queueParam))
	}

	// Case 1: create consumer
	consumer1 := uuid.New().String()
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push",
		}
		assert.Nil(uut.CreateConsumerForQueue(queue1, param))
	}

	// Case 2: re-use the consumer again with the same param
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push",
		}
		assert.Nil(uut.CreateConsumerForQueue(queue1, param))
	}
	// With different params
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 2, Mode: "push",
		}
		assert.NotNil(uut.CreateConsumerForQueue(queue1, param))
	}

	// Case 3: re-use the consumer again on a different queue
	{
		param := JetStreamConsumerParam{
			Name: consumer1, MaxInflight: 1, Mode: "push",
		}
		assert.Nil(uut.CreateConsumerForQueue(queue2, param))
	}

	// Case 4: verify the consumers are listed
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		queue1Consumers := uut.GetAllConsumersForQueue(queue1, ctxt)
		assert.Len(queue1Consumers, 1)
		queue1Con1, ok := queue1Consumers[consumer1]
		assert.True(ok)
		assert.Equal(consumer1, queue1Con1.Config.Durable)
		queue2Consumers := uut.GetAllConsumersForQueue(queue2, ctxt)
		assert.Len(queue2Consumers, 1)
		queue2Con1, ok := queue2Consumers[consumer1]
		assert.True(ok)
		assert.Equal(consumer1, queue2Con1.Config.Durable)
	}

	// Case 5: delete unknown consumer
	assert.NotNil(uut.DeleteConsumerOnQueue(queue1, uuid.New().String()))

	// Case 6: delete consumer from queue1
	assert.Nil(uut.DeleteConsumerOnQueue(queue1, consumer1))
	{
		ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
		defer cancel()
		queue1Consumers := uut.GetAllConsumersForQueue(queue1, ctxt)
		assert.Empty(queue1Consumers)
		queue2Consumers := uut.GetAllConsumersForQueue(queue2, ctxt)
		assert.Len(queue2Consumers, 1)
		queue2Con1, ok := queue2Consumers[consumer1]
		assert.True(ok)
		assert.Equal(consumer1, queue2Con1.Config.Durable)
	}

	// Case 7: create pull consumer
	consumer7 := uuid.New().String()
	{
		param := JetStreamConsumerParam{
			Name: consumer7, MaxInflight: 1, Mode: "pull",
		}
		assert.Nil(uut.CreateConsumerForQueue(queue1, param))
	}

	// Case 8: create pull consumer, but with delivery group
	consumer8 := uuid.New().String()
	{
		group := "receiver-group"
		param := JetStreamConsumerParam{
			Name: consumer8, MaxInflight: 1, Mode: "pull", DeliveryGroup: &group,
		}
		assert.NotNil(uut.CreateConsumerForQueue(queue2, param))
	}
}
