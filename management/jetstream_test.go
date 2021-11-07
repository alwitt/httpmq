package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apex/log"
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

	uut, err := GetJetStreamController(js, "ut-js-queues")
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
	topics1 := []string{"topic-1-0", "topic-1-1", "topic-1-2"}
	{
		queueParam := uut.GetBasicQueueParam(queue1, topics1, time.Second*30)
		assert.Nil(uut.CreateQueue(queueParam))
		param, err := uut.GetQueue(queue1)
		assert.Nil(err)
		assert.Equal(queue1, param.Config.Name)
		assert.EqualValues(topics1, param.Config.Subjects)
	}

	// Case 2: delete queue
	assert.Nil(uut.DeleteQueue(queue1))
	{
		_, err := uut.GetQueue(queue1)
		assert.NotNil(err)
	}

	// Case 3: change queue param
	queue3 := fmt.Sprintf("%s-03", testName)
	topics3 := []string{"topic-3-0", "topic-3-1"}
	{
		queueParam := uut.GetBasicQueueParam(queue3, topics3, time.Second*30)
		assert.Nil(uut.CreateQueue(queueParam))
		queueInfo, err := uut.GetQueue(queue3)
		assert.Nil(err)
		assert.Equal(queue3, queueInfo.Config.Name)
		assert.EqualValues(topics3, queueInfo.Config.Subjects)
	}
	{
		queueInfo, err := uut.GetQueue(queue3)
		assert.Nil(err)
		oldParam := queueInfo.Config
		oldParam.MaxMsgsPerSubject = 32
		topics3 = append(topics3, "topic-3-3")
		oldParam.Subjects = topics3
		assert.Nil(uut.UpdateQueueSetting(oldParam))
	}
	{
		queueInfo, err := uut.GetQueue(queue3)
		assert.Nil(err)
		assert.Equal(queue3, queueInfo.Config.Name)
		assert.Equal(int64(32), queueInfo.Config.MaxMsgsPerSubject)
		assert.EqualValues(topics3, queueInfo.Config.Subjects)
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
		queueParam := uut.GetBasicQueueParam(queueName, subjs, time.Second*30)
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
