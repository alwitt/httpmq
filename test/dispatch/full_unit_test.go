package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/dispatch"
	"gitlab.com/project-nan/httpmq/storage"
)

func TestFullUnit(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()

	testQueue, testStore, err := storage.CreateEtcdBackedStorage(
		[]string{"localhost:2379"}, time.Second*5,
	)
	assert.Nil(err)

	maxInflight := 2

	outputMsgs := make(chan dispatch.MessageInFlight, maxInflight*4)
	outputHandle := func(msg dispatch.MessageInFlight, useContext context.Context) error {
		select {
		case outputMsgs <- msg:
			break
		case <-useContext.Done():
			return useContext.Err()
		}
		return nil
	}

	queueName := uuid.New().String()
	initParam := dispatch.DispatcherInitParam{
		Client:                      "unit-tester",
		Queue:                       queueName,
		KeyPrefix:                   uuid.New().String(),
		MaxInflight:                 maxInflight,
		MessageForward:              outputHandle,
		DispatchUseBlockingRegister: true,
		ReadQueueTimeout:            time.Second,
		MaxRetry:                    1,
		RetryCheckInterval:          time.Millisecond * 100,
		WG:                          &wg,
		RootContext:                 ctxt,
		QueueInterface:              testQueue,
		QueueReadFailureMaxRetries:  4,
		StoreInterface:              testStore,
	}

	uut, err := dispatch.DefineDispatcher(initParam)
	assert.Nil(err)

	// Start the dispatcher
	assert.Nil(uut.Start())
	time.Sleep(time.Millisecond * 50)

	// Case 0: submit one message
	testMsgs0 := []common.Message{
		{
			Source: common.MessageSource{
				Sender: uuid.New().String(),
				SentAt: time.Now().UTC(),
			},
			Destination: common.MessageDestination{TargetQueue: queueName},
			ReceivedAt:  time.Now().UTC(),
			Tags:        map[string]string{"msg": "MSG.0.0"},
			Body:        []byte(uuid.New().String()),
		},
	}
	assert.Nil(testQueue.Write(testMsgs0[0], ctxt))
	// outputIndex0 := int64(0)
	{
		readCtxt, cancel := context.WithTimeout(ctxt, time.Second)
		defer cancel()
		select {
		case msg := <-outputMsgs:
			assert.EqualValues(testMsgs0[0], msg.Message)
			// outputIndex0 = msg.Index
		case <-readCtxt.Done():
			assert.Nil(readCtxt.Err())
		}
	}

	// Case 1: submit two messages
	testMsgs1 := []common.Message{}
	for i := 0; i < 2; i++ {
		testMsgs1 = append(testMsgs1, common.Message{
			Source: common.MessageSource{
				Sender: uuid.New().String(),
				SentAt: time.Now().UTC(),
			},
			Destination: common.MessageDestination{TargetQueue: queueName},
			ReceivedAt:  time.Now().UTC(),
			Tags:        map[string]string{"msg": fmt.Sprintf("MSG.1.%d", i)},
			Body:        []byte(uuid.New().String()),
		})
	}
	for _, msg := range testMsgs1 {
		assert.Nil(testQueue.Write(msg, ctxt))
	}
	// outputIndex1 := int64(0)
	{
		readCtxt, cancel := context.WithTimeout(ctxt, time.Second)
		defer cancel()
		select {
		case msg := <-outputMsgs:
			assert.EqualValues(testMsgs1[0], msg.Message)
			// outputIndex1 = msg.Index
		case <-readCtxt.Done():
			assert.Nil(readCtxt.Err())
		}
	}
	{
		// Should be only one message
		readCtxt, cancel := context.WithTimeout(ctxt, time.Millisecond*50)
		defer cancel()
		select {
		case <-outputMsgs:
			assert.False(true)
		case <-readCtxt.Done():
			assert.NotNil(readCtxt.Err())
		}
	}

	// TODO FIXME: continue after addressing starting index for queue read control

	// Stop the dispatcher
	time.Sleep(time.Millisecond * 50)
	assert.Nil(uut.Stop())
}
