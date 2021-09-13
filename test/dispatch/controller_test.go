package test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/dispatch"
	"gitlab.com/project-nan/httpmq/mocks"
	"gitlab.com/project-nan/httpmq/storage"
)

func TestController(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	mockMsgDispatch := new(mocks.MessageDispatch)
	mockMsgRetrans := new(mocks.MessageRetransmit)
	mockQueueReader := new(mocks.MessageFetch)

	_, testStore, err := storage.CreateEtcdBackedStorage(
		[]string{"localhost:2379"}, time.Second*5,
	)
	assert.Nil(err)

	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp, err := common.GetNewTaskProcessorInstance("unit-test", 4, ctxt)
	assert.Nil(err)

	testQueue := "unit-test"
	testClient := "unit-tester"
	testPrefix := uuid.New().String()
	expectedStoreKey := fmt.Sprintf(
		"%s/client/%s/queue/%s", testPrefix, testClient, testQueue,
	)
	maxRetrans := 2

	uut, err := dispatch.DefineController(
		testClient,
		testQueue,
		testStore,
		tp,
		testPrefix,
		maxRetrans,
		ctxt,
		mockMsgRetrans.RetransmitMessages,
		mockMsgRetrans.ReceivedACKs,
		mockMsgDispatch.SubmitMessageACK,
		mockQueueReader.StartReading,
	)
	assert.Nil(err)

	// Start the task processor
	assert.Nil(tp.StartEventLoop(&wg))

	rand.Seed(time.Now().UnixNano())

	// Case 0: interact before the system is ready
	{
		assert.NotNil(uut.ReceivedACKs([]int64{rand.Int63n(10000), rand.Int63n(10000)}, ctxt))
		assert.NotNil(uut.RegisterInflight(rand.Int63(), time.Now(), ctxt, true))
	}

	startIndex := rand.Int63n(1000000)
	startTime := time.Now()

	// Two inflight messages
	msgP0ID := startIndex - 2
	msgP1ID := startIndex - 1
	currentState := dispatch.QueueSubInfo{
		NewestACKedIndex: startIndex,
		Inflight: map[int64]dispatch.InfightMessageInfo{
			msgP0ID: {
				Index:       msgP0ID,
				FirstSentAt: startTime.Add(-time.Minute * 2),
				RetryCount:  1,
				MaxRetry:    3,
			},
			msgP1ID: {
				Index:       msgP1ID,
				FirstSentAt: startTime.Add(-time.Minute),
				RetryCount:  0,
				MaxRetry:    2,
			},
		},
	}
	assert.Nil(testStore.Set(expectedStoreKey, currentState, ctxt))

	// Case 1: start the controller
	{
		ran := false
		mockMsgRetrans.On(
			"RetransmitMessages",
			mock.AnythingOfType("[]int64"),
			mock.AnythingOfType("*context.cancelCtx"),
		).Run(func(args mock.Arguments) {
			indexes := args.Get(0).([]int64)
			assert.Equal(2, len(indexes))
			m := map[int64]bool{}
			for _, val := range indexes {
				if val == msgP0ID || val == msgP1ID {
					m[val] = true
				}
			}
			assert.Equal(2, len(m))
			ran = true
		}).Return(nil).Once()
		mockQueueReader.On(
			"StartReading", startIndex+1,
		).Return(nil).Once()
		assert.Nil(uut.Start(ctxt))
		assert.True(ran)
	}
	// Verify change in state
	var newState dispatch.QueueSubInfo
	assert.Nil(testStore.Get(expectedStoreKey, &newState, ctxt))
	{
		val, ok := newState.Inflight[msgP0ID]
		assert.True(ok)
		assert.Equal(2, val.RetryCount)
		val, ok = newState.Inflight[msgP1ID]
		assert.True(ok)
		assert.Equal(1, val.RetryCount)
	}

	// Case 2: trigger start again
	{
		assert.NotNil(uut.Start(ctxt))
	}

	// Case 3: run retransmit check again
	{
		ran := false
		mockMsgRetrans.On(
			"RetransmitMessages",
			mock.AnythingOfType("[]int64"),
			mock.AnythingOfType("*context.cancelCtx"),
		).Run(func(args mock.Arguments) {
			indexes := args.Get(0).([]int64)
			assert.Equal(2, len(indexes))
			m := map[int64]bool{}
			for _, val := range indexes {
				if val == msgP0ID || val == msgP1ID {
					m[val] = true
				}
			}
			assert.Equal(2, len(m))
			ran = true
		}).Return(nil).Once()
		assert.Nil(
			uut.TriggerMessageRetransmit(startTime.Add(time.Second*30), time.Second*30, ctxt),
		)
		time.Sleep(time.Millisecond * 100)
		assert.True(ran)
	}
	// Verify change in state
	var newState3 dispatch.QueueSubInfo
	assert.Nil(testStore.Get(expectedStoreKey, &newState3, ctxt))
	{
		val, ok := newState3.Inflight[msgP0ID]
		assert.True(ok)
		assert.Equal(3, val.RetryCount)
		val, ok = newState3.Inflight[msgP1ID]
		assert.True(ok)
		assert.Equal(2, val.RetryCount)
	}

	// Case 4: ACK one message
	{
		mockMsgRetrans.On(
			"ReceivedACKs",
			[]int64{msgP1ID},
			mock.AnythingOfType("*context.cancelCtx"),
		).Return(nil).Once()
		mockMsgDispatch.On(
			"SubmitMessageACK",
			[]int64{msgP1ID},
			mock.AnythingOfType("*context.cancelCtx"),
		).Return(nil).Once()
		assert.Nil(uut.ReceivedACKs([]int64{msgP1ID}, ctxt))
	}
	// Verify change in state
	var newState4 dispatch.QueueSubInfo
	assert.Nil(testStore.Get(expectedStoreKey, &newState4, ctxt))
	{
		val, ok := newState4.Inflight[msgP0ID]
		assert.True(ok)
		assert.Equal(3, val.RetryCount)
		_, ok = newState4.Inflight[msgP1ID]
		assert.False(ok)
	}

	// Case 5: register more messages
	msg5ID := startIndex + 3
	msg5SendTime := startTime.Add(time.Second * 45)
	assert.Nil(uut.RegisterInflight(msg5ID, msg5SendTime, ctxt, true))
	// Verify change in state
	var newState5 dispatch.QueueSubInfo
	assert.Nil(testStore.Get(expectedStoreKey, &newState5, ctxt))
	{
		val, ok := newState5.Inflight[msgP0ID]
		assert.True(ok)
		assert.Equal(3, val.RetryCount)
		val, ok = newState5.Inflight[msg5ID]
		assert.True(ok)
		assert.Equal(0, val.RetryCount)
		assert.Equal(maxRetrans, val.MaxRetry)
	}

	// Case 6: run retransmit check again
	{
		ran := false
		mockMsgRetrans.On(
			"ReceivedACKs",
			[]int64{msgP0ID},
			mock.AnythingOfType("*context.cancelCtx"),
		).Return(nil).Once()
		mockMsgDispatch.On(
			"SubmitMessageACK",
			[]int64{msgP0ID},
			mock.AnythingOfType("*context.cancelCtx"),
		).Run(func(args mock.Arguments) {
			ran = true
		}).Return(nil).Once()
		assert.Nil(
			uut.TriggerMessageRetransmit(startTime.Add(time.Second*60), time.Second*30, ctxt),
		)
		time.Sleep(time.Millisecond * 100)
		assert.True(ran)
	}
	// Verify change in state
	var newState6 dispatch.QueueSubInfo
	assert.Nil(testStore.Get(expectedStoreKey, &newState6, ctxt))
	{
		val, ok := newState6.Inflight[msgP0ID]
		assert.False(ok)
		val, ok = newState6.Inflight[msg5ID]
		assert.True(ok)
		assert.Equal(0, val.RetryCount)
		assert.Equal(maxRetrans, val.MaxRetry)
	}

	// Case 7: run retransmit check again
	{
		ran := false
		mockMsgRetrans.On(
			"RetransmitMessages",
			mock.AnythingOfType("[]int64"),
			mock.AnythingOfType("*context.cancelCtx"),
		).Run(func(args mock.Arguments) {
			indexes := args.Get(0).([]int64)
			assert.Equal(1, len(indexes))
			assert.Equal(msg5ID, indexes[0])
			ran = true
		}).Return(nil).Once()
		assert.Nil(
			uut.TriggerMessageRetransmit(startTime.Add(time.Second*90), time.Second*30, ctxt),
		)
		time.Sleep(time.Millisecond * 100)
		assert.True(ran)
	}
	// Verify change in state
	var newState7 dispatch.QueueSubInfo
	assert.Nil(testStore.Get(expectedStoreKey, &newState7, ctxt))
	{
		val, ok := newState7.Inflight[msg5ID]
		assert.True(ok)
		assert.Equal(1, val.RetryCount)
		assert.Equal(maxRetrans, val.MaxRetry)
	}
}
