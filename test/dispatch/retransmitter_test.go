package test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/dispatch"
	"gitlab.com/project-nan/httpmq/mocks"
)

func TestMessageRetransmitter(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	mockStorage := new(mocks.MessageQueues)
	mockMsgDispatch := new(mocks.MessageDispatch)

	tp, err := common.GetNewTaskProcessorInstance("unit-test", 4)
	assert.Nil(err)

	testQueue := "unit-test"
	uut, err := dispatch.DefineMessageRetransmit(
		testQueue, tp, mockStorage, time.Second, mockMsgDispatch.SubmitRetransmit,
	)
	assert.Nil(err)

	// Start the task processor
	wg := sync.WaitGroup{}
	assert.Nil(tp.StartEventLoop(&wg))

	rand.Seed(time.Now().UnixNano())
	msgIndexStart := int64(rand.Intn(100000))

	testMsg := make([]common.Message, 4)
	for idx := 0; idx < 4; idx++ {
		testMsg[idx] = common.Message{
			Source: common.MessageSource{
				Sender: uuid.New().String(),
				SentAt: time.Now().UTC(),
			},
			Destination: common.MessageDestination{
				TargetQueue: testQueue,
			},
			Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
			ReceivedAt: time.Now().UTC(),
			Body:       []byte(uuid.New().String()),
		}
	}

	// Case 0: request retransmission
	{
		mockStorage.On(
			"Read", testQueue, msgIndexStart, time.Second,
		).Return(testMsg[0], nil).Once()
		mockStorage.On(
			"Read", testQueue, msgIndexStart+1, time.Second,
		).Return(testMsg[1], nil).Once()
		mockMsgDispatch.On(
			"SubmitRetransmit", dispatch.MessageInFlight{
				Message: testMsg[0], Index: msgIndexStart, Redelivery: true,
			},
		).Return(nil).Once()
		mockMsgDispatch.On(
			"SubmitRetransmit", dispatch.MessageInFlight{
				Message: testMsg[1], Index: msgIndexStart + 1, Redelivery: true,
			},
		).Return(nil).Once()
		assert.Nil(
			uut.RetransmitMessages([]int64{msgIndexStart, msgIndexStart + 1}),
		)
	}

	// Case 1: request retransmission of same indexes
	{
		mockMsgDispatch.On(
			"SubmitRetransmit", dispatch.MessageInFlight{
				Message: testMsg[0], Index: msgIndexStart, Redelivery: true,
			},
		).Return(nil).Once()
		mockMsgDispatch.On(
			"SubmitRetransmit", dispatch.MessageInFlight{
				Message: testMsg[1], Index: msgIndexStart + 1, Redelivery: true,
			},
		).Return(nil).Once()
		assert.Nil(
			uut.RetransmitMessages([]int64{msgIndexStart, msgIndexStart + 1}),
		)
	}

	// Case 2: request retransmission of others
	{
		mockStorage.On(
			"Read", testQueue, msgIndexStart+2, time.Second,
		).Return(testMsg[2], nil).Once()
		mockStorage.On(
			"Read", testQueue, msgIndexStart+3, time.Second,
		).Return(testMsg[3], nil).Once()
		mockMsgDispatch.On(
			"SubmitRetransmit", dispatch.MessageInFlight{
				Message: testMsg[2], Index: msgIndexStart + 2, Redelivery: true,
			},
		).Return(nil).Once()
		mockMsgDispatch.On(
			"SubmitRetransmit", dispatch.MessageInFlight{
				Message: testMsg[3], Index: msgIndexStart + 3, Redelivery: true,
			},
		).Return(nil).Once()
		assert.Nil(
			uut.RetransmitMessages([]int64{msgIndexStart + 2, msgIndexStart + 3}),
		)
	}

	// Case 3: ACK messages
	assert.Nil(uut.ReceivedACKs([]int64{msgIndexStart + 1, msgIndexStart + 2}))

	// Case 4: request restransmission
	{
		mockStorage.On(
			"Read", testQueue, msgIndexStart+1, time.Second,
		).Return(testMsg[1], nil).Once()
		mockMsgDispatch.On(
			"SubmitRetransmit", dispatch.MessageInFlight{
				Message: testMsg[0], Index: msgIndexStart, Redelivery: true,
			},
		).Return(nil).Once()
		mockMsgDispatch.On(
			"SubmitRetransmit", dispatch.MessageInFlight{
				Message: testMsg[1], Index: msgIndexStart + 1, Redelivery: true,
			},
		).Return(nil).Once()
		assert.Nil(
			uut.RetransmitMessages([]int64{msgIndexStart, msgIndexStart + 1}),
		)
	}

	assert.Nil(tp.StopEventLoop())
	wg.Wait()
}
