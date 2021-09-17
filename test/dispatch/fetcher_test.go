package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	// apexJSON "github.com/apex/log/handlers/json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/dispatch"
	"gitlab.com/project-nan/httpmq/mocks"
	"gitlab.com/project-nan/httpmq/storage"
)

func TestMessageFetcher(t *testing.T) {
	assert := assert.New(t)
	// log.SetHandler(apexJSON.New(os.Stderr))
	log.SetLevel(log.DebugLevel)

	mockMsgDispatch := new(mocks.MessageDispatch)

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, utCancel := context.WithCancel(context.Background())
	defer utCancel()

	msgQueue, _, err := storage.CreateEtcdBackedStorage(
		[]string{"localhost:2379"}, time.Second*5,
	)
	assert.Nil(err)

	testTopic := uuid.New().String()
	uut, err := dispatch.DefineMessageFetcher(
		testTopic,
		&wg,
		msgQueue,
		mockMsgDispatch.SubmitMessageToDeliver,
		utCtxt,
	)
	assert.Nil(err)

	// Case 0: Prepare some messages before starting
	msgs0 := []common.Message{}
	for itr := 0; itr < 4; itr++ {
		msgs0 = append(
			msgs0, common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{TargetQueue: testTopic},
				ReceivedAt:  time.Now().UTC(),
				Tags: map[string]string{
					"flag 0": fmt.Sprintf("UT-Case 0 Msg %d", itr),
				},
				Body: []byte(fmt.Sprintf("UT-Case 0 Msg %d", itr)),
			},
		)
	}
	ctxt, cancel := context.WithTimeout(utCtxt, time.Second)
	assert.Nil(msgQueue.Write(msgs0[0], ctxt))
	minIdx0, _, err := msgQueue.IndexRange(testTopic, ctxt)
	assert.Nil(err)
	assert.Nil(msgQueue.Write(msgs0[1], ctxt))
	_, maxIdx01, err := msgQueue.IndexRange(testTopic, ctxt)
	assert.Nil(err)
	assert.Nil(msgQueue.Write(msgs0[2], ctxt))
	_, maxIdx02, err := msgQueue.IndexRange(testTopic, ctxt)
	assert.Nil(err)
	assert.Nil(msgQueue.Write(msgs0[3], ctxt))
	_, maxIdx03, err := msgQueue.IndexRange(testTopic, ctxt)
	cancel()
	assert.Nil(err)
	assert.Greater(minIdx0, int64(0))
	assert.LessOrEqual(maxIdx02, maxIdx03)

	// Case 1: Read from the middle of existing
	{
		rxACK := make(chan int)
		mockMsgDispatch.On(
			"SubmitMessageToDeliver",
			mock.AnythingOfType("dispatch.MessageInFlight"),
			mock.AnythingOfType("*context.cancelCtx"),
		).Run(func(args mock.Arguments) {
			msg := args.Get(0).(dispatch.MessageInFlight)
			assert.Equal(msgs0[2], msg.Message)
			assert.False(msg.Redelivery)
			assert.Equal(maxIdx02, msg.Index)
			rxACK <- 1
		}).Return(nil).Once()
		mockMsgDispatch.On(
			"SubmitMessageToDeliver",
			mock.AnythingOfType("dispatch.MessageInFlight"),
			mock.AnythingOfType("*context.cancelCtx"),
		).Run(func(args mock.Arguments) {
			msg := args.Get(0).(dispatch.MessageInFlight)
			assert.Equal(msgs0[3], msg.Message)
			assert.False(msg.Redelivery)
			assert.Equal(maxIdx03, msg.Index)
			rxACK <- 1
		}).Return(nil).Once()
		log.Debug("----------------------------------------------------------")
		assert.Nil(uut.StartReading(maxIdx01 + 1))
		readCtxt, readCancel := context.WithTimeout(utCtxt, time.Second)
		defer readCancel()
		for i := 0; i < 2; i++ {
			select {
			case val := <-rxACK:
				assert.Equal(1, val)
			case <-readCtxt.Done():
				assert.Nil(readCtxt.Err())
			}
		}
		log.Debug("==========================================================")
	}
	assert.Nil(uut.StopOperation())
	wg.Wait()

	// Case 2: send more mesages
	msgs2 := []common.Message{}
	for itr := 0; itr < 2; itr++ {
		msgs2 = append(
			msgs2, common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{TargetQueue: testTopic},
				ReceivedAt:  time.Now().UTC(),
				Tags: map[string]string{
					"flag 0": fmt.Sprintf("UT-Case 2 Msg %d", itr),
				},
				Body: []byte(fmt.Sprintf("UT-Case 2 Msg %d", itr)),
			},
		)
	}
	{
		rxACK := make(chan int)
		var maxIdx20, maxIdx21 int64
		{
			assert.Nil(msgQueue.Write(msgs2[0], utCtxt))
			minIdx2, t, err := msgQueue.IndexRange(testTopic, utCtxt)
			maxIdx20 = t
			assert.Nil(err)
			assert.Equal(minIdx0, minIdx2)
			assert.Nil(msgQueue.Write(msgs2[1], utCtxt))
			_, t, err = msgQueue.IndexRange(testTopic, utCtxt)
			maxIdx21 = t
			assert.Nil(err)
		}
		mockMsgDispatch.On(
			"SubmitMessageToDeliver",
			mock.AnythingOfType("dispatch.MessageInFlight"),
			mock.AnythingOfType("*context.cancelCtx"),
		).Run(func(args mock.Arguments) {
			msg := args.Get(0).(dispatch.MessageInFlight)
			assert.Equal(msgs2[0], msg.Message)
			assert.False(msg.Redelivery)
			assert.Equal(maxIdx20, msg.Index)
			rxACK <- 1
		}).Return(nil).Once()
		mockMsgDispatch.On(
			"SubmitMessageToDeliver",
			mock.AnythingOfType("dispatch.MessageInFlight"),
			mock.AnythingOfType("*context.cancelCtx"),
		).Run(func(args mock.Arguments) {
			msg := args.Get(0).(dispatch.MessageInFlight)
			assert.Equal(msgs2[1], msg.Message)
			assert.False(msg.Redelivery)
			assert.Equal(maxIdx21, msg.Index)
			rxACK <- 1
		}).Return(nil).Once()
		log.Debug("----------------------------------------------------------")
		assert.Nil(uut.StartReading(maxIdx03 + 1))
		readCtxt, readCancel := context.WithTimeout(utCtxt, time.Second*2)
		defer readCancel()
		for i := 0; i < 2; i++ {
			select {
			case val := <-rxACK:
				assert.Equal(1, val)
			case <-readCtxt.Done():
				assert.Nil(readCtxt.Err())
			}
		}
		log.Debug("==========================================================")
	}
	assert.Nil(uut.StopOperation())
}
