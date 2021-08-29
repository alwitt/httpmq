package dispatch

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.com/project-nan/httpmq/common"
)

func TestMessageDispatch(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	maxInflight := 3

	tp, err := common.GetNewTaskProcessorInstance("unit-test", maxInflight*2)
	assert.Nil(err)

	testMsgs := make(chan MessageInFlight, maxInflight*2)
	testMsgRecv := func(msg MessageInFlight) error {
		testMsgs <- msg
		return nil
	}

	testACKs := make(chan int64, maxInflight*2)
	testRetries := make(chan MessageInFlight, maxInflight*2)
	testTransmits := make(chan MessageInFlight, maxInflight*2)

	msgTxRegister := make(chan int64, maxInflight)
	msgTxRegisterRecv := func(msgIdx int64) error {
		msgTxRegister <- msgIdx
		return nil
	}

	testQueue := "unit-test"
	uut, err := DefineMessageDispatch(
		testQueue,
		tp,
		testACKs,
		testRetries,
		testTransmits,
		0,
		maxInflight,
		testMsgRecv,
		msgTxRegisterRecv,
	)
	assert.Nil(err)

	// Start the task processor
	wg := sync.WaitGroup{}
	assert.Nil(tp.StartEventLoop(&wg))

	// Case 0: nothing happening
	assert.Nil(uut.ProcessMessages())
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	rand.Seed(time.Now().UnixNano())
	msgIndexStart := rand.Intn(100000)
	msgIndex := msgIndexStart

	// Case 1: generate some messages
	testMsgs1 := make([]MessageInFlight, 2)
	for itr := 0; itr < 2; itr++ {
		testMsgs1[itr] = MessageInFlight{
			Message: common.Message{
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
			},
			Index:      int64(msgIndex),
			Redelivery: false,
		}
		msgIndex += 1
	}
	for itr := 0; itr < 2; itr++ {
		assert.Nil(uut.SubmitMessageToDeliver(testMsgs1[itr]))
	}
	// Verify transmission
	for itr := 0; itr < 2; itr++ {
		select {
		case msg := <-testMsgs:
			assert.EqualValues(testMsgs1[itr], msg)
		case <-time.After(time.Second):
			assert.True(false)
		}
		select {
		case sent := <-msgTxRegister:
			assert.EqualValues(testMsgs1[itr].Index, sent)
		case <-time.After(time.Second):
			assert.True(false)
		}
	}
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	// Case 2: generate more messages
	testMsgs2 := make([]MessageInFlight, 2)
	for itr := 0; itr < 2; itr++ {
		testMsgs2[itr] = MessageInFlight{
			Message: common.Message{
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
			},
			Index:      int64(msgIndex),
			Redelivery: false,
		}
		msgIndex += 1
	}
	for itr := 0; itr < 2; itr++ {
		assert.Nil(uut.SubmitMessageToDeliver(testMsgs2[itr]))
	}
	// Verify transmission
	{
		select {
		case msg := <-testMsgs:
			assert.EqualValues(testMsgs2[0], msg)
		case <-time.After(time.Second):
			assert.True(false)
		}
		select {
		case sent := <-msgTxRegister:
			assert.EqualValues(testMsgs2[0].Index, sent)
		case <-time.After(time.Second):
			assert.True(false)
		}
	}
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	// Case 3: ACK a message
	assert.Nil(uut.SubmitMessageACK(int64(msgIndexStart) + 1))
	// Verify transmission
	{
		select {
		case msg := <-testMsgs:
			assert.EqualValues(testMsgs2[1], msg)
		case <-time.After(time.Second):
			assert.True(false)
		}
		select {
		case sent := <-msgTxRegister:
			assert.EqualValues(testMsgs2[1].Index, sent)
		case <-time.After(time.Second):
			assert.True(false)
		}
	}
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	// Case 4: ACK more messages
	assert.Nil(uut.SubmitMessageACK(int64(msgIndexStart) + 0))
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	// Case 5: Re-transmit first batch of messages
	for itr := 0; itr < 2; itr++ {
		assert.Nil(uut.SubmitRetransmit(testMsgs1[itr]))
	}
	// Verify transmission
	for itr := 0; itr < 2; itr++ {
		tmp := testMsgs1[itr]
		tmp.Redelivery = true
		select {
		case msg := <-testMsgs:
			assert.EqualValues(tmp, msg)
		case <-time.After(time.Second):
			assert.True(false)
		}
	}
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	// Case 6: Re-transmit second batch of messages
	for itr := 0; itr < 2; itr++ {
		assert.Nil(uut.SubmitRetransmit(testMsgs2[itr]))
	}
	// Verify transmission
	for itr := 0; itr < 2; itr++ {
		tmp := testMsgs2[itr]
		tmp.Redelivery = true
		select {
		case msg := <-testMsgs:
			assert.EqualValues(tmp, msg)
		case <-time.After(time.Second):
			assert.True(false)
		}
	}
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	assert.Nil(tp.StopEventLoop())
	wg.Wait()
}

func TestMessageDispatchStartWithInflight(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	maxInflight := 3

	tp, err := common.GetNewTaskProcessorInstance("unit-test", maxInflight*2)
	assert.Nil(err)

	testMsgs := make(chan MessageInFlight, maxInflight*2)
	testMsgRecv := func(msg MessageInFlight) error {
		testMsgs <- msg
		return nil
	}

	testACKs := make(chan int64, maxInflight*2)
	testRetries := make(chan MessageInFlight, maxInflight*2)
	testTransmits := make(chan MessageInFlight, maxInflight*2)

	msgTxRegister := make(chan int64, maxInflight)
	msgTxRegisterRecv := func(msgIdx int64) error {
		msgTxRegister <- msgIdx
		return nil
	}

	testQueue := "unit-test"
	startingInflight := 2
	uut, err := DefineMessageDispatch(
		testQueue,
		tp,
		testACKs,
		testRetries,
		testTransmits,
		startingInflight,
		maxInflight,
		testMsgRecv,
		msgTxRegisterRecv,
	)
	assert.Nil(err)

	// Start the task processor
	wg := sync.WaitGroup{}
	assert.Nil(tp.StartEventLoop(&wg))

	// Case 0: nothing happening
	assert.Nil(uut.ProcessMessages())
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	rand.Seed(time.Now().UnixNano())
	msgIndexStart := rand.Intn(100000)
	msgIndex := msgIndexStart

	// Case 1: generate some messages
	testMsgs1 := make([]MessageInFlight, 2)
	for itr := 0; itr < 2; itr++ {
		testMsgs1[itr] = MessageInFlight{
			Message: common.Message{
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
			},
			Index:      int64(msgIndex),
			Redelivery: false,
		}
		msgIndex += 1
	}
	for itr := 0; itr < 2; itr++ {
		assert.Nil(uut.SubmitMessageToDeliver(testMsgs1[itr]))
	}
	// Verify transmission
	{
		select {
		case msg := <-testMsgs:
			assert.EqualValues(testMsgs1[0], msg)
		case <-time.After(time.Second):
			assert.True(false)
		}
		select {
		case sent := <-msgTxRegister:
			assert.EqualValues(testMsgs1[0].Index, sent)
		case <-time.After(time.Second):
			assert.True(false)
		}
	}
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	// Case 2: run-retransmission
	testMsgs2 := make([]MessageInFlight, 2)
	for itr := 0; itr < 2; itr++ {
		testMsgs2[itr] = MessageInFlight{
			Message: common.Message{
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
			},
			Index:      int64(msgIndex),
			Redelivery: true,
		}
		msgIndex += 1
	}
	for itr := 0; itr < 2; itr++ {
		assert.Nil(uut.SubmitRetransmit(testMsgs2[itr]))
	}
	// Verify transmission
	for itr := 0; itr < 2; itr++ {
		select {
		case msg := <-testMsgs:
			assert.EqualValues(testMsgs2[itr], msg)
		case <-time.After(time.Second):
			assert.True(false)
		}
	}
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	// Case 3: ACK a message
	assert.Nil(uut.SubmitMessageACK(int64(rand.Intn(100000))))
	// Verify transmission
	{
		select {
		case msg := <-testMsgs:
			assert.EqualValues(testMsgs1[1], msg)
		case <-time.After(time.Second):
			assert.True(false)
		}
		select {
		case sent := <-msgTxRegister:
			assert.EqualValues(testMsgs1[1].Index, sent)
		case <-time.After(time.Second):
			assert.True(false)
		}
	}
	assert.Empty(testMsgs)
	assert.Empty(msgTxRegister)

	assert.Nil(tp.StopEventLoop())
	wg.Wait()
}
