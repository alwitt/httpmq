package dispatch

import (
	"context"
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

	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp, err := common.GetNewTaskProcessorInstance("unit-test", maxInflight*2, ctxt)
	assert.Nil(err)

	testMsgs := make(chan MessageInFlight, maxInflight*2)
	testMsgRecv := func(msg MessageInFlight, ctxt context.Context) error {
		testMsgs <- msg
		return nil
	}

	testACKs := make(chan []int64, maxInflight*2)
	testRetries := make(chan MessageInFlight, maxInflight*2)
	testTransmits := make(chan MessageInFlight, maxInflight*2)

	msgTxRegister := make(chan int64, maxInflight)
	msgTxRegisterRecv := func(msgIdx int64, _ time.Time, ctxt context.Context) error {
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
	assert.Nil(tp.StartEventLoop(&wg))

	// Case 0: nothing happening
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.ProcessMessages(ctxt))
		cancel()
		assert.Empty(testMsgs)
		assert.Empty(msgTxRegister)
	}

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
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.SubmitMessageToDeliver(testMsgs1[itr], ctxt))
		cancel()
	}
	// Verify transmission
	for itr := 0; itr < 2; itr++ {
		select {
		case msg := <-testMsgs:
			assert.EqualValues(testMsgs1[itr], msg)
		case <-time.After(time.Second * 2):
			assert.True(false)
		}
		select {
		case sent := <-msgTxRegister:
			assert.EqualValues(testMsgs1[itr].Index, sent)
		case <-time.After(time.Second * 2):
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
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.SubmitMessageToDeliver(testMsgs2[itr], ctxt))
		cancel()
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
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.SubmitMessageACK([]int64{int64(msgIndexStart) + 1}, ctxt))
		cancel()
		// Verify transmission
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
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.SubmitMessageACK([]int64{int64(msgIndexStart) + 0}, ctxt))
		cancel()
		assert.Empty(testMsgs)
		assert.Empty(msgTxRegister)
	}

	// Case 5: Re-transmit first batch of messages
	for itr := 0; itr < 2; itr++ {
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.SubmitRetransmit(testMsgs1[itr], ctxt))
		cancel()
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
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.SubmitRetransmit(testMsgs2[itr], ctxt))
		cancel()
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

	assert.Nil(uut.StopOperation())
}

func TestMessageDispatchStartWithInflight(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	maxInflight := 3

	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp, err := common.GetNewTaskProcessorInstance("unit-test", maxInflight*2, ctxt)
	assert.Nil(err)

	testMsgs := make(chan MessageInFlight, maxInflight*2)
	testMsgRecv := func(msg MessageInFlight, ctxt context.Context) error {
		testMsgs <- msg
		return nil
	}

	testACKs := make(chan []int64, maxInflight*2)
	testRetries := make(chan MessageInFlight, maxInflight*2)
	testTransmits := make(chan MessageInFlight, maxInflight*2)

	msgTxRegister := make(chan int64, maxInflight)
	msgTxRegisterRecv := func(msgIdx int64, _ time.Time, ctxt context.Context) error {
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
	assert.Nil(tp.StartEventLoop(&wg))

	// Case 0: nothing happening
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.ProcessMessages(ctxt))
		cancel()
		assert.Empty(testMsgs)
		assert.Empty(msgTxRegister)
	}

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
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.SubmitMessageToDeliver(testMsgs1[itr], ctxt))
		cancel()
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
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.SubmitRetransmit(testMsgs2[itr], ctxt))
		cancel()
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
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.SubmitMessageACK([]int64{int64(rand.Intn(100000))}, ctxt))
		cancel()
		// Verify transmission
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

	assert.Nil(uut.StopOperation())
}
