package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.com/project-nan/httpmq/common"
)

func TestEtcdDriverBasic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut, err := CreateEtcdDriver([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)
	uutCase, ok := uut.(*etcdDriver)
	assert.True(ok)

	// Case 0: fetch on unknown topic
	testTopic0 := uuid.New().String()
	log.Debugf("Test Topic 0 %s", testTopic0)
	minIdx, maxIdx, err := uutCase.IndexRange(testTopic0, time.Second)
	assert.Nil(err)
	assert.Equal(int64(-1), minIdx)
	assert.Equal(int64(0), maxIdx)

	// Case 1: create a new topic
	msg1 := common.Message{
		Source: common.MessageSource{
			Sender: uuid.New().String(),
			SentAt: time.Now().UTC(),
		},
		Destination: common.MessageDestination{TargetQueue: testTopic0},
		ReceivedAt:  time.Now().UTC(),
		Tags:        map[string]string{"flag 0": uuid.New().String()},
		Body:        []byte(uuid.New().String()),
	}
	assert.Nil(uut.Put(msg1, time.Second))
	// Reference index
	minIdx1, maxIdx1, err := uutCase.IndexRange(testTopic0, time.Second)
	assert.Nil(err)
	assert.Greater(minIdx1, int64(0))

	// Case 2: update topic
	msg2 := common.Message{
		Source: common.MessageSource{
			Sender: uuid.New().String(),
			SentAt: time.Now().UTC(),
		},
		Destination: common.MessageDestination{TargetQueue: testTopic0},
		ReceivedAt:  time.Now().UTC(),
		Tags:        map[string]string{"flag 0": uuid.New().String()},
		Body:        []byte(uuid.New().String()),
	}
	assert.Nil(uut.Put(msg2, time.Second))
	// Reference index
	minIdx2, maxIdx2, err := uutCase.IndexRange(testTopic0, time.Second)
	assert.Nil(err)
	assert.Equal(minIdx1, minIdx2)
	assert.Greater(maxIdx2, maxIdx1)

	// Case 3: update topic
	msg3 := common.Message{
		Source: common.MessageSource{
			Sender: uuid.New().String(),
			SentAt: time.Now().UTC(),
		},
		Destination: common.MessageDestination{TargetQueue: testTopic0},
		ReceivedAt:  time.Now().UTC(),
		Tags:        map[string]string{"flag 0": uuid.New().String()},
		Body:        []byte(uuid.New().String()),
	}
	assert.Nil(uut.Put(msg3, time.Second))
	// Reference index
	minIdx3, maxIdx3, err := uutCase.IndexRange(testTopic0, time.Second)
	assert.Nil(err)
	assert.Equal(minIdx1, minIdx3)
	assert.Greater(maxIdx3, maxIdx2)

	// Case 4: reference old version
	readMsg1, err := uut.Get(testTopic0, maxIdx1, time.Second)
	assert.Nil(err)
	assert.EqualValues(msg1, readMsg1)
	readMsg2, err := uut.Get(testTopic0, maxIdx2, time.Second)
	assert.Nil(err)
	assert.EqualValues(msg2, readMsg2)
	readMsg3, err := uut.Get(testTopic0, maxIdx3, time.Second)
	assert.Nil(err)
	assert.EqualValues(msg3, readMsg3)

	// Case 5: check newest version
	readMsg, err := uut.GetNewest(testTopic0, time.Second)
	assert.Nil(err)
	assert.EqualValues(msg3, readMsg)

	// Case 6: check oldest version
	readMsg, err = uut.GetOldest(testTopic0, time.Second)
	assert.Nil(err)
	assert.EqualValues(msg1, readMsg)

	// Case 7: start new topic
	testTopic7 := uuid.New().String()
	msg7B := common.Message{
		Source: common.MessageSource{
			Sender: uuid.New().String(),
			SentAt: time.Now().UTC(),
		},
		Destination: common.MessageDestination{TargetQueue: testTopic7},
		ReceivedAt:  time.Now().UTC(),
		Tags:        map[string]string{"flag 0": uuid.New().String()},
		Body:        []byte(uuid.New().String()),
	}
	assert.Nil(uut.Put(msg7B, time.Second))
	// Reference index
	minIdx7A, maxIdx7A, err := uutCase.IndexRange(testTopic0, time.Second)
	assert.Nil(err)
	assert.Equal(minIdx1, minIdx7A)
	assert.Equal(maxIdx3, maxIdx7A)
	minIdx7B, maxIdx7B, err := uutCase.IndexRange(testTopic7, time.Second)
	assert.Nil(err)

	// Case 8: update topic
	msg8B := common.Message{
		Source: common.MessageSource{
			Sender: uuid.New().String(),
			SentAt: time.Now().UTC(),
		},
		Destination: common.MessageDestination{TargetQueue: testTopic7},
		ReceivedAt:  time.Now().UTC(),
		Tags:        map[string]string{"flag 0": uuid.New().String()},
		Body:        []byte(uuid.New().String()),
	}
	assert.Nil(uut.Put(msg8B, time.Second))
	// Reference index
	minIdx8A, maxIdx8A, err := uutCase.IndexRange(testTopic0, time.Second)
	assert.Nil(err)
	assert.Equal(minIdx1, minIdx8A)
	assert.Equal(maxIdx3, maxIdx8A)
	minIdx8B, maxIdx8B, err := uutCase.IndexRange(testTopic7, time.Second)
	assert.Nil(err)
	assert.Equal(minIdx7B, minIdx8B)
	assert.Greater(maxIdx8B, maxIdx7B)

	assert.Nil(uut.Close())
}

func TestEtcdDriverStreaming(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut, err := CreateEtcdDriver([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	stopSignal1 := make(chan bool, 1)

	// Case 0: watch for data on non-existing topic
	{
		topic := uuid.New().String()
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic,
				StartIndex:  1,
				Handler: func(msg common.Message, index int64) (bool, error) {
					assert.Falsef(true, "This should not be occuring")
					return false, nil
				},
			},
		}
		// Fire the stop signal after 100 ms
		go func() {
			time.Sleep(time.Millisecond * 100)
			stopSignal1 <- true
		}()
		// Start the watching
		assert.Nil(uut.ReadStream(watchTargets, stopSignal1))
	}

	// Case 1: watch for three messages on topic
	{
		topic := uuid.New().String()
		msgItr := 0
		testMsgs := make([]common.Message, 3)
		for itr := 0; itr < 3; itr++ {
			testMsgs[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		// Watch handler
		handler := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs[msgItr], msg)
			msgItr += 1
			return msgItr < 3, nil
		}
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic,
				StartIndex:  1,
				Handler:     handler,
			},
		}
		// Transmit the three messages
		go func() {
			for idx, msg := range testMsgs {
				log.Debugf("Sending test message %d", idx)
				assert.Nil(uut.Put(msg, time.Second))
			}
		}()
		// Start the watching
		assert.Nil(uut.ReadStream(watchTargets, stopSignal1))
		assert.Equal(3, msgItr)
	}

	// Case 2: send three messages but fails after two messages
	{
		topic := uuid.New().String()
		msgItr := 0
		testMsgs := make([]common.Message, 3)
		for itr := 0; itr < 3; itr++ {
			testMsgs[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		// Watch handler
		handler := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs[msgItr], msg)
			msgItr += 1
			if msgItr == 3 {
				return false, fmt.Errorf("dummy error")
			}
			return true, nil
		}
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic,
				StartIndex:  1,
				Handler:     handler,
			},
		}
		// Transmit the three messages
		go func() {
			for idx, msg := range testMsgs {
				log.Debugf("Sending test message %d", idx)
				assert.Nil(uut.Put(msg, time.Second))
			}
		}()
		// Start the watching
		assert.NotNil(uut.ReadStream(watchTargets, stopSignal1))
		assert.Equal(3, msgItr)
	}

	// Case 3: send three messages but handler become inactive on second message
	{
		topic := uuid.New().String()
		msgItr := 0
		testMsgs := make([]common.Message, 3)
		for itr := 0; itr < 3; itr++ {
			testMsgs[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		// Watch handler
		handler := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs[msgItr], msg)
			msgItr += 1
			if msgItr == 2 {
				return false, nil
			}
			return true, nil
		}
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic,
				StartIndex:  1,
				Handler:     handler,
			},
		}
		// Transmit the three messages
		go func() {
			for idx, msg := range testMsgs {
				log.Debugf("Sending test message %d", idx)
				assert.Nil(uut.Put(msg, time.Second))
			}
		}()
		// Start the watching
		assert.Nil(uut.ReadStream(watchTargets, stopSignal1))
		assert.Equal(2, msgItr)
	}

	// Case 4: watch two different topics, and send three on one topic, and two on another
	{
		topic1 := uuid.New().String()
		topic2 := uuid.New().String()
		testMsgs1 := make([]common.Message, 3)
		for itr := 0; itr < 3; itr++ {
			testMsgs1[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic1,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		testMsgs2 := make([]common.Message, 2)
		for itr := 0; itr < 2; itr++ {
			testMsgs2[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic2,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		// Watch handler
		msgItr1 := 0
		handler1 := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs1[msgItr1], msg)
			msgItr1 += 1
			return msgItr1 < 3, nil
		}
		msgItr2 := 0
		handler2 := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs2[msgItr2], msg)
			msgItr2 += 1
			return msgItr2 < 2, nil
		}
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic1,
				StartIndex:  1,
				Handler:     handler1,
			},
			{
				TargetQueue: topic2,
				StartIndex:  1,
				Handler:     handler2,
			},
		}
		// Transmit the three messages
		go func() {
			for idx, msg := range testMsgs1 {
				log.Debugf("Sending test message %d", idx)
				assert.Nil(uut.Put(msg, time.Second))
			}
		}()
		go func() {
			for idx, msg := range testMsgs2 {
				log.Debugf("Sending test message %d", idx)
				assert.Nil(uut.Put(msg, time.Second))
			}
		}()
		// Start the watching
		assert.Nil(uut.ReadStream(watchTargets, stopSignal1))
		assert.Equal(3, msgItr1)
		assert.Equal(2, msgItr2)
	}

	// Case 5: watch two different topics, and crash first handler immediately
	{
		topic1 := uuid.New().String()
		topic2 := uuid.New().String()
		testMsgs1 := make([]common.Message, 3)
		for itr := 0; itr < 3; itr++ {
			testMsgs1[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic1,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		testMsgs2 := make([]common.Message, 2)
		for itr := 0; itr < 2; itr++ {
			testMsgs2[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic2,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		// Watch handler
		msgItr1 := 0
		handler1 := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs1[msgItr1], msg)
			msgItr1 += 1
			return false, fmt.Errorf("dummy error")
		}
		msgItr2 := 0
		handler2 := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs2[msgItr2], msg)
			msgItr2 += 1
			return msgItr2 < 2, nil
		}
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic1,
				StartIndex:  1,
				Handler:     handler1,
			},
			{
				TargetQueue: topic2,
				StartIndex:  1,
				Handler:     handler2,
			},
		}
		// Transmit the three messages
		go func() {
			for idx, msg := range testMsgs1 {
				log.Debugf("Sending test message %d", idx)
				assert.Nil(uut.Put(msg, time.Second))
			}
		}()
		go func() {
			for idx, msg := range testMsgs2 {
				log.Debugf("Sending test message %d", idx)
				assert.Nil(uut.Put(msg, time.Second))
			}
		}()
		// Start the watching
		assert.NotNil(uut.ReadStream(watchTargets, stopSignal1))
		assert.Equal(1, msgItr1)
		assert.LessOrEqual(1, msgItr2)
	}

	// Case 6: watch from the middle of the stream of data
	{
		topic := uuid.New().String()
		testMsgs := make([]common.Message, 5)
		for itr := 0; itr < 5; itr++ {
			testMsgs[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		// Watch handler
		msgItr := 2
		handler := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs[msgItr], msg)
			msgItr += 1
			return msgItr < 5, nil
		}
		for itr := 0; itr < 2; itr++ {
			log.Debugf("Sending test message %d", itr)
			assert.Nil(uut.Put(testMsgs[itr], time.Second))
		}
		_, maxIdx, err := uut.IndexRange(topic, time.Second)
		assert.Nil(err)
		for itr := 2; itr < 5; itr++ {
			log.Debugf("Sending test message %d", itr)
			assert.Nil(uut.Put(testMsgs[itr], time.Second))
		}
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic,
				StartIndex:  maxIdx + 1,
				Handler:     handler,
			},
		}
		// Start the watching
		assert.Nil(uut.ReadStream(watchTargets, stopSignal1))
		assert.Equal(5, msgItr)
	}

	// Case 7: watch from REV 0 in middle of seq of messages
	{
		topic := uuid.New().String()
		testMsgs := make([]common.Message, 5)
		for itr := 0; itr < 5; itr++ {
			testMsgs[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		// Watch handler
		msgItr := 2
		handler := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs[msgItr], msg)
			msgItr += 1
			return msgItr < 5, nil
		}
		for itr := 0; itr < 2; itr++ {
			log.Debugf("Sending test message %d", itr)
			assert.Nil(uut.Put(testMsgs[itr], time.Second))
		}
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic,
				StartIndex:  0,
				Handler:     handler,
			},
		}
		go func() {
			for itr := 2; itr < 5; itr++ {
				log.Debugf("Sending test message %d", itr)
				assert.Nil(uut.Put(testMsgs[itr], time.Second))
			}
		}()
		// Start the watching
		assert.Nil(uut.ReadStream(watchTargets, stopSignal1))
		assert.Equal(5, msgItr)
	}
}

func TestEtcdDriverBasicAfterCompaction(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut, err := CreateEtcdDriver([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)
	uutCase, ok := uut.(*etcdDriver)
	assert.True(ok)

	// Case 1: create messages
	topic1 := uuid.New().String()
	testMsgs1 := make([]common.Message, 5)
	{
		for itr := 0; itr < 5; itr++ {
			testMsgs1[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic1,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		for idx, msg := range testMsgs1 {
			log.Debugf("Sending test message %d", idx)
			assert.Nil(uut.Put(msg, time.Second))
		}
	}

	// Case 2: Trigger compaction
	minIdx2, maxIdx2, err := uut.IndexRange(topic1, time.Second)
	log.Debugf("MIN2 %d, MAX2 %d", minIdx2, maxIdx2)
	assert.Nil(err)
	_, err = uutCase.client.Compact(context.Background(), maxIdx2)
	assert.Nil(err)

	// Case 3: verify compaction occurred
	{
		_, err := uut.Get(topic1, minIdx2, time.Second)
		assert.NotNil(err)
	}
	{
		_, err := uut.Get(topic1, maxIdx2-1, time.Second)
		assert.NotNil(err)
	}
	{
		val, err := uut.Get(topic1, maxIdx2, time.Second)
		assert.Nil(err)
		assert.EqualValues(testMsgs1[4], val)
	}

	// Case 4: create more messages
	testMsgs4 := make([]common.Message, 3)
	{
		for itr := 0; itr < 3; itr++ {
			testMsgs4[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic1,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		for idx, msg := range testMsgs4 {
			log.Debugf("Sending test message %d", idx)
			assert.Nil(uut.Put(msg, time.Second))
		}
	}

	// Case 5: Trigger compaction
	minIdx5, maxIdx5, err := uut.IndexRange(topic1, time.Second)
	log.Debugf("MIN5 %d, MAX5 %d", minIdx5, maxIdx5)
	assert.Nil(err)
	assert.Equal(minIdx2, minIdx5)
	_, err = uutCase.client.Compact(context.Background(), maxIdx5)
	assert.Nil(err)

	// Case 6: verify compaction occurred
	{
		_, err := uut.Get(topic1, maxIdx2, time.Second)
		assert.NotNil(err)
	}
	{
		val, err := uut.Get(topic1, maxIdx5, time.Second)
		assert.Nil(err)
		assert.EqualValues(testMsgs4[2], val)
	}
}

func TestEtcdDriverStreamingAfterCompaction(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut, err := CreateEtcdDriver([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)
	uutCase, ok := uut.(*etcdDriver)
	assert.True(ok)

	stopSignal := make(chan bool, 1)

	// Case 1: create messages
	topic := uuid.New().String()
	testMsgs1 := make([]common.Message, 5)
	{
		for itr := 0; itr < 5; itr++ {
			testMsgs1[itr] = common.Message{
				Source: common.MessageSource{
					Sender: uuid.New().String(),
					SentAt: time.Now().UTC(),
				},
				Destination: common.MessageDestination{
					TargetQueue: topic,
				},
				Tags:       map[string]string{uuid.New().String(): uuid.New().String()},
				ReceivedAt: time.Now().UTC(),
				Body:       []byte(uuid.New().String()),
			}
		}
		for idx, msg := range testMsgs1 {
			log.Debugf("Sending test message %d", idx)
			assert.Nil(uut.Put(msg, time.Second))
		}
	}
	minIdx1, maxIdx1, err := uut.IndexRange(topic, time.Second)
	log.Debugf("MIN1 %d, MAX1 %d", minIdx1, maxIdx1)
	assert.Nil(err)

	// Case 2: compaction
	_, err = uutCase.client.Compact(context.Background(), maxIdx1-2)
	assert.Nil(err)

	// Case 3: watch from beginning of topic
	{
		msgItr := 0
		handler := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs1[msgItr], msg)
			msgItr += 1
			return msgItr < 5, nil
		}
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic,
				StartIndex:  minIdx1,
				Handler:     handler,
			},
		}
		// Start the watching
		assert.Nil(uut.ReadStream(watchTargets, stopSignal))
		assert.Equal(0, msgItr)
	}

	// Case 4: watch from compaction point
	{
		msgItr := 2
		handler := func(msg common.Message, index int64) (bool, error) {
			assert.EqualValues(testMsgs1[msgItr], msg)
			msgItr += 1
			return msgItr < 5, nil
		}
		watchTargets := []ReadStreamParam{
			{
				TargetQueue: topic,
				StartIndex:  maxIdx1 - 2,
				Handler:     handler,
			},
		}
		// Start the watching
		assert.Nil(uut.ReadStream(watchTargets, stopSignal))
		assert.Equal(5, msgItr)
	}
}

func TestEtcdDriverMutex(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut, err := CreateEtcdDriver([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	uut2, err := CreateEtcdDriver([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	// Case 0: Unlock an unknown mutex
	mutex0 := fmt.Sprintf("mtx-%s", uuid.New().String())
	assert.NotNil(uut.Unlock(mutex0, time.Second))

	// Case 1: Lock a mutex
	mutex1 := fmt.Sprintf("mtx-%s", uuid.New().String())
	assert.Nil(uut.Lock(mutex1, time.Second))

	// Case 2: Lock again and fail
	assert.NotNil(uut2.Lock(mutex1, time.Millisecond*10))

	// Case 3: Unlock and try again
	assert.Nil(uut.Unlock(mutex1, time.Second))
	assert.Nil(uut2.Lock(mutex1, time.Millisecond*10))
}
