package storage

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.com/project-nan/httpmq/common"
)

func skipCompactionTest(t *testing.T) {
	if os.Getenv("RUN_ETCD_COMPACT_TESTS") == "" {
		t.Skip("Skipping ETCD compaction related tests")
	}
}

func TestEtcdDriverBasic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut, _, err := CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)
	uutCase, ok := uut.(*etcdBackedStorage)
	assert.True(ok)

	// Case 0: fetch on unknown topic
	testTopic0 := uuid.New().String()
	log.Debugf("Test Topic 0 %s", testTopic0)
	ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
	minIdx, maxIdx, err := uutCase.IndexRange(testTopic0, ctxt)
	cancel()
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
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	assert.Nil(uut.Write(msg1, ctxt))
	cancel()
	// Reference index
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	minIdx1, maxIdx1, err := uutCase.IndexRange(testTopic0, ctxt)
	cancel()
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
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	assert.Nil(uut.Write(msg2, ctxt))
	// Reference index
	minIdx2, maxIdx2, err := uutCase.IndexRange(testTopic0, ctxt)
	cancel()
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
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	assert.Nil(uut.Write(msg3, ctxt))
	// Reference index
	minIdx3, maxIdx3, err := uutCase.IndexRange(testTopic0, ctxt)
	cancel()
	assert.Nil(err)
	assert.Equal(minIdx1, minIdx3)
	assert.Greater(maxIdx3, maxIdx2)

	// Case 4: reference old version
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	readMsg1, err := uut.Read(testTopic0, maxIdx1, ctxt)
	assert.Nil(err)
	assert.EqualValues(msg1, readMsg1)
	readMsg2, err := uut.Read(testTopic0, maxIdx2, ctxt)
	assert.Nil(err)
	assert.EqualValues(msg2, readMsg2)
	readMsg3, err := uut.Read(testTopic0, maxIdx3, ctxt)
	assert.Nil(err)
	assert.EqualValues(msg3, readMsg3)
	cancel()

	// Case 5: check newest version
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	readMsg, err := uut.ReadNewest(testTopic0, ctxt)
	cancel()
	assert.Nil(err)
	assert.EqualValues(msg3, readMsg)

	// Case 6: check oldest version
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	readMsg, err = uut.ReadOldest(testTopic0, ctxt)
	cancel()
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
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	assert.Nil(uut.Write(msg7B, ctxt))
	// Reference index
	minIdx7A, maxIdx7A, err := uutCase.IndexRange(testTopic0, ctxt)
	assert.Nil(err)
	assert.Equal(minIdx1, minIdx7A)
	assert.Equal(maxIdx3, maxIdx7A)
	minIdx7B, maxIdx7B, err := uutCase.IndexRange(testTopic7, ctxt)
	assert.Nil(err)
	cancel()

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
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	assert.Nil(uut.Write(msg8B, ctxt))
	// Reference index
	minIdx8A, maxIdx8A, err := uutCase.IndexRange(testTopic0, ctxt)
	assert.Nil(err)
	assert.Equal(minIdx1, minIdx8A)
	assert.Equal(maxIdx3, maxIdx8A)
	minIdx8B, maxIdx8B, err := uutCase.IndexRange(testTopic7, ctxt)
	assert.Nil(err)
	assert.Equal(minIdx7B, minIdx8B)
	assert.Greater(maxIdx8B, maxIdx7B)
	cancel()

	assert.Nil(uut.Close())
}

func TestEtcdDriverStreaming(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut, _, err := CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	// Case 0: watch for data on non-existing topic
	{
		topic := uuid.New().String()
		watchTarget := ReadStreamParam{
			TargetQueue: topic,
			StartIndex:  1,
			Handler: func(msg common.Message, index int64) (bool, error) {
				assert.Falsef(true, "This should not be occuring")
				return false, nil
			},
		}
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		// Fire the stop signal after 100 ms
		go func() {
			time.Sleep(time.Millisecond * 100)
			cancel()
		}()
		// Start the watching
		_, err := uut.ReadStream(watchTarget, ctxt)
		assert.Nil(err)
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
		// Transmit the three messages
		for idx, msg := range testMsgs {
			log.Debugf("Sending test message %d", idx)
			ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
			assert.Nil(uut.Write(msg, ctxt))
			cancel()
		}
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		minIdx, maxIdx, err := uut.IndexRange(topic, ctxt)
		assert.Nil(err)
		watchTarget := ReadStreamParam{
			TargetQueue: topic,
			StartIndex:  minIdx,
			Handler:     handler,
		}
		// Start the watching
		nextIdx, err := uut.ReadStream(watchTarget, ctxt)
		assert.Nil(err)
		assert.Greater(nextIdx, maxIdx)
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
		// Transmit the three messages
		for idx, msg := range testMsgs {
			log.Debugf("Sending test message %d", idx)
			ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
			assert.Nil(uut.Write(msg, ctxt))
			cancel()
		}
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		minIdx, maxIdx, err := uut.IndexRange(topic, ctxt)
		assert.Nil(err)
		watchTarget := ReadStreamParam{
			TargetQueue: topic,
			StartIndex:  minIdx,
			Handler:     handler,
		}
		// Start the watching
		nextIdx, err := uut.ReadStream(watchTarget, ctxt)
		assert.NotNil(err)
		assert.InDelta(maxIdx, nextIdx, 2)
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
		// Transmit the three messages
		for idx, msg := range testMsgs {
			log.Debugf("Sending test message %d", idx)
			ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
			assert.Nil(uut.Write(msg, ctxt))
			cancel()
		}
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		minIdx, maxIdx, err := uut.IndexRange(topic, ctxt)
		assert.Nil(err)
		watchTarget := ReadStreamParam{
			TargetQueue: topic,
			StartIndex:  minIdx,
			Handler:     handler,
		}
		// Start the watching
		nextIdx, err := uut.ReadStream(watchTarget, ctxt)
		assert.Nil(err)
		assert.InDelta(maxIdx, nextIdx, 2)
		assert.Equal(2, msgItr)
	}

	// Case 4: watch from the middle of the stream of data
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
			ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
			assert.Nil(uut.Write(testMsgs[itr], ctxt))
			cancel()
		}
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		_, maxIdx, err := uut.IndexRange(topic, ctxt)
		cancel()
		assert.Nil(err)
		for itr := 2; itr < 5; itr++ {
			log.Debugf("Sending test message %d", itr)
			ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
			assert.Nil(uut.Write(testMsgs[itr], ctxt))
			cancel()
		}
		watchTarget := ReadStreamParam{
			TargetQueue: topic,
			StartIndex:  maxIdx + 1,
			Handler:     handler,
		}
		ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// Start the watching
		_, err = uut.ReadStream(watchTarget, ctxt)
		assert.Nil(err)
		assert.Equal(5, msgItr)
	}

	// Case 5: watch from REV 0 in middle of seq of messages
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
			ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
			assert.Nil(uut.Write(testMsgs[itr], ctxt))
			cancel()
		}
		watchTarget := ReadStreamParam{
			TargetQueue: topic,
			StartIndex:  0,
			Handler:     handler,
		}
		go func() {
			for itr := 2; itr < 5; itr++ {
				log.Debugf("Sending test message %d", itr)
				ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
				assert.Nil(uut.Write(testMsgs[itr], ctxt))
				cancel()
			}
		}()
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// Start the watching
		_, err = uut.ReadStream(watchTarget, ctxt)
		assert.Nil(err)
		assert.Equal(5, msgItr)
	}
}

func TestEtcdDriverBasicAfterCompaction(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	skipCompactionTest(t)

	uut, _, err := CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)
	uutCase, ok := uut.(*etcdBackedStorage)
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
			ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
			assert.Nil(uut.Write(msg, ctxt))
			cancel()
		}
	}

	// Case 2: Trigger compaction
	ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
	minIdx2, maxIdx2, err := uut.IndexRange(topic1, ctxt)
	cancel()
	log.Debugf("MIN2 %d, MAX2 %d", minIdx2, maxIdx2)
	assert.Nil(err)
	_, err = uutCase.client.Compact(context.Background(), maxIdx2)
	assert.Nil(err)

	// Case 3: verify compaction occurred
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	{
		_, err := uut.Read(topic1, minIdx2, ctxt)
		assert.NotNil(err)
	}
	{
		_, err := uut.Read(topic1, maxIdx2-1, ctxt)
		assert.NotNil(err)
	}
	{
		val, err := uut.Read(topic1, maxIdx2, ctxt)
		assert.Nil(err)
		assert.EqualValues(testMsgs1[4], val)
	}
	cancel()

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
			ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
			assert.Nil(uut.Write(msg, ctxt))
			cancel()
		}
	}

	// Case 5: Trigger compaction
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	minIdx5, maxIdx5, err := uut.IndexRange(topic1, ctxt)
	log.Debugf("MIN5 %d, MAX5 %d", minIdx5, maxIdx5)
	assert.Nil(err)
	assert.Equal(minIdx2, minIdx5)
	_, err = uutCase.client.Compact(context.Background(), maxIdx5)
	assert.Nil(err)
	cancel()

	// Case 6: verify compaction occurred
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	{
		_, err := uut.Read(topic1, maxIdx2, ctxt)
		assert.NotNil(err)
	}
	{
		val, err := uut.Read(topic1, maxIdx5, ctxt)
		assert.Nil(err)
		assert.EqualValues(testMsgs4[2], val)
	}
	cancel()

	time.Sleep(time.Second * 15)
}

func TestEtcdDriverMutex(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut, _, err := CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	uut2, _, err := CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	// Case 0: Unlock an unknown mutex
	ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
	mutex0 := fmt.Sprintf("mtx-%s", uuid.New().String())
	assert.NotNil(uut.Unlock(mutex0, ctxt))
	cancel()

	// Case 1: Lock a mutex
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	mutex1 := fmt.Sprintf("mtx-%s", uuid.New().String())
	assert.Nil(uut.Lock(mutex1, ctxt))
	cancel()

	// Case 2: Lock again and fail
	ctxt, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
	assert.NotNil(uut2.Lock(mutex1, ctxt))
	cancel()

	// Case 3: Unlock and try again
	ctxt, cancel = context.WithTimeout(context.Background(), time.Second)
	assert.Nil(uut.Unlock(mutex1, ctxt))
	cancel()
	ctxt, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
	assert.Nil(uut2.Lock(mutex1, ctxt))
	cancel()
}

type utDummyVal1 struct {
	Val1 string
}

func (v *utDummyVal1) Scan(src interface{}) error {
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("src is not []byte")
	}
	return json.Unmarshal(bytes, v)
}

func (v *utDummyVal1) Value() (driver.Value, error) {
	return json.Marshal(v)
}

type utDummyVal2 struct {
	Val1 string
	Val2 int
}

func (v *utDummyVal2) Scan(src interface{}) error {
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("src is not []byte")
	}
	return json.Unmarshal(bytes, v)
}

func (v *utDummyVal2) Value() (driver.Value, error) {
	return json.Marshal(v)
}

func TestEtcdDriverKeyValue(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	_, uut, err := CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	// Case 0: Get key which does not exist
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		var result utDummyVal1
		err := uut.Get(uuid.New().String(), &result, ctxt)
		assert.NotNil(err)
		cancel()
	}

	// Case 1: set a new key
	key1 := uuid.New().String()
	val1 := utDummyVal1{Val1: uuid.New().String()}
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Set(key1, &val1, ctxt))
		var result utDummyVal1
		err := uut.Get(key1, &result, ctxt)
		assert.Nil(err)
		assert.EqualValues(val1, result)
		cancel()
	}

	// Case 2: set a new key
	key2 := uuid.New().String()
	val2 := utDummyVal2{Val1: uuid.New().String(), Val2: 5123}
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Set(key2, &val2, ctxt))
		cancel()
	}
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		var result utDummyVal2
		err := uut.Get(key2, &result, ctxt)
		assert.Nil(err)
		assert.EqualValues(val2, result)
		cancel()
	}

	// Case 3: delete a key
	{
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Delete(key1, ctxt))
		var result utDummyVal1
		err := uut.Get(key1, &result, ctxt)
		assert.NotNil(err)
		cancel()
	}
}
