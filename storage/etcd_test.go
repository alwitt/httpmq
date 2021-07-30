package storage

import (
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
	assert.Equal(-1, minIdx)
	assert.Equal(0, maxIdx)

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
	assert.Greater(minIdx1, 0)

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
