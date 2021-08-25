package subscription

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/storage"
)

func TestSubscriptionRecorderBasic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	_, testKV, err := storage.CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	tp, err := common.GetNewTaskProcessorInstance("unit-test", 4)
	assert.Nil(err)

	testRecordPrefix := uuid.New().String()
	sessionRecords := fmt.Sprintf("%s/active-sessions", testRecordPrefix)
	uut, err := DefineSubscriptionRecorder(testKV, tp, testRecordPrefix, time.Second)
	assert.Nil(err)

	// Start the task processor
	wg := sync.WaitGroup{}
	assert.Nil(tp.StartEventLoop(&wg))

	// Case 0: initialize session record tracking
	{
		var r SubscriptionRecords
		err := testKV.Get(sessionRecords, &r, time.Second)
		assert.NotNil(err)
	}
	assert.Nil(uut.ReadySessionRecords())
	{
		var entry SubscriptionRecords
		err := testKV.Get(sessionRecords, &entry, time.Second)
		assert.Nil(err)
		assert.EqualValues(SubscriptionRecords{ActiveSessions: map[string]ClientSubscription{}}, entry)
	}

	// Case 1: refresh an unknown client
	assert.NotNil(uut.RefreshClientSession(uuid.New().String(), uuid.New().String(), time.Now()))

	// Case 2: clear an unknown client
	assert.NotNil(uut.ClearClientSession(uuid.New().String(), uuid.New().String(), time.Now()))

	// Case 3: create new client record
	client3 := uuid.New().String()
	node3 := uuid.New().String()
	{
		record, err := uut.LogClientSession(client3, node3, time.Now())
		assert.Nil(err)
		assert.Equal(client3, record.ClientName)
		assert.Equal(node3, record.ServingNode)
	}

	// Case 4: re-create the client record
	{
		record, err := uut.LogClientSession(client3, node3, time.Now())
		assert.NotNil(err)
		assert.Equal(client3, record.ClientName)
		assert.Equal(node3, record.ServingNode)
	}

	// Case 5: refresh the client record
	assert.Nil(uut.ReadySessionRecords())
	assert.Nil(uut.RefreshClientSession(client3, node3, time.Now()))

	// Case 6: refresh the client from a different node
	assert.NotNil(uut.RefreshClientSession(client3, uuid.New().String(), time.Now()))

	// Case 7: create a new client
	client7 := uuid.New().String()
	node7 := uuid.New().String()
	{
		record, err := uut.LogClientSession(client7, node7, time.Now())
		assert.Nil(err)
		assert.Equal(client7, record.ClientName)
		assert.Equal(node7, record.ServingNode)
	}

	// Case 8: clear a client record from wrong node
	assert.NotNil(uut.ClearClientSession(client3, uuid.New().String(), time.Now()))

	// Case 9: clear a client
	assert.Nil(uut.ReadySessionRecords())
	assert.Nil(uut.ClearClientSession(client3, node3, time.Now()))

	// Case 10: refresh client
	assert.NotNil(uut.RefreshClientSession(client3, node3, time.Now()))

	// Case 11: refresh remaining client
	assert.Nil(uut.RefreshClientSession(client7, node7, time.Now()))

	assert.Nil(tp.StopEventLoop())
	wg.Wait()
}

func TestSubscriptionRecorderTimeout(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	_, testKV, err := storage.CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	tp, err := common.GetNewTaskProcessorInstance("unit-test", 4)
	assert.Nil(err)

	testRecordPrefix := uuid.New().String()
	sessionRecords := fmt.Sprintf("%s/active-sessions", testRecordPrefix)
	uut, err := DefineSubscriptionRecorder(testKV, tp, testRecordPrefix, time.Second)
	assert.Nil(err)

	// Start the task processor
	wg := sync.WaitGroup{}
	assert.Nil(tp.StartEventLoop(&wg))

	assert.Nil(uut.ReadySessionRecords())

	// Case 0: run inactive check with no sessions
	assert.Nil(uut.ClearInactiveSessions(time.Second, time.Now()))

	startTime := time.Now()

	// Case 1: create new client record
	client1 := uuid.New().String()
	node1 := uuid.New().String()
	{
		record, err := uut.LogClientSession(client1, node1, startTime)
		assert.Nil(err)
		assert.Equal(client1, record.ClientName)
		assert.Equal(node1, record.ServingNode)
	}

	// Case 2: run inactive check
	assert.Nil(uut.ClearInactiveSessions(time.Second*10, startTime.Add(time.Second*5)))
	assert.Nil(uut.RefreshClientSession(client1, node1, startTime.Add(time.Second*6)))

	// Case 3: create new client record
	client3 := uuid.New().String()
	node3 := uuid.New().String()
	{
		record, err := uut.LogClientSession(client3, node3, startTime.Add(time.Second*3))
		assert.Nil(err)
		assert.Equal(client3, record.ClientName)
		assert.Equal(node3, record.ServingNode)
	}

	// Case 4: run inactive check
	assert.Nil(uut.ClearInactiveSessions(time.Second*3, startTime.Add(time.Second*7)))
	assert.NotNil(uut.RefreshClientSession(client3, node3, startTime.Add(time.Second*8)))
	assert.Nil(uut.RefreshClientSession(client1, node1, startTime.Add(time.Second*8)))
	{
		var entry SubscriptionRecords
		err := testKV.Get(sessionRecords, &entry, time.Second)
		assert.Nil(err)
		_, ok := entry.ActiveSessions[client3]
		assert.False(ok)
	}

	assert.Nil(tp.StopEventLoop())
	wg.Wait()
}
