package subscription

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
	"gitlab.com/project-nan/httpmq/storage"
)

func TestSubscriptionRecorderBasic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	_, testKV, err := storage.CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp, err := common.GetNewTaskProcessorInstance("unit-test", 4, ctxt)
	assert.Nil(err)

	testRecordPrefix := uuid.New().String()
	sessionRecords := fmt.Sprintf("%s/active-sessions", testRecordPrefix)
	uut, err := DefineSubscriptionRecorder(testKV, tp, testRecordPrefix, time.Second)
	assert.Nil(err)

	// Start the task processor
	assert.Nil(tp.StartEventLoop(&wg))

	// Case 0: initialize session record tracking
	{
		var r SubscriptionRecords
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		err := testKV.Get(sessionRecords, &r, ctxt)
		cancel()
		assert.NotNil(err)
	}
	assert.Nil(uut.ReadySessionRecords(ctxt))
	{
		var entry SubscriptionRecords
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		err := testKV.Get(sessionRecords, &entry, ctxt)
		cancel()
		assert.Nil(err)
		assert.EqualValues(SubscriptionRecords{ActiveSessions: map[string]ClientSubscription{}}, entry)
	}

	// Case 1: refresh an unknown client
	assert.NotNil(
		uut.RefreshClientSession(uuid.New().String(), uuid.New().String(), time.Now(), ctxt),
	)

	// Case 2: clear an unknown client
	assert.NotNil(
		uut.ClearClientSession(uuid.New().String(), uuid.New().String(), time.Now(), ctxt),
	)

	// Case 3: create new client record
	client3 := uuid.New().String()
	node3 := uuid.New().String()
	{
		record, err := uut.LogClientSession(client3, node3, time.Now(), ctxt)
		assert.Nil(err)
		assert.Equal(client3, record.ClientName)
		assert.Equal(node3, record.ServingNode)
	}

	// Case 4: re-create the client record
	{
		record, err := uut.LogClientSession(client3, node3, time.Now(), ctxt)
		assert.NotNil(err)
		assert.Equal(client3, record.ClientName)
		assert.Equal(node3, record.ServingNode)
	}

	// Case 5: refresh the client record
	assert.Nil(uut.ReadySessionRecords(ctxt))
	assert.Nil(uut.RefreshClientSession(client3, node3, time.Now(), ctxt))

	// Case 6: refresh the client from a different node
	assert.NotNil(
		uut.RefreshClientSession(client3, uuid.New().String(), time.Now(), ctxt),
	)

	// Case 7: create a new client
	client7 := uuid.New().String()
	node7 := uuid.New().String()
	{
		record, err := uut.LogClientSession(client7, node7, time.Now(), ctxt)
		assert.Nil(err)
		assert.Equal(client7, record.ClientName)
		assert.Equal(node7, record.ServingNode)
	}

	// Case 8: clear a client record from wrong node
	assert.NotNil(
		uut.ClearClientSession(client3, uuid.New().String(), time.Now(), ctxt),
	)

	// Case 9: clear a client
	assert.Nil(uut.ReadySessionRecords(ctxt))
	assert.Nil(uut.ClearClientSession(client3, node3, time.Now(), ctxt))

	// Case 10: refresh client
	assert.NotNil(uut.RefreshClientSession(client3, node3, time.Now(), ctxt))

	// Case 11: refresh remaining client
	assert.Nil(uut.RefreshClientSession(client7, node7, time.Now(), ctxt))
}

func TestSubscriptionRecorderTimeout(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	_, testKV, err := storage.CreateEtcdBackedStorage([]string{"localhost:2379"}, time.Second*5)
	assert.Nil(err)

	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp, err := common.GetNewTaskProcessorInstance("unit-test", 4, ctxt)
	assert.Nil(err)

	testRecordPrefix := uuid.New().String()
	sessionRecords := fmt.Sprintf("%s/active-sessions", testRecordPrefix)
	uut, err := DefineSubscriptionRecorder(testKV, tp, testRecordPrefix, time.Second)
	assert.Nil(err)

	// Start the task processor
	assert.Nil(tp.StartEventLoop(&wg))

	assert.Nil(uut.ReadySessionRecords(ctxt))

	// Case 0: run inactive check with no sessions
	assert.Nil(uut.ClearInactiveSessions(time.Second, time.Now(), ctxt))

	startTime := time.Now()

	// Case 1: create new client record
	client1 := uuid.New().String()
	node1 := uuid.New().String()
	{
		record, err := uut.LogClientSession(client1, node1, startTime, ctxt)
		assert.Nil(err)
		assert.Equal(client1, record.ClientName)
		assert.Equal(node1, record.ServingNode)
	}

	// Case 2: run inactive check
	assert.Nil(
		uut.ClearInactiveSessions(time.Second*10, startTime.Add(time.Second*5), ctxt),
	)
	assert.Nil(
		uut.RefreshClientSession(client1, node1, startTime.Add(time.Second*6), ctxt),
	)

	// Case 3: create new client record
	client3 := uuid.New().String()
	node3 := uuid.New().String()
	{
		record, err := uut.LogClientSession(
			client3, node3, startTime.Add(time.Second*3), ctxt,
		)
		assert.Nil(err)
		assert.Equal(client3, record.ClientName)
		assert.Equal(node3, record.ServingNode)
	}

	// Case 4: run inactive check
	assert.Nil(
		uut.ClearInactiveSessions(time.Second*3, startTime.Add(time.Second*7), ctxt),
	)
	assert.NotNil(
		uut.RefreshClientSession(client3, node3, startTime.Add(time.Second*8), ctxt),
	)
	assert.Nil(
		uut.RefreshClientSession(client1, node1, startTime.Add(time.Second*8), ctxt),
	)
	{
		var entry SubscriptionRecords
		ctxt, cancel := context.WithTimeout(context.Background(), time.Second)
		err := testKV.Get(sessionRecords, &entry, ctxt)
		cancel()
		assert.Nil(err)
		_, ok := entry.ActiveSessions[client3]
		assert.False(ok)
	}
}
