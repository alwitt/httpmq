package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/apex/log"
	"gitlab.com/project-nan/httpmq/common"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// etcdBackedStorage driver for interacting with ETCD as message queues
type etcdBackedStorage struct {
	common.Component
	client       *clientv3.Client
	session      *concurrency.Session
	knownMutexes map[string]*concurrency.Mutex
	lclMutex     sync.Mutex
}

// CreateEtcdBackedStorage define an etcd backed storage driver
func CreateEtcdBackedStorage(servers []string, timeout time.Duration) (
	MessageQueues, KeyValueStore, error,
) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   servers,
		DialTimeout: timeout,
	})
	if err != nil {
		log.WithError(err).Errorf("Unable to connect with etcd servers %s", servers)
		return nil, nil, err
	}
	session, err := concurrency.NewSession(client)
	if err != nil {
		log.WithError(err).Errorf("Unable to create concurrency session")
		return nil, nil, err
	}
	logTags := log.Fields{"module": "storage", "component": "etcd-backed"}
	log.WithFields(logTags).Infof("Connected with etcd servers %s", servers)
	instance := &etcdBackedStorage{
		client:       client,
		Component:    common.Component{LogTags: logTags},
		session:      session,
		knownMutexes: make(map[string]*concurrency.Mutex),
	}
	return instance, instance, nil
}

// ================================================================
// Message queue related operations

// Write record a new message
func (d *etcdBackedStorage) Write(message common.Message, timeout time.Duration) error {
	// the message is stored as serialized JSON
	toStore, err := json.Marshal(&message)
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Error("Unable to serialize message for storage")
		return err
	}
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := d.client.Put(
		useContext, message.Destination.TargetQueue, string(toStore),
	)
	cancel()
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf(
			"Failed to WRITE %s <== %s", message.Destination.TargetQueue, string(toStore),
		)
		return err
	}
	log.WithFields(d.LogTags).Debugf(
		"WRITE into %s@%d", message.Destination.TargetQueue, resp.Header.Revision,
	)
	return nil
}

// Read fetch message from target queue based on index
func (d *etcdBackedStorage) Read(
	targetQueue string, index int64, timeout time.Duration,
) (common.Message, error) {
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := d.client.Get(useContext, targetQueue, clientv3.WithRev(int64(index)))
	cancel()
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf(
			"Failed to READ %s@%d", targetQueue, index,
		)
		return common.Message{}, err
	}
	// Parse the message to get the structure
	if len(resp.Kvs) != 1 {
		log.WithFields(d.LogTags).Errorf(
			"READ %s@%d did not return one entry %d", targetQueue, index, len(resp.Kvs),
		)
		return common.Message{}, fmt.Errorf(
			"[ETCD Storage Driver] READ %s@%d did not return one entry %d",
			targetQueue,
			index,
			len(resp.Kvs),
		)
	}
	stored := resp.Kvs[0].Value
	var message common.Message
	if err := json.Unmarshal(stored, &message); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf(
			"Unable to parse message %s@%d", targetQueue, index,
		)
		return common.Message{}, err
	}
	return message, nil
}

// ReadNewest fetch the newest message from target queue
func (d *etcdBackedStorage) ReadNewest(
	targetQueue string, timeout time.Duration,
) (common.Message, error) {
	return d.Read(targetQueue, 0, timeout)
}

// ReadOldest fetch oldest available message from target queue
func (d *etcdBackedStorage) ReadOldest(
	targetQueue string, timeout time.Duration,
) (common.Message, error) {
	oldest, _, err := d.IndexRange(targetQueue, timeout)
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf(
			"Can't fetch oldest message on %s", targetQueue,
		)
		return common.Message{}, err
	}
	return d.Read(targetQueue, oldest, timeout)
}

// IndexRange get the oldest, and newest available index on a target queue
func (d *etcdBackedStorage) IndexRange(
	targetQueue string, timeout time.Duration,
) (int64, int64, error) {
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := d.client.Get(useContext, targetQueue, clientv3.WithRev(0))
	cancel()
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to GET %s@0", targetQueue)
		return 0, 0, err
	}
	for _, kv := range resp.Kvs {
		return (kv.CreateRevision), (kv.ModRevision), nil
	}
	return -1, 0, nil
}

// ReadStream read data stream from queue, and process message
func (d *etcdBackedStorage) ReadStream(target ReadStreamParam, stopFlag chan bool) error {
	dataStream := d.client.Watch(
		context.Background(),
		target.TargetQueue,
		clientv3.WithRev(int64(target.StartIndex)),
	)

	// Start stream processing
	for {
		select {
		case message, ok := <-dataStream:
			if !ok {
				log.WithFields(d.LogTags).Infof("Watch channel for %s closed", target.TargetQueue)
				return nil
			}
			log.WithFields(d.LogTags).Debugf(
				"Process message from channel %s", target.TargetQueue,
			)
			handlerActive, err := d.runMessageHandler(
				target.TargetQueue, message, target.Handler,
			)
			if err != nil {
				log.WithError(err).WithFields(d.LogTags).Errorf(
					"Channel %s handler failed", target.TargetQueue,
				)
				return err
			}
			if !handlerActive {
				log.WithFields(d.LogTags).Infof("Channel %s handler inactive", target.TargetQueue)
				return nil
			}
		case done, ok := <-stopFlag:
			if !ok {
				log.WithFields(d.LogTags).Errorf("stopFlag channel did not return valid signal")
				return nil
			}
			if done {
				// Received stop signal
				log.WithFields(d.LogTags).Info("Received stop signal. Exiting watch")
				return nil
			}
		}
	}
}

// ReadStreams read data stream from set of queues, and process messages from each
func (d *etcdBackedStorage) ReadStreams(targets []ReadStreamParam, stopFlag chan bool) error {
	readSelects := make([]reflect.SelectCase, len(targets)+1)
	dataStreams := make([]clientv3.WatchChan, len(targets))
	activeChannels := make([]bool, len(targets))
	for index, oneTarget := range targets {
		theChannel := d.client.Watch(
			context.Background(),
			oneTarget.TargetQueue,
			clientv3.WithRev(int64(oneTarget.StartIndex)),
		)
		dataStreams[index] = theChannel
		readSelects[index] = reflect.SelectCase{
			Dir: reflect.SelectRecv, Chan: reflect.ValueOf(theChannel),
		}
		activeChannels[index] = true
	}
	// Add a the stop flag channel
	readSelects[len(targets)] = reflect.SelectCase{
		Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stopFlag),
	}

	// Start stream processing
	haveActive := false
	var reference clientv3.WatchResponse
	for {
		// Check whether all streams are still functional
		haveActive = false
		for _, isActive := range activeChannels {
			if isActive {
				haveActive = true
				break
			}
		}
		// All channels are now inactive
		if !haveActive {
			break
		}
		// Wait for messages
		chosen, received, ok := reflect.Select(readSelects)
		if ok && chosen == len(targets) {
			// Received stop signal
			log.WithFields(d.LogTags).Info("Received stop signal. Exiting watch")
			return nil
		}
		if !ok {
			activeChannels[chosen] = false
			log.WithFields(d.LogTags).Infof(
				"Watch channel for %s closed", targets[chosen].TargetQueue,
			)
			continue
		}
		// Process the received message
		if received.Type() == reflect.TypeOf(reference) {
			converted, ok := received.Interface().(clientv3.WatchResponse)
			if !ok {
				err := fmt.Errorf(
					"unable to convert message from channel %s to clientv3.WatchResponse",
					targets[chosen].TargetQueue,
				)
				log.WithError(err).WithFields(d.LogTags).Errorf(
					"Watch failure on channel %s", targets[chosen].TargetQueue,
				)
				return err
			}
			log.WithFields(d.LogTags).Debugf(
				"Process message from channel %s", targets[chosen].TargetQueue,
			)
			handlerActive, err := d.runMessageHandler(
				targets[chosen].TargetQueue, converted, targets[chosen].Handler,
			)
			if err != nil {
				log.WithError(err).WithFields(d.LogTags).Errorf(
					"Channel %s handler failed", targets[chosen].TargetQueue,
				)
				return err
			}
			if !handlerActive {
				activeChannels[chosen] = false
			}
		} else {
			log.WithFields(d.LogTags).Errorf(
				"Watch channel for %s send unexpected message: %v",
				targets[chosen].TargetQueue,
				received.Type(),
			)
		}
	}

	return nil
}

func (d *etcdBackedStorage) runMessageHandler(
	targetQueue string, msg clientv3.WatchResponse, handler MessageProcessor,
) (bool, error) {
	// Process potentially multiple events
	for _, oneEvent := range msg.Events {
		index := oneEvent.Kv.ModRevision
		stored := oneEvent.Kv.Value
		var message common.Message
		if err := json.Unmarshal(stored, &message); err != nil {
			log.WithError(err).WithFields(d.LogTags).Errorf(
				"Failed to parse message of key %s", string(oneEvent.Kv.Key),
			)
			return false, err
		}
		handlerActive, err := handler(message, index)
		if err != nil {
			log.WithError(err).WithFields(d.LogTags).Errorf(
				"Failed to process message of key %s", string(oneEvent.Kv.Key),
			)
			return false, err
		}
		if !handlerActive {
			log.WithFields(d.LogTags).Infof("Handler for channel %s is inactive", targetQueue)
			return false, nil
		}
	}
	return true, nil
}

func (d *etcdBackedStorage) getMutex(lockName string) *concurrency.Mutex {
	d.lclMutex.Lock()
	defer d.lclMutex.Unlock()
	theMutex, ok := d.knownMutexes[lockName]
	if !ok {
		theMutex = concurrency.NewMutex(d.session, lockName)
		d.knownMutexes[lockName] = theMutex
	}
	return theMutex
}

// Lock acquire a named lock given a timeout
func (d *etcdBackedStorage) Lock(lockName string, timeout time.Duration) error {
	theMutex := d.getMutex(lockName)
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := theMutex.Lock(useContext); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Unable to lock %s", lockName)
		return err
	}
	return nil
}

// Unlock release a named lock given a timeout
func (d *etcdBackedStorage) Unlock(lockName string, timeout time.Duration) error {
	theMutex := d.getMutex(lockName)
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := theMutex.Unlock(useContext); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Unable to unlock %s", lockName)
		return err
	}
	return nil
}

// Close close etcd storage driver
func (d *etcdBackedStorage) Close() error {
	if err := d.session.Close(); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error("Failed to close session")
	}
	if err := d.client.Close(); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error("Failed to close driver")
		return err
	}
	return nil
}

// ================================================================
// Key-Value store related operations

// Set2 record a K/V pair in etcd
func (d *etcdBackedStorage) Set(key string, value driver.Valuer, timeout time.Duration) error {
	serialized, err := value.Value()
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to SET %s", key)
		return err
	}
	// Convert to byte
	asBytes, ok := serialized.([]byte)
	if !ok {
		err := fmt.Errorf("unable to convert value output to []byte for storage")
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to SET %s", key)
		return err
	}
	// Insert into ETCD
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := d.client.Put(useContext, key, string(asBytes))
	cancel()
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to SET %s <== %s", key, asBytes)
		return err
	}
	log.WithFields(d.LogTags).Debugf(
		"SET %s@%d <== %s", key, resp.Header.Revision, asBytes,
	)
	return nil
}

// Get2 read a K/V pair from etcd
func (d *etcdBackedStorage) Get(key string, result sql.Scanner, timeout time.Duration) error {
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := d.client.Get(useContext, key, clientv3.WithRev(int64(0)))
	cancel()
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to GET %s@0", key)
		return err
	}
	// Parse the message to get the structure
	if len(resp.Kvs) != 1 {
		log.WithFields(d.LogTags).Errorf(
			"GET %s@0 did not return one entry %d", key, len(resp.Kvs),
		)
		return fmt.Errorf(
			"[ETCD Storage Driver] READ %s@0 did not return one entry %d", key, len(resp.Kvs),
		)
	}
	// Give input to scanner for parsing
	if err := result.Scan(resp.Kvs[0].Value); err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to GET %s@0", key)
		return err
	}
	return nil
}

// Delete delete a key from ETCD
func (d *etcdBackedStorage) Delete(key string, timeout time.Duration) error {
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := d.client.Delete(useContext, key, clientv3.WithPrefix())
	cancel()
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to DELETE %s", key)
		return err
	}
	log.WithFields(d.LogTags).Infof("Deleted %d instances of %s", resp.Deleted, key)
	return nil
}
