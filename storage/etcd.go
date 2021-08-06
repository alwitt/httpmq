package storage

import (
	"context"
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

// etcdDriver storage driver interacting with ETCD
type etcdDriver struct {
	common.Component
	client       *clientv3.Client
	session      *concurrency.Session
	knownMutexes map[string]*concurrency.Mutex
	lclMutex     sync.Mutex
}

// CreateEtcdDriver define an etcd storage driver
func CreateEtcdDriver(servers []string, timeout time.Duration) (Driver, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   servers,
		DialTimeout: timeout,
	})
	if err != nil {
		log.WithError(err).Errorf("Unable to connect with etcd servers %s", servers)
		return nil, err
	}
	session, err := concurrency.NewSession(client)
	if err != nil {
		log.WithError(err).Errorf("Unable to create concurrency session")
		return nil, err
	}
	logTags := log.Fields{"module": "storage", "component": "etcd-storage"}
	log.WithFields(logTags).Infof("Connected with etcd servers %s", servers)
	return &etcdDriver{
		client:       client,
		Component:    common.Component{LogTags: logTags},
		session:      session,
		knownMutexes: make(map[string]*concurrency.Mutex),
	}, nil
}

// Put record a new message
func (d *etcdDriver) Put(message common.Message, timeout time.Duration) error {
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
			"Failed to PUT %s <== %s", message.Destination.TargetQueue, string(toStore),
		)
		return err
	}
	log.WithFields(d.LogTags).Debugf(
		"PUT into %s@%d", message.Destination.TargetQueue, resp.Header.Revision,
	)
	return nil
}

// Get fetch message from target queue based on index
func (d *etcdDriver) Get(
	targetQueue string, index int64, timeout time.Duration,
) (common.Message, error) {
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := d.client.Get(useContext, targetQueue, clientv3.WithRev(int64(index)))
	cancel()
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf(
			"Failed to GET %s@%d", targetQueue, index,
		)
		return common.Message{}, err
	}
	// Parse the message to get the structure
	if len(resp.Kvs) != 1 {
		log.WithFields(d.LogTags).Errorf(
			"GET %s@%d did not return one entry %d", targetQueue, index, len(resp.Kvs),
		)
		return common.Message{}, fmt.Errorf(
			"[ETCD Storage Driver] GET %s@%d did not return one entry %d",
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

// GetNewest fetch the newest message from target queue
func (d *etcdDriver) GetNewest(
	targetQueue string, timeout time.Duration,
) (common.Message, error) {
	return d.Get(targetQueue, 0, timeout)
}

// GetOldest fetch oldest available message from target queue
func (d *etcdDriver) GetOldest(
	targetQueue string, timeout time.Duration,
) (common.Message, error) {
	oldest, _, err := d.IndexRange(targetQueue, timeout)
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf(
			"Can't fetch oldest message on %s", targetQueue,
		)
		return common.Message{}, err
	}
	return d.Get(targetQueue, oldest, timeout)
}

// IndexRange get the oldest, and newest available index on a target queue
func (d *etcdDriver) IndexRange(
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

// ReadStream read data stream from set of queues, and process messages from each
func (d *etcdDriver) ReadStream(targets []ReadStreamParam, stopFlag chan bool) error {
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

func (d *etcdDriver) runMessageHandler(
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

func (d *etcdDriver) getMutex(lockName string) *concurrency.Mutex {
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
func (d *etcdDriver) Lock(lockName string, timeout time.Duration) error {
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
func (d *etcdDriver) Unlock(lockName string, timeout time.Duration) error {
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
func (d *etcdDriver) Close() error {
	if err := d.session.Close(); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error("Failed to close session")
	}
	if err := d.client.Close(); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error("Failed to close driver")
		return err
	}
	return nil
}
