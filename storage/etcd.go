package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apex/log"
	"gitlab.com/project-nan/httpmq/common"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcdDriver storage driver interacting with ETCD
type etcdDriver struct {
	common.Component
	client *clientv3.Client
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
	logTags := log.Fields{"module": "storage", "component": "etcd-storage"}
	log.WithFields(logTags).Infof("Connected with etcd servers %s", servers)
	return &etcdDriver{
		client: client, Component: common.Component{LogTags: logTags},
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
	targetQueue string, index int, timeout time.Duration,
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
func (d *etcdDriver) IndexRange(targetQueue string, timeout time.Duration) (int, int, error) {
	useContext, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := d.client.Get(useContext, targetQueue, clientv3.WithRev(0))
	cancel()
	if err != nil {
		log.WithError(err).WithFields(d.LogTags).Errorf("Failed to GET %s@0", targetQueue)
		return 0, 0, err
	}
	for _, kv := range resp.Kvs {
		return int(kv.CreateRevision), int(kv.ModRevision), nil
	}
	return -1, 0, nil
}

// Close close etcd storage driver
func (d *etcdDriver) Close() error {
	if err := d.client.Close(); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error("Failed to close driver")
		return err
	}
	return nil
}
