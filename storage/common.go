package storage

import (
	"time"

	"gitlab.com/project-nan/httpmq/common"
)

// MessageProcessor handler function for processing a message. Used with message streaming
type MessageProcessor func(msg common.Message, index int64) (bool, error)

// ReadStreamParam parameters relating to one ReadStream target
type ReadStreamParam struct {
	TargetQueue string
	StartIndex  int64
	Handler     MessageProcessor
}

// MessageQueues message queue operator
type MessageQueues interface {
	// Queue related operations
	Write(message common.Message, timeout time.Duration) error
	Read(targetQueue string, index int64, timeout time.Duration) (common.Message, error)
	ReadNewest(targetQueue string, timeout time.Duration) (common.Message, error)
	ReadOldest(targetQueue string, timeout time.Duration) (common.Message, error)
	IndexRange(targetQueue string, timeout time.Duration) (int64, int64, error)
	ReadStream(targets []ReadStreamParam, stopFlag chan bool) error
	// Mutex related operations
	Lock(lockName string, timeout time.Duration) error
	Unlock(lockName string, timeout time.Duration) error
	Close() error
}

// KeyValueStore key-value store operator
type KeyValueStore interface {
	// Key-Value store related operations
	Set(key string, value string, timeout time.Duration) error
	Get(key string, timeout time.Duration) (string, error)
	Delete(key string, timeout time.Duration) error
	// Mutex related operations
	Lock(lockName string, timeout time.Duration) error
	Unlock(lockName string, timeout time.Duration) error
	Close() error
}
