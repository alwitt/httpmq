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

// Driver storage driver
type Driver interface {
	Put(message common.Message, timeout time.Duration) error
	Get(targetQueue string, index int64, timeout time.Duration) (common.Message, error)
	GetNewest(targetQueue string, timeout time.Duration) (common.Message, error)
	GetOldest(targetQueue string, timeout time.Duration) (common.Message, error)
	IndexRange(targetQueue string, timeout time.Duration) (int64, int64, error)
	ReadStream(targets []ReadStreamParam, stopFlag chan bool) error
	Lock(lockName string, timeout time.Duration) error
	Unlock(lockName string, timeout time.Duration) error
	Close() error
}
