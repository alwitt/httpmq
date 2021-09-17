package storage

import (
	"context"

	"database/sql"
	"database/sql/driver"

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
	Write(message common.Message, ctxt context.Context) error
	Read(targetQueue string, index int64, ctxt context.Context) (common.Message, error)
	ReadNewest(targetQueue string, ctxt context.Context) (common.Message, error)
	ReadOldest(targetQueue string, ctxt context.Context) (common.Message, error)
	IndexRange(targetQueue string, ctxt context.Context) (int64, int64, error)
	ReadStream(target ReadStreamParam, ctxt context.Context) (int64, error)
	// Mutex related operations
	Lock(lockName string, ctxt context.Context) error
	Unlock(lockName string, ctxt context.Context) error
	Close() error
}

// KeyValueStore key-value store operator
type KeyValueStore interface {
	// Key-Value store related operations
	Set(key string, value driver.Valuer, ctxt context.Context) error
	Get(key string, result sql.Scanner, ctxt context.Context) error
	Delete(key string, ctxt context.Context) error
	// Mutex related operations
	Lock(lockName string, ctxt context.Context) error
	Unlock(lockName string, ctxt context.Context) error
	Close() error
}
