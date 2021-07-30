package storage

import (
	"time"

	"gitlab.com/project-nan/httpmq/common"
)

// Driver storage driver
type Driver interface {
	Put(message common.Message, timeout time.Duration) error
	Get(targetQueue string, index int, timeout time.Duration) (common.Message, error)
	GetNewest(targetQueue string, timeout time.Duration) (common.Message, error)
	GetOldest(targetQueue string, timeout time.Duration) (common.Message, error)
	IndexRange(targetQueue string, timeout time.Duration) (int, int, error)
	Close() error
}
