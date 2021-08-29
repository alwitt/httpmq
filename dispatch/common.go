package dispatch

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"gitlab.com/project-nan/httpmq/common"
)

// InfightMessageInfo info regarding an inflight message
type InfightMessageInfo struct {
	Index       int64     `json:"index"`
	FirstSentAt time.Time `json:"first_sent_at"`
	RetryCount  int       `json:"retry_count"`
	MaxRetry    int       `json:"max_retry"`
}

// QueueSubInfo tracks the status of a client's subscription to queue
type QueueSubInfo struct {
	NewestACKedIndex int64                        `json:"acked_index"`
	Inflight         map[int64]InfightMessageInfo `json:"inflight"`
}

// Scan implements the sql.Scanner interface
func (r *QueueSubInfo) Scan(src interface{}) error {
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("src is not []byte")
	}
	return json.Unmarshal(bytes, r)
}

// Value implements the sql/driver.Valuer interface
func (r QueueSubInfo) Value() (driver.Value, error) {
	return json.Marshal(&r)
}

// MessageInFlight wrapper around common.Message which include additional metadata
type MessageInFlight struct {
	common.Message
	Index      int64 `json:"index"`
	Redelivery bool  `json:"redelivery"`
}

// Scan implements the sql.Scanner interface
func (r *MessageInFlight) Scan(src interface{}) error {
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("src is not []byte")
	}
	return json.Unmarshal(bytes, r)
}

// Value implements the sql/driver.Valuer interface
func (r MessageInFlight) Value() (driver.Value, error) {
	return json.Marshal(&r)
}

// ========================================================================================

// SubmitMessage function signature for sending a message
type SubmitMessage func(msg MessageInFlight) error

// registerInflightMessage function signature for registering a new message is inflight
type registerInflightMessage func(msgIdx int64) error
