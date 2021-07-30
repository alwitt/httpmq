package common

import "time"

// MessageSource message generator info
type MessageSource struct {
	Sender string `json:"sender" validate:"required"`
	// Sender's timestamp of when it generated the message
	SentAt time.Time `json:"sent_at"`
}

// MessageDestination message destination info
type MessageDestination struct {
	TargetQueue string `json:"target_queue" validate:"required"`
}

// Message representing one message
type Message struct {
	Source      MessageSource      `json:"source" validate:"required,dive"`
	Destination MessageDestination `json:"destination" validate:"required,dive"`
	Tags        map[string]string  `json:"tags,omitempty" validate:"omitempty"`
	ReceivedAt  time.Time          `json:"received_at"`
	Body        []byte             `json:"body" validate:"required"`
}
