package dataplane

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

// msgToString helper function for standardizing the printing of nats.Msg
func msgToString(msg *nats.Msg) string {
	if meta, err := msg.Metadata(); err == nil {
		msgName := fmt.Sprintf(
			"%s@%s:MSG[S:%d C:%d]",
			meta.Consumer,
			meta.Stream,
			meta.Sequence.Stream,
			meta.Sequence.Consumer,
		)
		return msgName
	}
	return msg.Subject
}

// ==============================================================================

// MsgToDeliverSeq JetStream message sequence parameter
type MsgToDeliverSeq struct {
	Stream   uint64 `json:"stream" validate:"gte=0"`
	Consumer uint64 `json:"consumer" validate:"gte=0"`
}

// MsgToDeliver a structure for representing a message to send out
type MsgToDeliver struct {
	Stream   string          `json:"stream" validate:"required"`
	Subject  string          `json:"subject" validate:"required"`
	Consumer string          `json:"consumer" validate:"required"`
	Sequence MsgToDeliverSeq `json:"sequence" validate:"required,dive"`
	Message  []byte          `json:"b64_msg" validate:"required"`
}

// ConvertJSMessageDeliver convert a JetStream message for delivery
func ConvertJSMessageDeliver(subject string, msg *nats.Msg) (MsgToDeliver, error) {
	meta, err := msg.Metadata()
	if err == nil {
		return MsgToDeliver{
			Stream:   meta.Stream,
			Subject:  subject,
			Consumer: meta.Consumer,
			Sequence: MsgToDeliverSeq{
				Stream: meta.Sequence.Stream, Consumer: meta.Sequence.Consumer,
			},
			Message: msg.Data,
		}, nil
	}
	return MsgToDeliver{}, err
}

// String toString function
func (m MsgToDeliver) String() string {
	return fmt.Sprintf(
		"%s@%s/%s:MSG[S:%d C:%d]",
		m.Consumer,
		m.Stream, m.Subject,
		m.Sequence.Stream,
		m.Sequence.Consumer,
	)
}
