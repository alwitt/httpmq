package dataplane

import (
	"encoding/base64"
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
	Stream   uint64 `json:"for_stream" validate:"gte=0"`
	Consumer uint64 `json:"for_consumer" validate:"gte=0"`
}

// MsgToDeliver a structure for representing a message to send out
type MsgToDeliver struct {
	Stream   string          `json:"stream" validate:"required"`
	Subject  string          `json:"subject" validate:"required"`
	Consumer string          `json:"consumer" validate:"required"`
	Sequence MsgToDeliverSeq `json:"sequence" validate:"required,dive"`
	Message  string          `json:"msg_base64" validate:"required"`
}

// ConvertJSMessageDeliver convert a JetStream message for delivery
func ConvertJSMessageDeliver(subject string, msg *nats.Msg) (*MsgToDeliver, error) {
	meta, err := msg.Metadata()
	if err == nil {
		return &MsgToDeliver{
			Stream:   meta.Stream,
			Subject:  subject,
			Consumer: meta.Consumer,
			Sequence: MsgToDeliverSeq{
				Stream: meta.Sequence.Stream, Consumer: meta.Sequence.Consumer,
			},
			Message: base64.StdEncoding.EncodeToString(msg.Data),
		}, nil
	}
	return nil, err
}

// GetOriginalMsg given a message, get the original decoded data
func (m *MsgToDeliver) GetOriginalMsg() ([]byte, error) {
	return base64.StdEncoding.DecodeString(m.Message)
}
