// Copyright 2021-2022 The httpmq Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataplane

import (
	"fmt"

	"github.com/alwitt/httpmq/common"
	"github.com/go-playground/validator/v10"
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

// wrapperForValidation a helper structure used for validation naming tuples
type wrapperForValidation struct {
	Stream        string  `validate:"alphaunicode|uuid"`
	Consumer      string  `validate:"alphaunicode|uuid"`
	DeliveryGroup *string `validate:"omitempty,alphaunicode|uuid"`
	Subject       *string
}

// Validate perform the validation
func (w wrapperForValidation) Validate(validate *validator.Validate) error {
	if err := validate.Struct(&w); err != nil {
		return err
	}
	if w.Subject != nil {
		return common.ValidateSubjectName(*w.Subject)
	}
	return nil
}

// ==============================================================================

// MsgToDeliverSeq sequence numbers for a JetStream message
type MsgToDeliverSeq struct {
	// Stream is the message sequence number within the stream
	Stream uint64 `json:"stream" validate:"required,gte=0"`
	// Consumer is the message sequence number for this consumer
	Consumer uint64 `json:"consumer" validate:"required,gte=0"`
}

// MsgToDeliver a structure for representing a message to send out to a subscribing client
type MsgToDeliver struct {
	// Stream is the name of the stream
	Stream string `json:"stream" validate:"required,alphaunicode|uuid"`
	// Subject is the name of the subject / subject filter
	Subject string `json:"subject" validate:"required"`
	// Consumer is the name of the consumer
	Consumer string `json:"consumer" validate:"required,alphaunicode|uuid"`
	// Sequence is the sequence numbers for this JetStream message
	Sequence MsgToDeliverSeq `json:"sequence" validate:"required,dive"`
	// Message is the message body
	Message []byte `json:"b64_msg" validate:"required" swaggertype:"string" format:"base64" example:"SGVsbG8gV29ybGQK"`
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

// String toString function for MsgToDeliver
func (m MsgToDeliver) String() string {
	return fmt.Sprintf(
		"%s@%s/%s:MSG[S:%d C:%d]",
		m.Consumer,
		m.Stream, m.Subject,
		m.Sequence.Stream,
		m.Sequence.Consumer,
	)
}
