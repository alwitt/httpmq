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
