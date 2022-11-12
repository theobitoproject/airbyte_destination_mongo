package mongo

import (
	"github.com/theobitoproject/kankuro/pkg/messenger"
)

// Marshaler takes incoming record messages,
// creates mongo documents and
// send these to the next stage of processing
type Marshaler interface {
	// AddWorker adds a new thread to marshal records async
	// and send these to the next stage of processing
	AddWorker(messenger.ChannelHub)
	// ExtractFields defines the fields for mongo documents
	// ExtractFields([]protocol.ConfiguredStream)
}
