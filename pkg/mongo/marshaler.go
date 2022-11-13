package mongo

import (
	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

// Marshaler takes incoming record messages,
// creates mongo documents and
// send these to the next stage of processing
type Marshaler interface {
	// AddWorker adds a new thread to marshal records async
	// and send these to the next stage of processing
	AddWorker(messenger.ChannelHub, *protocol.ConfiguredCatalog, bool)
}

type marshaler struct {
	documentChannel DocumentChannel
	workersDoneChan chan bool
}

// NewRawMarshaler creates an instance of Marshaler
// related to the Airbyte raw format
func NewRawMarshaler(
	documentChannel DocumentChannel,
	workersDoneChan chan bool,
) Marshaler {
	return &marshaler{
		documentChannel: documentChannel,
		workersDoneChan: workersDoneChan,
	}
}

// AddWorker adds a new thread to marshal records async
// and send these to the next stage of processing
func (m *marshaler) AddWorker(
	hub messenger.ChannelHub,
	cc *protocol.ConfiguredCatalog,
	enableBasicNormalization bool,
) {
	normalizer := newNormalizer(cc, enableBasicNormalization)

	go func() {
		for {
			rec, channelOpen := <-hub.GetRecordChannel()
			if !channelOpen {
				m.removeWorker()
				return
			}

			doc, err := normalizer.Marshal(rec)
			if err != nil {
				hub.GetErrorChannel() <- err
				continue
			}

			m.documentChannel <- doc
		}
	}()
}

func (rm *marshaler) removeWorker() {
	rm.workersDoneChan <- true
}
