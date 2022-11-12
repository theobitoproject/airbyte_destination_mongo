package mongo

import (
	"strconv"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
	"go.mongodb.org/mongo-driver/bson"
)

type rawMarshaler struct {
	documentChannel DocumentChannel
	workersDoneChan chan bool
}

// NewRawMarshaler creates an instance of Marshaler
// related to the Airbyte raw format
func NewRawMarshaler(
	documentChannel DocumentChannel,
	workersDoneChan chan bool,
) Marshaler {
	return &rawMarshaler{
		documentChannel: documentChannel,
		workersDoneChan: workersDoneChan,
	}
}

// AddWorker adds a new thread to marshal records async
// and send these to the next stage of processing
func (m *rawMarshaler) AddWorker(hub messenger.ChannelHub) {
	go func() {
		for {
			rec, channelOpen := <-hub.GetRecordChannel()
			if !channelOpen {
				m.removeWorker()
				return
			}

			doc, err := m.marshal(rec)
			if err != nil {
				hub.GetErrorChannel() <- err
				continue
			}

			m.documentChannel <- doc
		}
	}()
}

func (rm *rawMarshaler) removeWorker() {
	rm.workersDoneChan <- true
}

func (rm *rawMarshaler) marshal(rec *protocol.Record) (*document, error) {
	doc := &document{
		stream: rec.Stream,
		data:   bson.D{},
	}

	rawRec := rec.GetRawRecord()
	doc.data = append(
		doc.data,
		bson.E{Key: protocol.AirbyteAbId, Value: rawRec.ID},
	)
	doc.data = append(
		doc.data,
		bson.E{Key: protocol.AirbyteEmittedAt, Value: strconv.Itoa(int(rawRec.EmittedAt))},
	)
	doc.data = append(
		doc.data,
		bson.E{Key: protocol.AirbyteData, Value: rawRec.Data.String()},
	)
	return doc, nil
}
