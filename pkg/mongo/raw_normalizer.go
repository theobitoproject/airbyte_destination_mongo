package mongo

import (
	"strconv"

	"github.com/theobitoproject/kankuro/pkg/protocol"
	"go.mongodb.org/mongo-driver/bson"
)

type rawNormalizer struct{}

func newRawNormalizer() Normalizer {
	return &rawNormalizer{}
}

// Marshal takes an Airbyte record and
// and returns the respective document
func (n *rawNormalizer) Marshal(rec *protocol.Record) (*document, error) {
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
		bson.E{Key: protocol.AirbyteData, Value: rawRec.Data},
	)
	return doc, nil
}
