package mongo

import (
	"github.com/theobitoproject/kankuro/pkg/protocol"
	"go.mongodb.org/mongo-driver/bson"
)

type basicNormalizer struct {
	propertiesPerStream map[string][]string
}

func newBasicNormalizer(cc *protocol.ConfiguredCatalog) Normalizer {
	propertiesPerStream := getPropertiesPerStream(cc)
	return &basicNormalizer{propertiesPerStream}
}

// Marshal takes an Airbyte record and
// and returns the respective document
func (n *basicNormalizer) Marshal(rec *protocol.Record) (*document, error) {
	doc := &document{
		stream: rec.Stream,
		data:   bson.D{},
	}

	properties := n.propertiesPerStream[rec.Stream]

	for _, p := range properties {
		data := *rec.Data
		doc.data = append(
			doc.data,
			bson.E{Key: p, Value: data[p]},
		)
	}

	return doc, nil
}

func getPropertiesPerStream(cc *protocol.ConfiguredCatalog) map[string][]string {
	propertiesPerStream := map[string][]string{}

	for _, stream := range cc.Streams {
		properties := []string{}

		for propertyName := range stream.Stream.JSONSchema.Properties {
			properties = append(properties, string(propertyName))
		}

		propertiesPerStream[stream.Stream.Name] = properties
	}

	return propertiesPerStream
}
