package mongo

import "go.mongodb.org/mongo-driver/bson"

// DocumentChannel is a channel to share documents
type DocumentChannel chan *document

type document struct {
	stream string
	data   bson.D
}

// NewDocumentChannel creates a new instance of DocumentChannel
func NewDocumentChannel() DocumentChannel {
	return make(DocumentChannel)
}
