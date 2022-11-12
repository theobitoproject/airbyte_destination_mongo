package mongo

import "go.mongodb.org/mongo-driver/mongo"

type collectionHandler struct {
	collection *mongo.Collection
	records    []interface{}
}

func newCollectionHandler(collection *mongo.Collection) *collectionHandler {
	return &collectionHandler{
		collection: collection,
		records:    []interface{}{},
	}
}
