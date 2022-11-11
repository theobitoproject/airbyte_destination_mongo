package main

import "go.mongodb.org/mongo-driver/bson"

type mongoRecord struct {
	stream string
	data   bson.D
}

type mongoRecordChannel chan *mongoRecord

func newMongoRecordChannel() mongoRecordChannel {
	return make(mongoRecordChannel)
}
