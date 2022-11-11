package main

import (
	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
	"go.mongodb.org/mongo-driver/bson"
)

type recordMarshaler struct {
	hub                 messenger.ChannelHub
	mongoRecordChannel  mongoRecordChannel
	workersDoneChan     chan bool
	propertiesPerStream map[string][]string
}

func newRecordMarshaler(
	hub messenger.ChannelHub,
	mongoRecordChannel mongoRecordChannel,
	workersDoneChan chan bool,
) *recordMarshaler {
	return &recordMarshaler{
		hub:                 hub,
		mongoRecordChannel:  mongoRecordChannel,
		workersDoneChan:     workersDoneChan,
		propertiesPerStream: map[string][]string{},
	}
}

func (rm *recordMarshaler) addWorker() {
	go func() {
		for {
			rec, channelOpen := <-rm.hub.GetRecordChannel()
			if !channelOpen {
				rm.removeWorker()
				return
			}

			mongoRec, err := rm.marshal(rec)
			if err != nil {
				rm.hub.GetErrorChannel() <- err
				continue
			}

			rm.mongoRecordChannel <- mongoRec
		}
	}()
}

func (rm *recordMarshaler) removeWorker() {
	rm.workersDoneChan <- true
}

func (rm *recordMarshaler) marshal(rec *protocol.Record) (*mongoRecord, error) {
	mongoRec := mongoRecord{
		stream: rec.Stream,
		data:   bson.D{},
	}

	properties := rm.propertiesPerStream[rec.Stream]

	for _, p := range properties {
		data := *rec.Data
		mongoRec.data = append(
			mongoRec.data,
			bson.E{Key: p, Value: data[p]},
		)
	}

	return &mongoRec, nil
}

func (rm *recordMarshaler) extractProperties(
	streams []protocol.ConfiguredStream,
) {
	for _, stream := range streams {
		properties := []string{}

		for propertyName := range stream.Stream.JSONSchema.Properties {
			properties = append(properties, string(propertyName))
		}

		rm.propertiesPerStream[stream.Stream.Name] = properties
	}
}
