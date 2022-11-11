package main

import (
	"context"
	"sync"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"go.mongodb.org/mongo-driver/mongo"
)

type mongoRepository struct {
	hub                messenger.ChannelHub
	mongoRecordChannel mongoRecordChannel

	workersDoneChan          chan bool
	database                 *mongo.Database
	collectionConnsPerStream map[string]*collectionConn
	mu                       *sync.Mutex
	batchSize                int
}

type collectionConn struct {
	collection *mongo.Collection
	records    []interface{}
}

func newMongoRepository(
	hub messenger.ChannelHub,
	mongoRecordChannel mongoRecordChannel,
	workersDoneChan chan bool,
	database *mongo.Database,
	batchSize int,
) *mongoRepository {
	return &mongoRepository{
		hub:                      hub,
		mongoRecordChannel:       mongoRecordChannel,
		workersDoneChan:          workersDoneChan,
		database:                 database,
		collectionConnsPerStream: map[string]*collectionConn{},
		mu:                       &sync.Mutex{},
		batchSize:                batchSize,
	}
}

func (m *mongoRepository) addWorker() {
	go func() {
		for {
			rec, channelOpen := <-m.mongoRecordChannel
			if !channelOpen {
				m.flushAll()
				m.removeWorker()
				return
			}

			m.mu.Lock()

			cc, err := m.getCollectionConnForStream(rec.stream)
			if err != nil {
				m.hub.GetErrorChannel() <- err
				continue
			}

			cc.records = append(cc.records, rec.data)

			if len(cc.records) < m.batchSize {
				m.mu.Unlock()
				continue
			}

			ctx := context.TODO()
			_, err = cc.collection.InsertMany(ctx, cc.records)
			if err != nil {
				m.hub.GetErrorChannel() <- err
				continue
			}
			cc.records = []interface{}{}

			m.mu.Unlock()
		}
	}()
}

func (m *mongoRepository) flushAll() {
	for _, cc := range m.collectionConnsPerStream {
		if len(cc.records) == 0 {
			continue
		}

		ctx := context.TODO()
		_, err := cc.collection.InsertMany(ctx, cc.records)
		if err != nil {
			m.hub.GetErrorChannel() <- err
			continue
		}
	}
}

func (m *mongoRepository) removeWorker() {
	m.workersDoneChan <- true
}

func (m *mongoRepository) getCollectionConnForStream(
	stream string,
) (*collectionConn, error) {
	cc, created := m.collectionConnsPerStream[stream]
	if created {
		return cc, nil
	}

	collection := m.database.Collection(stream)
	err := collection.Drop(context.TODO())
	if created {
		return nil, err
	}

	cc = &collectionConn{
		collection: collection,
		records:    []interface{}{},
	}

	m.collectionConnsPerStream[stream] = cc

	return cc, nil
}
