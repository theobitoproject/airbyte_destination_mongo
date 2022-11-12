package mongo

import (
	"context"
	"sync"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"go.mongodb.org/mongo-driver/mongo"
)

// Handler allows interaction with a Mongo database
type Handler interface {
	// AddWorker adds a new thread to add the document to a batch
	// and store the data when necessary
	AddWorker(
		messenger.ChannelHub,
		*mongo.Database,
	)
}

type handler struct {
	documentChannel DocumentChannel

	workersDoneChan    chan bool
	collectionHandlers map[string]*collectionHandler
	mu                 *sync.Mutex
	batchSize          int
}

// NewHandler creates a new instance of Handler
func NewHandler(
	documentChannel DocumentChannel,
	workersDoneChan chan bool,
	batchSize int,
) Handler {
	return &handler{
		documentChannel:    documentChannel,
		workersDoneChan:    workersDoneChan,
		collectionHandlers: map[string]*collectionHandler{},
		mu:                 &sync.Mutex{},
		batchSize:          batchSize,
	}
}

// AddWorker adds a new thread to add the document to a batch
// and store the data when necessary
func (h *handler) AddWorker(
	hub messenger.ChannelHub,
	database *mongo.Database,
) {
	go func() {
		for {
			doc, channelOpen := <-h.documentChannel
			if !channelOpen {
				h.flushAll(hub)
				h.removeWorker()
				return
			}

			h.mu.Lock()

			cc, err := h.getCollectionConnForStream(database, doc.stream)
			if err != nil {
				hub.GetErrorChannel() <- err
				continue
			}

			cc.records = append(cc.records, doc.data)

			if len(cc.records) < h.batchSize {
				h.mu.Unlock()
				continue
			}

			ctx := context.TODO()
			_, err = cc.collection.InsertMany(ctx, cc.records)
			if err != nil {
				hub.GetErrorChannel() <- err
				continue
			}
			cc.records = []interface{}{}

			h.mu.Unlock()
		}
	}()
}

func (h *handler) flushAll(hub messenger.ChannelHub) {
	for _, cc := range h.collectionHandlers {
		if len(cc.records) == 0 {
			continue
		}

		ctx := context.TODO()
		_, err := cc.collection.InsertMany(ctx, cc.records)
		if err != nil {
			hub.GetErrorChannel() <- err
			continue
		}
	}
}

func (h *handler) removeWorker() {
	h.workersDoneChan <- true
}

func (h *handler) getCollectionConnForStream(
	database *mongo.Database,
	stream string,
) (*collectionHandler, error) {
	cc, created := h.collectionHandlers[stream]
	if created {
		return cc, nil
	}

	collection := database.Collection(stream)
	err := collection.Drop(context.TODO())
	if created {
		return nil, err
	}

	cc = newCollectionHandler(collection)

	h.collectionHandlers[stream] = cc

	return cc, nil
}
