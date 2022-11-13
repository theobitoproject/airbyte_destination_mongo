package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// DestinationMongo is the Airbyte destination connector
// to store data in Mongo databases
type DestinationMongo struct {
	marshaler    Marshaler
	mongoHandler Handler

	docChan                 DocumentChannel
	marshalerWorkersChan    chan (bool)
	mongoHandlerWorkersChan chan (bool)
}

type destinationConfiguration struct {
	Host                     string `json:"host"`
	Port                     string `json:"port"`
	DBName                   string `json:"db_name"`
	User                     string `json:"user"`
	Password                 string `json:"password"`
	EnableBasicNormalization bool   `json:"enable_basic_normalization"`
}

// NewDestinationMongo creates a new instance of DestinationMongo
func NewDestinationMongo(
	marshaler Marshaler,
	mongoHandler Handler,
	docChan DocumentChannel,
	marshalerWorkersChan chan (bool),
	mongoHandlerWorkersChan chan (bool),
) *DestinationMongo {
	return &DestinationMongo{
		marshaler,
		mongoHandler,
		docChan,
		marshalerWorkersChan,
		mongoHandlerWorkersChan,
	}
}

// Spec returns the schema which described how the source connector can be configured
func (d *DestinationMongo) Spec(
	mw messenger.MessageWriter,
	cp messenger.ConfigParser,
) (*protocol.ConnectorSpecification, error) {
	return &protocol.ConnectorSpecification{
		DocumentationURL:      "https://www.mongodb.com/docs/",
		ChangeLogURL:          "https://www.mongodb.com/docs/",
		SupportsNormalization: false,
		SupportsDBT:           false,
		SupportedDestinationSyncModes: []protocol.DestinationSyncMode{
			protocol.DestinationSyncModeOverwrite,
		},
		ConnectionSpecification: protocol.ConnectionSpecification{
			Title:       "Golang - Mongo",
			Description: "This destination writes all data in a Mongo database",
			Type:        "object",
			Required: []protocol.PropertyName{
				"host",
				"port",
				"db_name",
				"user",
				"password",
				"enable_basic_normalization",
			},
			Properties: protocol.Properties{
				Properties: map[protocol.PropertyName]protocol.PropertySpec{
					"host": {
						Title: "Host",
						PropertyType: protocol.PropertyType{
							Type: []protocol.PropType{
								protocol.String,
							},
						},
					},
					"port": {
						Title: "Port",
						PropertyType: protocol.PropertyType{
							Type: []protocol.PropType{
								protocol.String,
							},
						},
					},
					"db_name": {
						Title: "Database name",
						PropertyType: protocol.PropertyType{
							Type: []protocol.PropType{
								protocol.String,
							},
						},
					},
					"user": {
						Title: "User",
						PropertyType: protocol.PropertyType{
							Type: []protocol.PropType{
								protocol.String,
							},
						},
					},
					"password": {
						Title: "Password",
						PropertyType: protocol.PropertyType{
							Type: []protocol.PropType{
								protocol.String,
							},
						},
					},
					"enable_basic_normalization": {
						Title: "Enable normalization",
						PropertyType: protocol.PropertyType{
							Type: []protocol.PropType{
								protocol.Boolean,
							},
						},
					},
				},
			},
		},
	}, nil
}

// Check verifies that, given a configuration, data can be accessed properly
func (d *DestinationMongo) Check(
	mw messenger.MessageWriter,
	cp messenger.ConfigParser,
) error {
	err := mw.WriteLog(protocol.LogLevelInfo, "running check from destination mongo")
	if err != nil {
		return err
	}

	var dc destinationConfiguration
	err = cp.UnmarshalConfigPath(&dc)
	if err != nil {
		return err
	}

	// TODO: Define which context should be used
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, disconnect, err := getMongoClient(ctx, &dc)
	if err != nil {
		return err
	}
	defer func() {
		if err = disconnect(); err != nil {
			mw.WriteLog(
				protocol.LogLevelError,
				"error disconnecting from database"+err.Error(),
			)
		}
	}()

	return client.Ping(ctx, readpref.Primary())
}

// Write takes the data from the record channel
// and stores it in the destination
// Note: all channels except record channel from hub needs to be closed
func (d *DestinationMongo) Write(
	cc *protocol.ConfiguredCatalog,
	mw messenger.MessageWriter,
	cp messenger.ConfigParser,
	hub messenger.ChannelHub,
) {
	err := mw.WriteLog(protocol.LogLevelInfo, "running write from destination mongo")
	if err != nil {
		hub.GetErrorChannel() <- err
	}

	var dc destinationConfiguration
	err = cp.UnmarshalConfigPath(&dc)
	if err != nil {
		hub.GetErrorChannel() <- err
		return
	}

	// TODO: Define which context should be used
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, disconnect, err := getMongoClient(ctx, &dc)
	if err != nil {
		hub.GetErrorChannel() <- err
		return
	}
	defer func() {
		if err = disconnect(); err != nil {
			mw.WriteLog(
				protocol.LogLevelError,
				"error disconnecting from database"+err.Error(),
			)
		}
	}()
	database := client.Database(dc.DBName)

	d.marshaler.AddWorker(hub, cc, dc.EnableBasicNormalization)

	d.mongoHandler.AddWorker(hub, database)

	<-d.marshalerWorkersChan
	close(d.docChan)
	close(d.marshalerWorkersChan)

	<-d.mongoHandlerWorkersChan
	close(d.mongoHandlerWorkersChan)

	close(hub.GetErrorChannel())
}

func getMongoClient(
	ctx context.Context,
	dc *destinationConfiguration,
) (*mongo.Client, func() error, error) {
	uri := fmt.Sprintf(
		"mongodb://%s:%s@%s:%s",
		dc.User,
		dc.Password,
		dc.Host,
		dc.Port,
	)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, nil, err
	}

	disconnect := func() error {
		return client.Disconnect(ctx)
	}

	return client, disconnect, nil
}
