package main

import (
	"context"
	"time"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type destinationMongo struct{}

type destinationConfiguration struct {
	URI    string `json:"uri"`
	DBName string `json:"db_name"`
}

func newDestinationMongo() *destinationMongo {
	return &destinationMongo{}
}

// Spec returns the schema which described how the source connector can be configured
func (d *destinationMongo) Spec(
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
			Required:    []protocol.PropertyName{"uri", "db_name"},
			Properties: protocol.Properties{
				Properties: map[protocol.PropertyName]protocol.PropertySpec{
					"uri": {
						Description: "String format to stablish connection with database",
						PropertyType: protocol.PropertyType{
							Type: []protocol.PropType{
								protocol.String,
							},
						},
						Examples: []string{"mongodb://<user>:<password>@<host>:<port>"},
					},
					"db_name": {
						Description: "Name of the database",
						PropertyType: protocol.PropertyType{
							Type: []protocol.PropType{
								protocol.String,
							},
						},
					},
				},
			},
		},
	}, nil
}

// Check verifies that, given a configuration, data can be accessed properly
func (d *destinationMongo) Check(
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dc.URI))
	if err != nil {
		return err
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			mw.WriteLog(protocol.LogLevelError, "error disconnecting from database"+err.Error())
		}
	}()

	return client.Ping(ctx, readpref.Primary())
}

// Write takes the data from the record channel
// and stores it in the destination
// Note: all channels except record channel from hub needs to be closed
func (d *destinationMongo) Write(
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

}
