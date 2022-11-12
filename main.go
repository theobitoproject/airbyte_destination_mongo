package main

import (
	"log"
	"os"

	"github.com/theobitoproject/airbyte_destination_mongo/pkg/mongo"
	"github.com/theobitoproject/kankuro/pkg/destination"
)

func main() {
	docChan := mongo.NewDocumentChannel()
	marshalerWorkersChan := make(chan bool)
	mongoHandlerWorkersChan := make(chan bool)

	marshaler := mongo.NewRawMarshaler(
		docChan,
		marshalerWorkersChan,
	)

	mongoHandler := mongo.NewHandler(
		docChan,
		mongoHandlerWorkersChan,
		1000,
	)

	dst := mongo.NewDestinationMongo(
		marshaler,
		mongoHandler,
		docChan,
		marshalerWorkersChan,
		mongoHandlerWorkersChan,
	)
	runner := destination.NewSafeDestinationRunner(dst, os.Stdout, os.Stdin, os.Args)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
