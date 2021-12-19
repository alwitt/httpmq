// Copyright 2021-2022 The httpmq Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/alwitt/httpmq/apis"
	"github.com/alwitt/httpmq/core"
	"github.com/alwitt/httpmq/dataplane"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/urfave/cli/v2"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// DataplaneRestEndpoints end-point path configs for dataplane API
type DataplaneRestEndpoints struct {
	PathPrefix string
}

// DataplaneCLIArgs arguments
type DataplaneCLIArgs struct {
	ServerPort int `validate:"required,gt=0,lt=65536"`
	Endpoints  DataplaneRestEndpoints
}

// GetDataplaneCLIFlags retrieve the set of CMD flags for dataplane server
func GetDataplaneCLIFlags(args *DataplaneCLIArgs) []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{
			Name:        "dataplane-server-port",
			Usage:       "Dataplane server port",
			Aliases:     []string{"dsp"},
			EnvVars:     []string{"DATAPLANE_SERVER_PORT"},
			Value:       3001,
			DefaultText: "3001",
			Destination: &args.ServerPort,
			Required:    false,
		},
		// End-point related
		&cli.StringFlag{
			Name:        "dataplane-server-endpoint-prefix",
			Usage:       "Set the end-point path prefix for the dataplane APIs",
			Aliases:     []string{"dsep"},
			EnvVars:     []string{"DATAPLANE_SERVER_ENDPOINT_PREFIX"},
			Value:       "/",
			DefaultText: "/",
			Destination: &args.Endpoints.PathPrefix,
			Required:    false,
		},
	}
}

// RunDataplaneServer run the dataplane server
func RunDataplaneServer(
	params DataplaneCLIArgs,
	instance string,
	natsClient *core.NatsClient,
	runTimeContext context.Context,
	wg *sync.WaitGroup,
) error {
	logTags := log.Fields{
		"module":    "cmd",
		"component": "management",
		"instance":  instance,
	}

	validate := validator.New()
	if err := validate.Struct(&params); err != nil {
		log.WithError(err).WithFields(logTags).Error("Invalid CMD args")
		return err
	}

	msgPub, err := dataplane.GetJetStreamPublisher(natsClient, instance)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define message publisher")
		return err
	}

	ackPub, err := dataplane.GetJetStreamACKBroadcaster(natsClient, instance)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define ACK broadcaster")
		return err
	}

	localCtxt, lclCancel := context.WithCancel(runTimeContext)
	defer lclCancel()
	httpHandler, err := apis.GetAPIRestJetStreamDataplaneHandler(
		natsClient, msgPub, ackPub, localCtxt, wg,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define HTTP handler")
		return err
	}

	// -------------------------------------------------------------------
	// Start the HTTP server

	router := mux.NewRouter()
	mainRouter := apis.RegisterPathPrefix(router, params.Endpoints.PathPrefix, nil)

	// Message publish
	_ = apis.RegisterPathPrefix(
		mainRouter, "/v1/data/subject/{subjectName}", map[string]http.HandlerFunc{
			"post": httpHandler.PublishMessageHandler(),
		},
	)

	// Subscription
	subscribeAPIRouter := apis.RegisterPathPrefix(
		mainRouter,
		"/v1/data/stream/{streamName}/consumer/{consumerName}",
		map[string]http.HandlerFunc{
			"get": httpHandler.PushSubscribeHandler(),
		},
	)
	_ = apis.RegisterPathPrefix(
		subscribeAPIRouter, "/ack", map[string]http.HandlerFunc{
			"post": httpHandler.ReceiveMsgACKHandler(),
		},
	)

	// Health check
	_ = apis.RegisterPathPrefix(mainRouter, "/alive", map[string]http.HandlerFunc{
		"get": httpHandler.AliveHandler(),
	})
	_ = apis.RegisterPathPrefix(mainRouter, "/ready", map[string]http.HandlerFunc{
		"get": httpHandler.ReadyHandler(),
	})

	// Add logging
	router.Use(func(next http.Handler) http.Handler {
		return handlers.CombinedLoggingHandler(httpHandler, next)
	})

	serverListen := fmt.Sprintf(":%d", params.ServerPort)
	httpSrv := &http.Server{
		Addr:         serverListen,
		WriteTimeout: time.Second * 60,
		Handler:      h2c.NewHandler(router, &http2.Server{}),
	}

	// Cancel runtime context on shutdown
	httpSrv.RegisterOnShutdown(lclCancel)

	// Start the server
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Error("HTTP Server Failure")
		}
	}()

	log.WithFields(logTags).Infof("Started HTTP server on http://%s", serverListen)

	// ============================================================================

	<-runTimeContext.Done()

	// Stop the HTTP server
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := httpSrv.Shutdown(ctx); err != nil {
			log.WithError(err).Error("Failure during HTTP shutdown")
		}
	}

	return nil
}
