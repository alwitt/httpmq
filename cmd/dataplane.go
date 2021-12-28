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
	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/core"
	"github.com/alwitt/httpmq/dataplane"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/gorilla/mux"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// RunDataplaneServer run the dataplane server
func RunDataplaneServer(
	runTimeContext context.Context,
	params *common.DataplaneServerConfig,
	instance string,
	natsClient *core.NatsClient,
	wg *sync.WaitGroup,
) error {
	logTags := log.Fields{
		"module":    "cmd",
		"component": "dataplane",
		"instance":  instance,
	}

	validate := validator.New()
	if err := validate.Struct(params); err != nil {
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
		localCtxt, natsClient, &params.HTTPSetting, msgPub, ackPub, wg,
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
	_ = apis.RegisterPathPrefix(mainRouter, "/v1/data/alive", map[string]http.HandlerFunc{
		"get": httpHandler.AliveHandler(),
	})
	_ = apis.RegisterPathPrefix(mainRouter, "/v1/data/ready", map[string]http.HandlerFunc{
		"get": httpHandler.ReadyHandler(),
	})

	serverListen := fmt.Sprintf(
		"%s:%d", params.HTTPSetting.Server.ListenOn, params.HTTPSetting.Server.Port,
	)
	httpSrv := &http.Server{
		Addr:         serverListen,
		WriteTimeout: time.Second * time.Duration(params.HTTPSetting.Server.WriteTimeout),
		ReadTimeout:  time.Second * time.Duration(params.HTTPSetting.Server.ReadTimeout),
		IdleTimeout:  time.Second * time.Duration(params.HTTPSetting.Server.IdleTimeout),
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
