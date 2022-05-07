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
	"time"

	"github.com/alwitt/httpmq/apis"
	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/core"
	"github.com/alwitt/httpmq/management"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/gorilla/mux"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// RunManagementServer run the management server
func RunManagementServer(
	runtimeContext context.Context,
	params *common.ManagementServerConfig,
	instance string,
	natsClient *core.NatsClient,
) error {
	logTags := log.Fields{
		"module":    "cmd",
		"component": "management",
		"instance":  instance,
	}

	validate := validator.New()
	if err := validate.Struct(params); err != nil {
		log.WithError(err).WithFields(logTags).Error("Invalid CMD args")
		return err
	}

	controller, err := management.GetJetStreamController(natsClient, instance)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define JetStream controller")
		return err
	}

	httpHandler, err := apis.GetAPIRestJetStreamManagementHandler(controller, &params.HTTPSetting)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define HTTP handler")
		return err
	}

	// -------------------------------------------------------------------
	// Start the HTTP server

	router := mux.NewRouter()
	mainRouter := apis.RegisterPathPrefix(router, params.Endpoints.PathPrefix, nil)

	// All stream routes
	streamAPIRouter := apis.RegisterPathPrefix(
		mainRouter, "/v1/admin/stream", map[string]http.HandlerFunc{
			"post": httpHandler.CreateStreamHandler(),
			"get":  httpHandler.GetAllStreamsHandler(),
		},
	)

	// Per stream routes
	perStreamAPIRounter := apis.RegisterPathPrefix(
		streamAPIRouter, "/{streamName}", map[string]http.HandlerFunc{
			"get":    httpHandler.GetStreamHandler(),
			"delete": httpHandler.DeleteStreamHandler(),
		},
	)
	_ = apis.RegisterPathPrefix(perStreamAPIRounter, "/subject", map[string]http.HandlerFunc{
		"put": httpHandler.ChangeStreamSubjectsHandler(),
	})
	_ = apis.RegisterPathPrefix(perStreamAPIRounter, "/limit", map[string]http.HandlerFunc{
		"put": httpHandler.UpdateStreamLimitsHandler(),
	})

	// All consumer routes
	consumerAPIRouter := apis.RegisterPathPrefix(
		perStreamAPIRounter, "/consumer", map[string]http.HandlerFunc{
			"post": httpHandler.CreateConsumerHandler(),
			"get":  httpHandler.GetAllConsumersHandler(),
		},
	)
	_ = apis.RegisterPathPrefix(consumerAPIRouter, "/{consumerName}", map[string]http.HandlerFunc{
		"get":    httpHandler.GetConsumerHandler(),
		"delete": httpHandler.DeleteConsumerHandler(),
	})

	// Health check
	_ = apis.RegisterPathPrefix(mainRouter, "/v1/admin/alive", map[string]http.HandlerFunc{
		"get": httpHandler.AliveHandler(),
	})
	_ = apis.RegisterPathPrefix(mainRouter, "/v1/admin/ready", map[string]http.HandlerFunc{
		"get": httpHandler.ReadyHandler(),
	})

	// Add debug logging
	// FIXME: switch to middleware

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

	// Start the server
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Error("HTTP Server Failure")
		}
	}()

	log.WithFields(logTags).Infof("Started HTTP server on http://%s", serverListen)

	// ============================================================================

	<-runtimeContext.Done()

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
