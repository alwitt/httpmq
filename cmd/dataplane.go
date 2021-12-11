package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/urfave/cli/v2"
	"gitlab.com/project-nan/httpmq/apis"
	"gitlab.com/project-nan/httpmq/core"
	"gitlab.com/project-nan/httpmq/dataplane"
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

// GetDataplaneCLIFlags retreive the set of CMD flags for dataplane server
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
	params DataplaneCLIArgs, instance string, natsClient *core.NatsClient,
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

	httpHandler, err := apis.GetAPIRestJetStreamDataplaneHandler(natsClient, msgPub, ackPub)
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

	// Start the server
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil {
			log.WithError(err).Error("HTTP Server Failure")
		}
	}()

	log.WithFields(logTags).Infof("Started HTTP server on http://%s", serverListen)

	// ============================================================================

	cc := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	signal.Notify(cc, os.Interrupt)

	<-cc

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