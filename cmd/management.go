package cmd

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/urfave/cli/v2"
	"gitlab.com/project-nan/httpmq/apis"
	"gitlab.com/project-nan/httpmq/core"
	"gitlab.com/project-nan/httpmq/management"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// ManagementRestEndpoints end-point path configs for management control API
type ManagementRestEndpoints struct {
	PathPrefix string
}

// ManagementCLIArgs arguments
type ManagementCLIArgs struct {
	ServerPort int `validate:"required,gt=0,lt=65536"`
	Endpoints  ManagementRestEndpoints
}

// GetManagementCLIFlags retreive the set of CMD flags for management server
func GetManagementCLIFlags(args *ManagementCLIArgs) []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{
			Name:        "management-server-port",
			Usage:       "Management server port",
			Aliases:     []string{"msp"},
			EnvVars:     []string{"MANAGEMENT_SERVER_PORT"},
			Value:       3000,
			DefaultText: "3000",
			Destination: &args.ServerPort,
			Required:    false,
		},
		// End-point related
		&cli.StringFlag{
			Name:        "management-server-endpoint-prefix",
			Usage:       "Set the end-point path prefix for the management APIs",
			Aliases:     []string{"msep"},
			EnvVars:     []string{"MANAGEMENT_SERVER_ENDPOINT_PREFIX"},
			Value:       "/",
			DefaultText: "/",
			Destination: &args.Endpoints.PathPrefix,
			Required:    false,
		},
	}
}

// RunManagementServer run the management server
func RunManagementServer(
	params ManagementCLIArgs,
	instance string,
	natsClient *core.NatsClient,
	runtimeContext context.Context,
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

	controller, err := management.GetJetStreamController(natsClient, instance)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("Unable to define JetStream controller")
		return err
	}

	httpHandler, err := apis.GetAPIRestJetStreamManagementHandler(controller)
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
		ReadTimeout:  time.Second * 60,
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
