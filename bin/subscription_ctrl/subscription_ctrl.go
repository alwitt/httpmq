package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/apex/log"
	apexJSON "github.com/apex/log/handlers/json"
	"github.com/go-playground/validator"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/urfave/cli/v2"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/rest"
	"gitlab.com/project-nan/httpmq/storage"
	"gitlab.com/project-nan/httpmq/subscription"
)

type endPointArgs struct {
	BaseURL string `json:"base_url"`
	Alive   string `json:"alive" validate:"required"`
	Ready   string `json:"ready" validate:"required"`
}

type etcdArgs struct {
	Host      string `json:"host" validate:"required"`
	OpTimeout time.Duration
}

type cmdArgs struct {
	ServerPort    int `validate:"required"`
	JSONLog       bool
	LogLevel      string       `validate:"required,oneof=debug info warn error"`
	Endpoints     endPointArgs `json:"end_points" validate:"required,dive"`
	Etcd          etcdArgs     `json:"etcd" validate:"required,dive"`
	KVStorePrefix string       `json:"kv_store_prefix" validate:"required"`
}

var args cmdArgs

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:        "port",
				Usage:       "Set the server listen port",
				Aliases:     []string{"p"},
				EnvVars:     []string{"SERVER_PORT"},
				Value:       3000,
				DefaultText: "3000",
				Destination: &args.ServerPort,
				Required:    false,
			},
			// LOGGING
			&cli.BoolFlag{
				Name:        "json-log",
				Usage:       "Whether to log in JSON format",
				Aliases:     []string{"j"},
				EnvVars:     []string{"LOG_AS_JSON"},
				Value:       false,
				DefaultText: "false",
				Destination: &args.JSONLog,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "log-level",
				Usage:       "Logging level: [debug info warn error]",
				Aliases:     []string{"l"},
				EnvVars:     []string{"LOG_LEVEL"},
				Value:       "warn",
				DefaultText: "warn",
				Destination: &args.LogLevel,
				Required:    false,
			},
			// End-points
			&cli.StringFlag{
				Name:        "ep-base-url",
				Usage:       "Base end-point for the various end-points",
				Aliases:     []string{"epbu"},
				EnvVars:     []string{"EP_BASE_URL"},
				Value:       "",
				DefaultText: "",
				Destination: &args.Endpoints.BaseURL,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "ep-alive",
				Usage:       "End-point for liveness check",
				Aliases:     []string{"ephl"},
				EnvVars:     []string{"EP_ALIVE"},
				Value:       "/alive",
				DefaultText: "/alive",
				Destination: &args.Endpoints.Alive,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "ep-ready",
				Usage:       "End-point for readiness check",
				Aliases:     []string{"ephr"},
				EnvVars:     []string{"EP_READY"},
				Value:       "/ready",
				DefaultText: "/ready",
				Destination: &args.Endpoints.Ready,
				Required:    false,
			},
			// ETCD
			&cli.StringFlag{
				Name:        "etcd-host",
				Usage:       "ETCD server host name",
				EnvVars:     []string{"ETCD_HOST"},
				Aliases:     []string{"eh"},
				Value:       "localhost:2379",
				DefaultText: "localhost:2379",
				Destination: &args.Etcd.Host,
				Required:    false,
			},
			&cli.DurationFlag{
				Name:        "etcd-op-timeout",
				Usage:       "ETCD OPS timeout",
				EnvVars:     []string{"ETCD_OPS_TIMEOUT"},
				Aliases:     []string{"eot"},
				Value:       time.Second * 5,
				DefaultText: "5s",
				Destination: &args.Etcd.OpTimeout,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "kv-store-prefix",
				Usage:       "K/V store prefix",
				EnvVars:     []string{"KEY_VAL_STORE_PREFIX"},
				Aliases:     []string{"kysp"},
				Destination: &args.KVStorePrefix,
				Required:    true,
			},
		},
		Action: startServer,
	}

	err := app.Run(os.Args)
	if err != nil {
		log.WithError(err).Fatal("Program shutdown")
	}
}

func startServer(c *cli.Context) error {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	opContext, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Double check the input
	{
		validate := validator.New()
		if err := validate.Struct(&args); err != nil {
			return err
		}
	}

	// Prepare the logging
	if args.JSONLog {
		log.SetHandler(apexJSON.New(os.Stderr))
	}
	switch args.LogLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.ErrorLevel)
	}

	{
		tmp, _ := json.Marshal(&args)
		log.Debugf("Starting params %s", tmp)
	}

	// Create the KV store interface
	_, kvStore, err := storage.CreateEtcdBackedStorage(
		[]string{args.Etcd.Host}, args.Etcd.OpTimeout,
	)
	if err != nil {
		log.WithError(err).Errorf("Failed to define KV store interface")
		return err
	}

	// Create task processor
	tp, err := common.GetNewTaskProcessorInstance("subscription-manager", 8, opContext)
	if err != nil {
		log.WithError(err).Errorf("Failed to define task-processor")
		return err
	}

	// ------------------------------------------------------------------------
	// Create core controller
	controller, err := subscription.DefineSubscriptionRecorder(
		kvStore, tp, args.KVStorePrefix, args.Etcd.OpTimeout,
	)
	if err != nil {
		log.WithError(err).Errorf("Failed to define core-controller")
		return err
	}

	// Start controller
	if err := tp.StartEventLoop(&wg); err != nil {
		log.WithError(err).Errorf("Failed to start the core controller task-processor")
		return err
	}

	// ------------------------------------------------------------------------
	// Create HTTP server

	restHandler := subscription.DefineSubscriptionRESTController(controller)
	r := mux.NewRouter()

	_ = rest.RegisterPathPrefix(r, fmt.Sprintf("%s/node/{nodeID}/client/{clientID}", args.Endpoints.BaseURL), map[string]http.HandlerFunc{
		"post":   restHandler.NewClientSessionHandler(),
		"put":    restHandler.RenewClientSessionHandler(),
		"delete": restHandler.DeleteClientSessionHandler(),
	})

	_ = rest.RegisterPathPrefix(r, args.Endpoints.Alive, map[string]http.HandlerFunc{
		"get": restHandler.AliveHandler(),
	})

	_ = rest.RegisterPathPrefix(r, args.Endpoints.Ready, map[string]http.HandlerFunc{
		"get": restHandler.ReadyHandler(),
	})

	// Add logging
	var lw restLogWrapper
	r.Use(func(next http.Handler) http.Handler {
		return handlers.CombinedLoggingHandler(lw, next)
	})

	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", args.ServerPort),
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      r,
	}

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil {
			log.WithError(err).Error("HTTP Server Failure")
		}
	}()

	// ------------------------------------------------------------------------

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

type restLogWrapper string

func (r restLogWrapper) Write(p []byte) (n int, err error) {
	log.Warnf("%s", p)
	return len(p), nil
}
