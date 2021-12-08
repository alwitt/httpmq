package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/apex/log"
	apexJSON "github.com/apex/log/handlers/json"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/urfave/cli/v2"
	"gitlab.com/project-nan/httpmq/cmd"
	"gitlab.com/project-nan/httpmq/core"
)

type cliArgs struct {
	JSONLog  bool
	LogLevel string                 `validate:"required,oneof=debug info warn error"`
	NATS     core.NATSConnectParams `validate:"required,dive"`
	Hostname string
	// For various subcommands
	Management cmd.ManagementCLIArgs `validate:"-"`
	Dataplane  cmd.DataplaneCLIArgs  `validate:"-"`
}

var cmdArgs cliArgs

var logTags log.Fields

// @title httpmq
// @version v0.1.0
// @description httpmq primary app
// @termsOfService http://swagger.io/terms/

// @host localhost:3000
// @BasePath /
// @query.collection.format multi
func main() {
	hostname, err := os.Hostname()
	if err != nil {
		log.WithError(err).Fatal("Unable to read hostname")
	}
	cmdArgs.Hostname = hostname
	logTags = log.Fields{
		"module":    "main",
		"component": "main",
		"instance":  hostname,
	}

	app := &cli.App{
		Flags: []cli.Flag{
			// LOGGING
			&cli.BoolFlag{
				Name:        "json-log",
				Usage:       "Whether to log in JSON format",
				Aliases:     []string{"j"},
				EnvVars:     []string{"LOG_AS_JSON"},
				Value:       false,
				DefaultText: "false",
				Destination: &cmdArgs.JSONLog,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "log-level",
				Usage:       "Logging level: [debug info warn error]",
				Aliases:     []string{"l"},
				EnvVars:     []string{"LOG_LEVEL"},
				Value:       "warn",
				DefaultText: "warn",
				Destination: &cmdArgs.LogLevel,
				Required:    false,
			},
			// NATs
			&cli.StringFlag{
				Name:        "nats-server-uri",
				Usage:       "NATS server URI",
				Aliases:     []string{"nsu"},
				EnvVars:     []string{"NATS_SERVER_URI"},
				Value:       "nats://127.0.0.1:4222",
				DefaultText: "nats://127.0.0.1:4222",
				Destination: &cmdArgs.NATS.ServerURI,
				Required:    false,
			},
			&cli.DurationFlag{
				Name:        "nats-connect-timeout",
				Usage:       "NATS connection timeout",
				Aliases:     []string{"ncto"},
				EnvVars:     []string{"NATS_CONNECT_TIMEOUT"},
				Value:       time.Second * 15,
				DefaultText: "15s",
				Destination: &cmdArgs.NATS.ConnectTimeout,
				Required:    false,
			},
			&cli.DurationFlag{
				Name:        "nats-reconnect-wait",
				Usage:       "NATS duration between reconnect attempts",
				Aliases:     []string{"nrcw"},
				EnvVars:     []string{"NATS_RECONNECT_WAIT"},
				Value:       time.Second * 15,
				DefaultText: "15s",
				Destination: &cmdArgs.NATS.ReconnectWait,
				Required:    false,
			},
			&cli.IntFlag{
				Name:        "nats-max-reconnect-attempts",
				Usage:       "NATS maximum reconnect attempts",
				Aliases:     []string{"nmra"},
				EnvVars:     []string{"NATS_MAX_RECONNECT_ATTEMPTS"},
				Value:       -1,
				DefaultText: "-1",
				Destination: &cmdArgs.NATS.MaxReconnectAttempt,
				Required:    false,
			},
		},
		// Components
		Commands: []*cli.Command{
			{
				Name:   "management",
				Usage:  "Run the httpmq management server",
				Flags:  cmd.GetManagementCLIFlags(&cmdArgs.Management),
				Action: startManagementServer,
			},
			{
				Name:   "dataplane",
				Usage:  "Run the httpmq dataplane server",
				Flags:  cmd.GetDataplaneCLIFlags(&cmdArgs.Dataplane),
				Action: startDataplaneServer,
			},
		},
	}

	err = app.Run(os.Args)
	if err != nil {
		log.WithError(err).WithFields(logTags).Fatal("Program shutdown")
	}
}

// setupLogging helper function to prepare the app logging
func setupLogging() {
	if cmdArgs.JSONLog {
		log.SetHandler(apexJSON.New(os.Stderr))
	}
	switch cmdArgs.LogLevel {
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
}

// initialCmdArgsProcessing perform initial CMD arg processing
func initialCmdArgsProcessing() error {
	validate := validator.New()
	if err := validate.Struct(&cmdArgs); err != nil {
		log.WithError(err).WithFields(logTags).Error("Invalid CMD args")
		return err
	}
	setupLogging()
	tmp, _ := json.Marshal(&cmdArgs)
	// if err != nil {
	// 	log.WithError(err).WithFields(logTags).Error("Failed to marshal args")
	// 	return err
	// }
	log.Debugf("Starting params %s", tmp)
	return nil
}

// prepareJetStreamClient define the NATS client
func prepareJetStreamClient() (*core.NatsClient, error) {
	natsParam := core.NATSConnectParams{
		ServerURI:           cmdArgs.NATS.ServerURI,
		ConnectTimeout:      cmdArgs.NATS.ConnectTimeout,
		MaxReconnectAttempt: cmdArgs.NATS.MaxReconnectAttempt,
		ReconnectWait:       cmdArgs.NATS.ReconnectWait,
		OnDisconnectCallback: func(_ *nats.Conn, e error) {
			log.WithError(e).WithFields(logTags).Errorf(
				"NATS client disconnected from server %s", cmdArgs.NATS.ServerURI,
			)
		},
		OnReconnectCallback: func(_ *nats.Conn) {
			log.WithFields(logTags).Warnf(
				"NATS client reconnected with server %s", cmdArgs.NATS.ServerURI,
			)
		},
		OnCloseCallback: func(_ *nats.Conn) {
			log.WithFields(logTags).Fatal("NATS client closed connection")
		},
	}
	return core.GetJetStream(natsParam)
}

// ============================================================================
// Management subcommand

// startManagementServer run the management server
func startManagementServer(c *cli.Context) error {
	if err := initialCmdArgsProcessing(); err != nil {
		return err
	}

	js, err := prepareJetStreamClient()
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf(
			"Failed to defin NATS client with %s", cmdArgs.NATS.ServerURI,
		)
		return nil
	}

	return cmd.RunManagementServer(cmdArgs.Management, cmdArgs.Hostname, js)
}

// ============================================================================
// Dataplane subcommand

// startDataplaneServer run the dataplane server
func startDataplaneServer(c *cli.Context) error {
	if err := initialCmdArgsProcessing(); err != nil {
		return err
	}

	js, err := prepareJetStreamClient()
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf(
			"Failed to defin NATS client with %s", cmdArgs.NATS.ServerURI,
		)
		return nil
	}

	return cmd.RunDataplaneServer(cmdArgs.Dataplane, cmdArgs.Hostname, js)
}
