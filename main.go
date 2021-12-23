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

package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/alwitt/httpmq/cmd"
	"github.com/alwitt/httpmq/core"
	"github.com/apex/log"
	apexJSON "github.com/apex/log/handlers/json"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/urfave/cli/v2"
)

type natsArgs struct {
	ServerURI           string `validate:"required,uri"`
	ConnectTimeout      time.Duration
	MaxReconnectAttempt int `validate:"gte=-1"`
	ReconnectWait       time.Duration
}

type cliArgs struct {
	JSONLog  bool
	LogLevel string   `validate:"required,oneof=debug info warn error"`
	NATS     natsArgs `validate:"required,dive"`
	Hostname string
	// For various subcommands
	Management cmd.ManagementCLIArgs `validate:"-"`
	Dataplane  cmd.DataplaneCLIArgs  `validate:"-"`
}

var cmdArgs cliArgs

var logTags log.Fields

// @title httpmq
// @version v0.1.1
// @description HTTP/2 based message broker built around NATS JetStream

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
		Version:     "v0.1.1",
		Usage:       "application entrypoint",
		Description: "HTTP/2 based message broker built around NATS JetStream",
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
				Name:        "management",
				Usage:       "Run the httpmq management server",
				Description: "Serves the REST API for managing JetStream streams and consumers",
				Flags:       cmd.GetManagementCLIFlags(&cmdArgs.Management),
				Action:      startManagementServer,
			},
			{
				Name:        "dataplane",
				Usage:       "Run the httpmq data plane server",
				Description: "Serves the REST API for message publish, and subscribing through JetStream",
				Flags:       cmd.GetDataplaneCLIFlags(&cmdArgs.Dataplane),
				Action:      startDataplaneServer,
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
	tmp, err := json.Marshal(&cmdArgs)
	// args don't marshal
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to marshal args")
		return err
	}
	log.Debugf("Starting params %s", tmp)
	return nil
}

// prepareJetStreamClient define the NATS client
func prepareJetStreamClient(ctxtCancel context.CancelFunc) (*core.NatsClient, error) {
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
			log.WithFields(logTags).Error("NATS client closed connection")
			ctxtCancel()
		},
	}
	return core.GetJetStream(natsParam)
}

func defineControlVars() (*sync.WaitGroup, context.Context, context.CancelFunc) {
	runTimeContext, rtCancel := context.WithCancel(context.Background())
	return &sync.WaitGroup{}, runTimeContext, rtCancel
}

// signalRecvSetup helper function for setting up the SIG receive handler
func signalRecvSetup(wg *sync.WaitGroup, ctxtCancel context.CancelFunc) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		cc := make(chan os.Signal, 1)
		// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
		// SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
		signal.Notify(cc, os.Interrupt)
		<-cc
		ctxtCancel()
	}()
}

// ============================================================================
// Management subcommand

// startManagementServer run the management server
func startManagementServer(c *cli.Context) error {
	if err := initialCmdArgsProcessing(); err != nil {
		return err
	}

	wg, runTimeContext, rtCancel := defineControlVars()
	defer wg.Wait()
	defer rtCancel()

	js, err := prepareJetStreamClient(rtCancel)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf(
			"Failed to define NATS client with %s", cmdArgs.NATS.ServerURI,
		)
		return nil
	}

	signalRecvSetup(wg, rtCancel)

	return cmd.RunManagementServer(runTimeContext, cmdArgs.Management, cmdArgs.Hostname, js)
}

// ============================================================================
// Dataplane subcommand

// startDataplaneServer run the dataplane server
func startDataplaneServer(c *cli.Context) error {
	if err := initialCmdArgsProcessing(); err != nil {
		return err
	}

	wg, runTimeContext, rtCancel := defineControlVars()
	defer wg.Wait()
	defer rtCancel()

	js, err := prepareJetStreamClient(rtCancel)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf(
			"Failed to define NATS client with %s", cmdArgs.NATS.ServerURI,
		)
		return nil
	}

	signalRecvSetup(wg, rtCancel)

	return cmd.RunDataplaneServer(
		runTimeContext, cmdArgs.Dataplane, cmdArgs.Hostname, js, wg,
	)
}
