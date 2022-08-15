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
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/alwitt/httpmq/cmd"
	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/core"
	"github.com/apex/log"
	apexJSON "github.com/apex/log/handlers/json"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
)

type cliArgs struct {
	JSONLog    bool
	LogLevel   string `validate:"required,oneof=debug info warn error"`
	ConfigFile string `validate:"omitempty,file"`
	Hostname   string
}

var cmdArgs cliArgs

var logTags log.Fields

// @title httpmq
// @version v0.4.1-rc.5
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

	common.InstallDefaultConfigValues()

	app := &cli.App{
		Version:     "v0.4.1-rc.5",
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
			// Config file
			&cli.StringFlag{
				Name:        "config-file",
				Usage:       "Application config file. Use DEFAULT if not specified.",
				Aliases:     []string{"c"},
				EnvVars:     []string{"CONFIG_FILE"},
				Value:       "",
				DefaultText: "",
				Destination: &cmdArgs.ConfigFile,
				Required:    false,
			},
		},
		// Components
		Commands: []*cli.Command{
			{
				Name:        "management",
				Usage:       "Run the httpmq management server",
				Description: "Serves the REST API for managing JetStream streams and consumers",
				Action:      startManagementServer,
			},
			{
				Name:        "dataplane",
				Usage:       "Run the httpmq dataplane server",
				Description: "Serves the REST API for message publish, and subscribing through JetStream",
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
func initialCmdArgsProcessing() (*common.SystemConfig, error) {
	validate := validator.New()
	// Validate command line argument
	if err := validate.Struct(&cmdArgs); err != nil {
		log.WithError(err).WithFields(logTags).Error("Invalid CMD args")
		return nil, err
	}
	setupLogging()
	tmp, err := json.MarshalIndent(&cmdArgs, "", "  ")
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to marshal args")
		return nil, err
	}
	log.Debugf("Starting params\n%s", tmp)
	// Parse the config file
	if len(cmdArgs.ConfigFile) > 0 {
		viper.SetConfigFile(cmdArgs.ConfigFile)
		if err := viper.ReadInConfig(); err != nil {
			log.WithError(err).WithFields(logTags).Errorf(
				"Failed to read config file %s", cmdArgs.ConfigFile,
			)
			return nil, err
		}
	}
	var config common.SystemConfig
	if err := viper.Unmarshal(&config); err != nil {
		log.WithError(err).WithFields(logTags).Errorf(
			"Failed to parse config file %s", cmdArgs.ConfigFile,
		)
		return nil, err
	}
	tmp, err = json.MarshalIndent(&config, "", "  ")
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to marshal config files")
		return nil, err
	}
	log.Debugf("Config file\n%s", tmp)
	if err := validate.Struct(&config); err != nil {
		log.WithError(err).WithFields(logTags).Error("Invalid config file content")
		return nil, err
	}
	return &config, nil
}

// prepareJetStreamClient define the NATS client
func prepareJetStreamClient(
	config common.NATSConfig, ctxtCancel context.CancelFunc,
) (*core.NatsClient, error) {
	natsParam := core.NATSConnectParams{
		ServerURI:           config.ServerURI,
		ConnectTimeout:      time.Second * time.Duration(config.ConnectTimeout),
		MaxReconnectAttempt: config.Reconnect.MaxAttempts,
		ReconnectWait:       time.Second * time.Duration(config.Reconnect.WaitInterval),
		OnDisconnectCallback: func(_ *nats.Conn, e error) {
			log.WithError(e).WithFields(logTags).Errorf(
				"NATS client disconnected from server %s", config.ServerURI,
			)
		},
		OnReconnectCallback: func(_ *nats.Conn) {
			log.WithFields(logTags).Warnf(
				"NATS client reconnected with server %s", config.ServerURI,
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
	config, err := initialCmdArgsProcessing()
	if err != nil {
		return err
	}
	if config.Management == nil {
		return fmt.Errorf("management server can't start without its configurations")
	}

	wg, runTimeContext, rtCancel := defineControlVars()
	defer wg.Wait()
	defer rtCancel()

	js, err := prepareJetStreamClient(config.NATS, rtCancel)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf(
			"Failed to define NATS client with %s", config.NATS.ServerURI,
		)
		return nil
	}

	signalRecvSetup(wg, rtCancel)

	return cmd.RunManagementServer(runTimeContext, config.Management, cmdArgs.Hostname, js)
}

// ============================================================================
// Dataplane subcommand

// startDataplaneServer run the dataplane server
func startDataplaneServer(c *cli.Context) error {
	config, err := initialCmdArgsProcessing()
	if err != nil {
		return err
	}
	if config.Dataplane == nil {
		return fmt.Errorf("dataplane server can't start without its configurations")
	}

	wg, runTimeContext, rtCancel := defineControlVars()
	defer wg.Wait()
	defer rtCancel()

	js, err := prepareJetStreamClient(config.NATS, rtCancel)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf(
			"Failed to define NATS client with %s", config.NATS.ServerURI,
		)
		return nil
	}

	signalRecvSetup(wg, rtCancel)

	return cmd.RunDataplaneServer(
		runTimeContext, config.Dataplane, cmdArgs.Hostname, js, wg,
	)
}
