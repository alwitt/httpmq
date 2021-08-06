package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/apex/log"
	apexJSON "github.com/apex/log/handlers/json"
	"github.com/go-playground/validator"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
	"gitlab.com/project-nan/httpmq/storage"
)

type cmdArgs struct {
	JSONLog    bool
	LogLevel   string `validate:"required,oneof=debug info warn error"`
	EtcdHost   string `validate:"required"`
	LockName   string `validate:"required"`
	Threads    int
	Iterations int
}

var args cmdArgs

func main() {
	mutexName := fmt.Sprintf("mutex-%s", uuid.New().String())

	app := &cli.App{
		Flags: []cli.Flag{
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
			&cli.StringFlag{
				Name:        "etcd-host",
				Usage:       "ETCD server host name",
				EnvVars:     []string{"ETCD_HOST"},
				Aliases:     []string{"s"},
				Value:       "localhost:2379",
				DefaultText: "localhost:2379",
				Destination: &args.EtcdHost,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "mutex-name",
				Usage:       "Target mutex name",
				EnvVars:     []string{"MUTEX_NAME"},
				Aliases:     []string{"m"},
				Value:       mutexName,
				DefaultText: mutexName,
				Destination: &args.LockName,
				Required:    false,
			},
			&cli.IntFlag{
				Name:        "threads",
				Usage:       "Number of test threads",
				EnvVars:     []string{"TEST_THREADS"},
				Aliases:     []string{"t"},
				Value:       2,
				DefaultText: "2",
				Destination: &args.Threads,
				Required:    false,
			},
			&cli.IntFlag{
				Name:        "iterations",
				Usage:       "Number of lock/unlock cycles",
				EnvVars:     []string{"TEST_ITERATIONS"},
				Aliases:     []string{"c"},
				Value:       10,
				DefaultText: "10",
				Destination: &args.Iterations,
				Required:    false,
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

	// Define the test connections
	connections := make([]storage.Driver, args.Threads)
	for itr := 0; itr < args.Threads; itr++ {
		driver, err := storage.CreateEtcdDriver([]string{args.EtcdHost}, time.Second)
		if err != nil {
			log.WithError(err).Errorf("Failed to create ETCD driver for %s", args.EtcdHost)
			return err
		}
		connections[itr] = driver
	}

	// Start the tests
	testDurations := make([]time.Duration, args.Threads)
	wg := sync.WaitGroup{}
	testFunction := func(index int) {
		startTime := time.Now()
		for itr := 0; itr < args.Iterations; itr++ {
			if err := connections[index].Lock(args.LockName, time.Second*10); err != nil {
				log.WithError(err).Errorf("Lock %s failed", args.LockName)
			}
			if err := connections[index].Unlock(args.LockName, time.Second*10); err != nil {
				log.WithError(err).Errorf("Unlock %s failed", args.LockName)
			}
		}
		endTime := time.Now()
		testDurations[index] = endTime.Sub(startTime)
		wg.Done()
	}
	wg.Add(args.Threads)
	for itr := 0; itr < args.Threads; itr++ {
		go testFunction(itr)
	}
	// Wait for all test threads to exit
	wg.Wait()

	// Get average lock / unlock time
	avgCycle := time.Second * 0
	for _, totalTime := range testDurations {
		avgCycle += totalTime / time.Duration(args.Iterations)
	}
	avgCycleMs := float64(avgCycle) / float64(time.Millisecond) / float64(args.Threads)
	log.Infof("AVG Lock / Unlock Cycle: %.03f ms", avgCycleMs)

	for _, connection := range connections {
		if err := connection.Close(); err != nil {
			log.WithError(err).Errorf("Failed to close ETCD driver for %s", args.EtcdHost)
		}
	}
	return nil
}
