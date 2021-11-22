package core

import (
	"context"
	"time"

	"github.com/apex/log"
	"github.com/nats-io/nats.go"
	"gitlab.com/project-nan/httpmq/common"
)

// NATSConnectParams NATS connection parameter
type NATSConnectParams struct {
	// ServerURI connect to NATS JetStream cluster with URI
	ServerURI string `validate:"required,uri"`
	// ConnectTimeout max time to wait for connection
	ConnectTimeout time.Duration
	// MaxReconnectAttempt on connection failure, max number of reconnect
	// attempt. "-1" means infinite
	MaxReconnectAttempt int
	// ReconnectWait wait duration between reconnect attempts
	ReconnectWait time.Duration
	// OnDisconnectCallback callback on disconnect
	OnDisconnectCallback func(*nats.Conn, error)
	// OnReconnectCallback callback on reconnect
	OnReconnectCallback func(*nats.Conn)
	// OnCloseCallback callback on close
	OnCloseCallback func(*nats.Conn)
}

// NatsClient NATS NatsClient as message broker core
type NatsClient struct {
	common.Component
	nc *nats.Conn
	js nats.JetStreamContext
}

// Close close a JetStream client
func (js NatsClient) Close(ctxt context.Context) {
	if err := js.nc.FlushWithContext(ctxt); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("NATS flush failed")
	}
	js.nc.Close()
	log.WithFields(js.LogTags).Infof("Close NATS client")
}

// JetStream fetch the JetStream client
func (js NatsClient) JetStream() nats.JetStreamContext {
	return js.js
}

// GetJetStream define a new NATS JetStream core
func GetJetStream(param NATSConnectParams) (NatsClient, error) {
	logTags := log.Fields{
		"module":    "core",
		"component": "jetstream-backend",
		"instance":  param.ServerURI,
	}
	// Create the NATS transport
	nc, err := nats.Connect(
		param.ServerURI,
		nats.Timeout(param.ConnectTimeout),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(param.MaxReconnectAttempt),
		nats.ReconnectWait(param.ReconnectWait),
		nats.DisconnectErrHandler(param.OnDisconnectCallback),
		nats.ReconnectHandler(param.OnReconnectCallback),
		nats.ClosedHandler(param.OnCloseCallback),
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Errorf("NATS client connect failed")
		return NatsClient{}, err
	}

	// Define the JetStream client
	js, err := nc.JetStream()
	if err != nil {
		log.WithError(err).WithFields(logTags).Error(
			"Failed to define JetStream client",
		)
	} else {
		log.WithFields(logTags).Info("Created JetStream client")
	}

	return NatsClient{
		Component: common.Component{LogTags: logTags},
		nc:        nc,
		js:        js,
	}, err
}
