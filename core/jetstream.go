package core

import (
	"context"
	"time"

	"github.com/alwitt/httpmq/common"
	"github.com/apex/log"
	"github.com/nats-io/nats.go"
)

// NATSConnectParams contains NATS connection parameters
type NATSConnectParams struct {
	// connect to NATS JetStream cluster with this URI
	ServerURI string `validate:"required,uri"`
	// max time to wait for connection in ns
	ConnectTimeout time.Duration
	// on connection failure, max number of reconnect attempt. "-1" means infinite
	MaxReconnectAttempt int `validate:"gte=-1"`
	// wait duration between reconnect attempts
	ReconnectWait time.Duration
	// callback on client disconnect
	OnDisconnectCallback func(*nats.Conn, error)
	// callback on client reconnect
	OnReconnectCallback func(*nats.Conn)
	// callback on client connection closed
	OnCloseCallback func(*nats.Conn)
}

// NatsClient is a wrapper around NATS client handle objects
type NatsClient struct {
	common.Component
	nc *nats.Conn
	js nats.JetStreamContext
}

// Close closes a JetStream client
func (js *NatsClient) Close(ctxt context.Context) {
	if err := js.nc.FlushWithContext(ctxt); err != nil {
		log.WithError(err).WithFields(js.LogTags).Errorf("NATS flush failed")
	}
	js.nc.Close()
	log.WithFields(js.LogTags).Infof("Close NATS client")
}

// NATs fetches the NATs client handle
func (js *NatsClient) NATs() *nats.Conn {
	return js.nc
}

// JetStream fetches the JetStream client handle
func (js *NatsClient) JetStream() nats.JetStreamContext {
	return js.js
}

// GetJetStream defines a new NATS client object wrapper
//
// NOTE: Function will also attempt to connect with NATs server
func GetJetStream(param NATSConnectParams) (*NatsClient, error) {
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
		return &NatsClient{}, err
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

	return &NatsClient{
		Component: common.Component{LogTags: logTags},
		nc:        nc,
		js:        js,
	}, err
}
