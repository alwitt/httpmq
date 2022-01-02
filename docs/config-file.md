# Application Configuration File

The default application configuration (when none is given) is

> **IMPORTANT:** The `nats` section is required regardless of whether `httpmq` is serving the `management` or `dataplane` API groups. When serving the `management` API group, the application will ignore settings in the `dataplane` section, and vice versa.

```yaml
---
# Core NATS configuration
nats:
  # the NATS connection URI
  server_uri: nats://127.0.0.1:4222
  # the max duration for connecting to NATS server in seconds
  connect_timeout_sec: 30
  # reconnect parameters
  reconnect:
    # the max number of reconnect attempts (-1 is unlimited)
    max_attempts: -1
    # the duration between reconnect attempts in seconds
    wait_interval_sec: 15

# Management API Group configuration
management:
  # the API endpoint config parameters for the management API server
  endpoint_config:
    # the end-point path prefix for the management APIs
    path_prefix: /
  # the HTTP API / server parameters for the management API server
  api_server:
    # HTTP server parameters
    server_config:
      # the interface the HTTP server will listen on
      listen_on: 0.0.0.0
      # the port the HTTP server will listen on
      listen_port: 3000
      # the maximum duration for reading the entire request,
      # including the body in seconds. A zero or negative
      # value means there will be no timeout.
      read_timeout_sec: 60
      # the maximum duration before timing out writes of the
      # response in seconds. A zero or negative value means
      # there will be no timeout.
      write_timeout_sec: 60
      # the maximum amount of time to wait for the next request
      # when keep-alives are enabled in seconds. If IdleTimeout
      # is zero, the value of ReadTimeout is used. If both are
      # zero, there is no timeout.
      idle_timeout_sec: 600
    # logging parameters
    logging_config:
      # the message logged marking start of a request
      start_of_request_message: "Request Starting"
      # the message logged marking end of a request
      end_of_request_message: "Request Complete"
      # the list of headers to not include in logging metadata
      do_not_log_headers:
        - "WWW-Authenticate"
        - "Authorization"
        - "Proxy-Authenticate"
        - "Proxy-Authorization"

# Dataplane API Group configuration
dataplane:
  # the API endpoint config parameters for the dataplane API server
  endpoint_config:
    # the end-point path prefix for the dataplane APIs
    path_prefix: /
  # the HTTP API / server parameters for the dataplane API server
  api_server:
    server_config:
      listen_on: 0.0.0.0
      listen_port: 3001
      read_timeout_sec: 60
      write_timeout_sec: 60
      idle_timeout_sec: 600
    logging_config:
      start_of_request_message: "Request Starting"
      end_of_request_message: "Request Complete"
      do_not_log_headers:
        - "WWW-Authenticate"
        - "Authorization"
        - "Proxy-Authenticate"
        - "Proxy-Authorization"
```

## 1. Providing Custom Configuration

The usage message for `httpmq` is

```shell
$ ./httpmq.bin -h
NAME:
   httpmq.bin - application entrypoint

USAGE:
   httpmq.bin [global options] command [command options] [arguments...]

VERSION:
   v0.2.0

DESCRIPTION:
   HTTP/2 based message broker built around NATS JetStream

COMMANDS:
   management  Run the httpmq management server
   dataplane   Run the httpmq dataplane server
   help, h     Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --json-log, -j                 Whether to log in JSON format (default: false) [$LOG_AS_JSON]
   --log-level value, -l value    Logging level: [debug info warn error] (default: warn) [$LOG_LEVEL]
   --config-file value, -c value  Application config file. Use DEFAULT if not specified. [$CONFIG_FILE]
   --help, -h                     show help (default: false)
   --version, -v                  print the version (default: false)
```

The option flag "`--config-file value`" allows the user provide a custom runtime configuration file.

## 2. Selective Configuration Override

A new custom configuration does not need to specify every single parameters seen in the default configuration. Instead, the user can provide only the fields to be replaced. As an example

```yaml
---
nats:
  server_uri: nats://dev-nats:4222

management:
  api_server:
    server_config:
      listen_on: 192.168.12.128
      listen_port: 3000

dataplane:
  api_server:
    server_config:
      listen_on: 192.168.13.128
      listen_port: 3000
```

will replace the fields specified, whereas the rest of the fields will continue to use the default values set in the application.
