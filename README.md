# HTTP MQ

[HTTP/2](https://http2.github.io/) based [message broker](https://en.wikipedia.org/wiki/Message_broker) built around [NATS JetStream](https://nats.io).

[![License Apache 2][License-Image]][License-Url] [![Go Report Card][ReportCard-Image]][ReportCard-Url] ![CICD workflow](https://github.com/alwitt/httpmq/actions/workflows/cicd.yaml/badge.svg) [![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Falwitt%2Fhttpmq.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Falwitt%2Fhttpmq?ref=badge_shield)

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/alwitt/httpmq
[ReportCard-Image]: https://goreportcard.com/badge/github.com/alwitt/httpmq

# Documentation

The REST API documentation can be found in here: [httpmq-api](https://github.com/alwitt/httpmq-api/blob/main/README.md).

<!-- Documentation generated with [widdershins](https://github.com/Mermade/widdershins)
```shell
$ node widdershins --code --summary=true --search=false /path/to/docs/swagger.yaml -o README.md
``` -->

# Getting Started

Start the local development NATS server with JetStream enabled

```shell
make compose
```

```shell
$ docker logs docker_nats_1
[1] 2021/12/08 18:15:54.793626 [INF] Starting nats-server
[1] 2021/12/08 18:15:54.793669 [INF]   Version:  2.6.2
[1] 2021/12/08 18:15:54.793673 [INF]   Git:      [f7c3ac5]
[1] 2021/12/08 18:15:54.793675 [INF]   Name:     dev-nats
[1] 2021/12/08 18:15:54.793677 [INF]   Node:     EUUGZUxq
[1] 2021/12/08 18:15:54.793678 [INF]   ID:       ND77HNUBFZG5HCF6N7AOSWO2NOZAU23DTQAB2GZ56JCQDMTZ2RH4YR32
[1] 2021/12/08 18:15:54.793681 [INF] Using configuration file: nats-server.conf
[1] 2021/12/08 18:15:54.794314 [INF] Starting JetStream
[1] 2021/12/08 18:15:54.794547 [INF]     _ ___ _____ ___ _____ ___ ___   _   __  __
[1] 2021/12/08 18:15:54.794558 [INF]  _ | | __|_   _/ __|_   _| _ \ __| /_\ |  \/  |
[1] 2021/12/08 18:15:54.794559 [INF] | || | _|  | | \__ \ | | |   / _| / _ \| |\/| |
[1] 2021/12/08 18:15:54.794560 [INF]  \__/|___| |_| |___/ |_| |_|_\___/_/ \_\_|  |_|
[1] 2021/12/08 18:15:54.794561 [INF]
[1] 2021/12/08 18:15:54.794562 [INF]          https://docs.nats.io/jetstream
[1] 2021/12/08 18:15:54.794564 [INF]
[1] 2021/12/08 18:15:54.794565 [INF] ---------------- JETSTREAM ----------------
[1] 2021/12/08 18:15:54.794571 [INF]   Max Memory:      64.00 MB
[1] 2021/12/08 18:15:54.794573 [INF]   Max Storage:     256.00 MB
[1] 2021/12/08 18:15:54.794574 [INF]   Store Directory: "/mnt/nats/jetstream"
[1] 2021/12/08 18:15:54.794575 [INF] -------------------------------------------
[1] 2021/12/08 18:15:54.795736 [INF] Starting http monitor on 0.0.0.0:8222
[1] 2021/12/08 18:15:54.795840 [INF] Listening for client connections on 0.0.0.0:4222
[1] 2021/12/08 18:15:54.796280 [INF] Server is ready
```

Available Makefile targets are

```shell
$ make help
lint                           Lint the files
compose                        Run docker-compose to create the DEV ENV
doc                            Generate the OpenAPI spec
mock                           Generate test mock interfaces
test                           Run unittests
build                          Build project binaries
clean                          Clean up DEV ENV
help                           Display this help screen
```

Verify the project builds, and passes unit-tests

```shell
$ make
$ make test
```

By default, the server application is `httpmq.bin`.

```shell
$ ./httpmq.bin --help
NAME:
   httpmq.bin - application entrypoint

USAGE:
   httpmq.bin [global options] command [command options] [arguments...]

VERSION:
   v0.1.3

DESCRIPTION:
   HTTP/2 based message broker built around NATS JetStream

COMMANDS:
   management  Run the httpmq management server
   dataplane   Run the httpmq data plane server
   help, h     Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --json-log, -j                                     Whether to log in JSON format (default: false) [$LOG_AS_JSON]
   --log-level value, -l value                        Logging level: [debug info warn error] (default: warn) [$LOG_LEVEL]
   --http-idle-timeout value, -t value                HTTP connection idle timeout (default: 1h) [$HTTP_CONNECTION_IDLE_TIMEOUT]
   --nats-server-uri value, --nsu value               NATS server URI (default: nats://127.0.0.1:4222) [$NATS_SERVER_URI]
   --nats-connect-timeout value, --ncto value         NATS connection timeout (default: 15s) [$NATS_CONNECT_TIMEOUT]
   --nats-reconnect-wait value, --nrcw value          NATS duration between reconnect attempts (default: 15s) [$NATS_RECONNECT_WAIT]
   --nats-max-reconnect-attempts value, --nmra value  NATS maximum reconnect attempts (default: -1) [$NATS_MAX_RECONNECT_ATTEMPTS]
   --help, -h                                         show help (default: false)
   --version, -v                                      print the version (default: false)
```

```shell
$ ./httpmq.bin management -h
NAME:
   httpmq.bin management - Run the httpmq management server

USAGE:
   httpmq.bin management [command options] [arguments...]

DESCRIPTION:
   Serves the REST API for managing JetStream streams and consumers

OPTIONS:
   --management-server-port value, --msp value              Management server port (default: 3000) [$MANAGEMENT_SERVER_PORT]
   --management-server-endpoint-prefix value, --msep value  Set the end-point path prefix for the management APIs (default: /) [$MANAGEMENT_SERVER_ENDPOINT_PREFIX]
   --help, -h                                               show help (default: false)
```

```shell
$ ./httpmq.bin dataplane -h
NAME:
   httpmq.bin dataplane - Run the httpmq data plane server

USAGE:
   httpmq.bin dataplane [command options] [arguments...]

DESCRIPTION:
   Serves the REST API for message publish, and subscribing through JetStream

OPTIONS:
   --dataplane-server-port value, --dsp value              Dataplane server port (default: 3001) [$DATAPLANE_SERVER_PORT]
   --dataplane-server-endpoint-prefix value, --dsep value  Set the end-point path prefix for the dataplane APIs (default: /) [$DATAPLANE_SERVER_ENDPOINT_PREFIX]
   --help, -h                                              show help (default: false)
```

---
## Start Local Test Servers

To start the management server locally

```shell
./httpmq.bin -l debug --nmra 1 management
```

```shell
$ ./httpmq.bin -l debug --nmra 1 management
2021/12/08 10:22:51  info Created JetStream client  component=jetstream-backend instance=nats://127.0.0.1:4222 module=core
2021/12/08 10:22:51  info Started HTTP server on http://:3000 component=management instance=dvm-personal module=cmd
```

To start the dataplane server locally

```shell
./httpmq.bin -l debug --nmra 1 dataplane
```

```shell
$ ./httpmq.bin -l debug --nmra 1 dataplane
2021/12/08 10:24:31  info Created JetStream client  component=jetstream-backend instance=nats://127.0.0.1:4222 module=core
2021/12/08 10:24:31  info Started HTTP server on http://:3001 component=management instance=dvm-personal module=cmd
```

---
## Define Elements For Testing

Start by defining a JetStream stream

```shell
curl -X POST 'http://127.0.0.1:3000/v1/admin/stream' \
--header 'Content-Type: application/json' \
--data-raw '{
    "name": "testStream00",
    "max_age": 300000000000,
    "subjects": [
        "test-subject.00",
        "test-subject.01"
    ]
}'
```

Response should be `{"success":true}`.

Verify the stream is defined

```shell
curl 'http://127.0.0.1:3000/v1/admin/stream/testStream00'
```

```json
{
  "success": true,
  "stream": {
    "config": {
      "name": "testStream00",
      "subjects": [
        "test-subject.00",
        "test-subject.01"
      ],
      "max_consumers": -1,
      "max_msgs": -1,
      "max_bytes": -1,
      "max_age": 300000000000,
      "max_msgs_per_subject": -1,
      "max_msg_size": -1
    },
    "created": "2021-12-27T18:26:48.419816409Z",
    "state": {
      "messages": 0,
      "bytes": 0,
      "first_seq": 0,
      "first_ts": "0001-01-01T00:00:00Z",
      "last_seq": 0,
      "last_ts": "0001-01-01T00:00:00Z",
      "consumer_count": 0
    }
  }
}
```

Define a consumer for the stream

```shell
curl -X POST 'http://127.0.0.1:3000/v1/admin/stream/testStream00/consumer' \
--header 'Content-Type: application/json' \
--data-raw '{
    "max_inflight": 4,
    "mode": "push",
    "name": "testConsumer00",
    "filter_subject": "test-subject.01"
}'
```

Response should be `{"success":true}`.

Verify the consumer is defined

```shell
curl 'http://127.0.0.1:3000/v1/admin/stream/testStream00/consumer/testConsumer00'
```

```json
{
  "success": true,
  "consumer": {
    "stream_name": "testStream00",
    "name": "testConsumer00",
    "created": "2021-12-27T18:27:58.055568398Z",
    "config": {
      "deliver_subject": "_INBOX.NZbAf8BCfeTA5s4Yxwnxnh",
      "max_deliver": -1,
      "ack_wait": 30000000000,
      "filter_subject": "test-subject.01",
      "max_ack_pending": 4
    },
    "delivered": {
      "consumer_seq": 0,
      "stream_seq": 0
    },
    "ack_floor": {
      "consumer_seq": 0,
      "stream_seq": 0
    },
    "num_ack_pending": 0,
    "num_redelivered": 0,
    "num_waiting": 0,
    "num_pending": 0
  }
}
```

---
## Publishing Messages

To publish a message for a subject

```shell
curl -X POST 'http://127.0.0.1:3001/v1/data/subject/test-subject.01' --header 'Content-Type: text/plain' --data-raw "$(echo 'Hello World' | base64)"
```

> **IMPORTANT:** The message body must be Base64 encoded.
>
> ```shell
> $ echo "Hello World" | base64
> SGVsbG8gV29ybGQK
> ```

---
## Subscribing For Messages

To subscribe to messages for a consumer on a stream

```shell
curl http://127.0.0.1:3001/v1/data/stream/testStream00/consumer/testConsumer00?subject_name=test-subject.01 --http2-prior-knowledge
```

```shell
$ curl http://127.0.0.1:3001/v1/data/stream/testStream00/consumer/testConsumer00?subject_name=test-subject.01 --http2-prior-knowledge
{"success":true,"stream":"testStream00","subject":"test-subject.01","consumer":"testConsumer00","sequence":{"stream":1,"consumer":1},"b64_msg":"SGVsbG8gV29ybGQK"}
```

After receiving a message, acknowledge receiving the message with

```shell
curl -X POST 'http://127.0.0.1:3001/v1/data/stream/testStream00/consumer/testConsumer00/ack' --header 'Content-Type: application/json' --data-raw '{"consumer": 1,"stream": 1}'
```

The `consumer` and `stream` fields are the sequence numbers which came with the message.

If an acknowledgement is not sent within the consumer's configured max ACK wait duration, the message will be sent through this consumer's subscription again. This time, the `stream` sequence number is unchanged, but the `consumer` sequence number is increased by one.

```shell
{"success":true,"stream":"testStream00","subject":"test-subject.01","consumer":"testConsumer00","sequence":{"stream":1,"consumer":2},"b64_msg":"SGVsbG8gV29ybGQK"}
```

When acknowledging this message now, use `'{"consumer": 2,"stream": 1}'` as the payload.

# License

Unless otherwise noted, the httpmq source files are distributed under the Apache Version 2.0 license found in the LICENSE file.

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Falwitt%2Fhttpmq.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Falwitt%2Fhttpmq?ref=badge_large)
