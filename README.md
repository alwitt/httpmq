# HTTP MQ

[HTTP/2](https://http2.github.io/) based [message broker](https://en.wikipedia.org/wiki/Message_broker) built around [NATS JetStream](https://nats.io).

[![License Apache 2][License-Image]][License-Url] [![Go Report Card][ReportCard-Image]][ReportCard-Url] ![CICD workflow](https://github.com/alwitt/httpmq/actions/workflows/cicd.yaml/badge.svg)

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/alwitt/httpmq
[ReportCard-Image]: https://goreportcard.com/badge/github.com/alwitt/httpmq

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

The REST API documentation can be found here [here](apis/README.md).
> Documentation generated with [widdershins](https://github.com/Mermade/widdershins)
>
> ```shell
> $ node widdershins --code --summary=true --search=false /path/to/docs/swagger.yaml -o README.md
> ```

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
    "name": "test-stream-00",
    "max_age": 300000000000,
    "subjects": [
        "test-subject.00",
        "test-subject.01"
    ]
}'
```

Response should be `{"success":true}`.

Define a consumer for the stream

```shell
curl -X POST 'http://127.0.0.1:3000/v1/admin/stream/test-stream-00/consumer' \
--header 'Content-Type: application/json' \
--data-raw '{
    "max_inflight": 4,
    "mode": "push",
    "name": "test-consumer-00",
    "filter_subject": "test-subject.01"
}'
```

Response should be `{"success":true}`.

Verify the consumer is defined

```shell
curl 'http://127.0.0.1:3000/v1/admin/stream/test-stream-00/consumer/test-consumer-00'
```

```json
{
    "success": true,
    "consumer": {
        "stream_name": "test-stream-00",
        "name": "test-consumer-00",
        "created": "2021-12-08T18:28:39.508206919Z",
        "config": {
            "deliver_subject": "_INBOX.bw26u2OqnVvhimXiT6RTeg",
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

> **IMPORTANT:** The message body is Base64 encoded.
>
> ```shell
> $ echo "Hello World" | base64
> SGVsbG8gV29ybGQK
> ```

---
## Subscribing For Messages

To subscribe to messages for a consumer on a stream

```shell
curl http://127.0.0.1:3001/v1/data/stream/test-stream-00/consumer/test-consumer-00?subject_name=test-subject.01 --http2-prior-knowledge
```

```shell
$ curl http://127.0.0.1:3001/v1/data/stream/test-stream-00/consumer/test-consumer-00?subject_name=test-subject.01 --http2-prior-knowledge
{"stream":"test-stream-00","subject":"test-subject.01","consumer":"test-consumer-00","sequence":{"stream":1,"consumer":1},"b64_msg":"SGVsbG8gV29ybGQK"}
```

After receiving a message, ACK that message with

```shell
curl -X POST 'http://127.0.0.1:3001/v1/data/stream/test-stream-00/consumer/test-consumer-00/ack' --header 'Content-Type: application/json' --data-raw '{"consumer": 1,"stream": 1}'
```
