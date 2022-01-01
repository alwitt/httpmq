# HTTP MQ

[HTTP/2](https://http2.github.io/) based [message broker](https://en.wikipedia.org/wiki/Message_broker) built around [NATS JetStream](https://nats.io).

[![License Apache 2][License-Image]][License-Url] [![Go Report Card][ReportCard-Image]][ReportCard-Url] ![CICD workflow](https://github.com/alwitt/httpmq/actions/workflows/cicd.yaml/badge.svg) [![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Falwitt%2Fhttpmq.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Falwitt%2Fhttpmq?ref=badge_shield)

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/alwitt/httpmq
[ReportCard-Image]: https://goreportcard.com/badge/github.com/alwitt/httpmq

# Table of Content

- [1. Introduction](#1-introduction)
  * [1.1 References](#11-references)
- [2. Getting Started](#2-getting-started)
  * [2.1 Start the Management API](#21-start-the-management-api)
  * [2.2 Start the Dataplane API](#22-start-the-dataplane-api)
  * [2.3 Define Elements For Testing](#23-define-elements-for-testing)
  * [2.4 Publishing Messages](#24-publishing-messages)
  * [2.5 Subscribing For Messages](#25-subscribing-for-messages)
- [3. License](#3-license)

---

# [1. Introduction](#table-of-content)

[NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) is (as described in its documentation) a persistent message streaming solution designed to address "`problems identified with streaming in technology today - complexity, fragility, and a lack of scalability`". With `JetStream` serving as the core, `httpmq` is a thin HTTP/2 API layer which exposes some of core features of `JetStream`.

The `httpmq` application serves two API groups:

* `management`: allow users to administer JetStream streams, consumers, and subjects.
* `dataplane`: allow users to publish and subscribe through JetStream subjects.

Through the `management` API group, users can:

* For streams:
  * Define new stream.
  * Fetch parameters of all defined streams.
  * Fetch parameters of one defined stream.
  * Change a stream's data retention policy.
  * Change a stream's subjects of interest.
  * Delete a stream.

* For consumers:
  * Define new consumer on a stream.
    * Supports delivery / queue groups.
  * Fetch parameters of all defined consumers of a stream.
  * Fetch parameters of one defined consumer of a stream.
  * Delete a consumer.

Through the `dataplane` API group, users can:

* Publish messages to a subject.
* Push-subscribe for messages on a particular subject as a consumer of a stream.
  * Supports delivery / queue groups.

The `httpmq` application hosts the two API groups as different runtime modes; the application is either serving the `management` API group, or serving the `dataplane` API group.

## [1.1 References](#table-of-content)

* [NATS](https://nats.io)
* [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream)
* [NATS Subject-Based Messaging](https://docs.nats.io/nats-concepts/subjects)
* [NATS Queue Group / JetStream Delivery Group](https://docs.nats.io/nats-concepts/core-nats/queue)
* [NATS JetStream Deep Dive](https://docs.nats.io/using-nats/developer/develop_jetstream)
* [httpmq OpenAPI Specification](https://github.com/alwitt/httpmq-api/blob/main/README.md).

<!-- Documentation generated with [widdershins](https://github.com/Mermade/widdershins)
```shell
$ node widdershins --code --summary=true --search=false /path/to/docs/swagger.yaml -o README.md
``` -->

# [2. Getting Started](#table-of-content)

A helper Makefile is included to automate the common development tasks. The available make targets are:

```shell
$ make help
lint                           Lint the files
fix                            Lint and fix vialoations
compose                        Run docker-compose to create the DEV ENV
doc                            Generate the OpenAPI spec
mock                           Generate test mock interfaces
test                           Run unittests
build                          Build project binaries
clean                          Clean up DEV ENV
help                           Display this help screen
```

First, start the local development NATS server with JetStream enabled:

```shell
$ make compose
Removing docker_nats_1 ... done
Removing network docker_httpmq-test
Removing volume docker_nats_js_store
Creating network "docker_httpmq-test" with driver "bridge"
Creating volume "docker_nats_js_store" with default driver
Creating docker_nats_1 ... done
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

Verify the project builds, and passes unit-tests

```shell
$ make
$ make test
?   	github.com/alwitt/httpmq	[no test files]
?   	github.com/alwitt/httpmq/apis	[no test files]
?   	github.com/alwitt/httpmq/cmd	[no test files]
ok  	github.com/alwitt/httpmq/common	0.323s
?   	github.com/alwitt/httpmq/core	[no test files]
ok  	github.com/alwitt/httpmq/dataplane	0.967s
ok  	github.com/alwitt/httpmq/management	0.081s
```

## [2.1 Start the Management API](#table-of-content)

Start `httpmq` serving the management API group

```shell
$ ./httpmq.bin -l info management
2021/12/28 15:21:00  info Created JetStream client  component=jetstream-backend instance=nats://127.0.0.1:4222 module=core
2021/12/28 15:21:00  info Started HTTP server on http://127.0.0.1:3000 component=management instance=dvm-personal module=cmd
```

## [2.2 Start the Dataplane API](#table-of-content)

Start `httpmq` serving the dataplane API group

```shell
$ ./httpmq.bin -l info dataplane
2021/12/28 15:21:19  info Created JetStream client  component=jetstream-backend instance=nats://127.0.0.1:4222 module=core
2021/12/28 15:21:19  info Started HTTP server on http://127.0.0.1:3001 component=dataplane instance=dvm-personal module=cmd
```

## [2.3 Define Elements For Testing](#table-of-content)

Define a test stream

```shell
$ curl -X POST 'http://127.0.0.1:3000/v1/admin/stream' \
--header 'Content-Type: application/json' \
--data-raw '{
    "name": "testStream00",
    "max_age": 300000000000,
    "subjects": [
        "test-subject.00",
        "test-subject.01"
    ]
}'
{"success":true}
```

Verify the stream is defined

```shell
$ curl 'http://127.0.0.1:3000/v1/admin/stream/testStream00' | jq '.'
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

Define a test consumer on this stream

```shell
$ curl -X POST 'http://127.0.0.1:3000/v1/admin/stream/testStream00/consumer' \
--header 'Content-Type: application/json' \
--data-raw '{
    "max_inflight": 4,
    "mode": "push",
    "name": "testConsumer00",
    "filter_subject": "test-subject.01"
}'
{"success":true}
```

Verify the consumer is defined

```shell
$ curl 'http://127.0.0.1:3000/v1/admin/stream/testStream00/consumer/testConsumer00' | jq '.'
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

## [2.4 Publishing Messages](#table-of-content)

Publish a message for a subject

```shell
$ curl -X POST 'http://127.0.0.1:3001/v1/data/subject/test-subject.01' --header 'Content-Type: text/plain' --data-raw "$(echo 'Hello World' | base64)"
{"success":true}
```

> **IMPORTANT:** The message body must be Base64 encoded.
>
> ```shell
> $ echo "Hello World" | base64
> SGVsbG8gV29ybGQK
> ```

## [2.5 Subscribing For Messages](#table-of-content)

Subscribe to messages for a consumer on a stream

```shell
$ curl http://127.0.0.1:3001/v1/data/stream/testStream00/consumer/testConsumer00?subject_name=test-subject.01 --http2-prior-knowledge
{"success":true,"stream":"testStream00","subject":"test-subject.01","consumer":"testConsumer00","sequence":{"stream":1,"consumer":1},"b64_msg":"SGVsbG8gV29ybGQK"}
...
```

After receiving a message, acknowledge receiving that message with

```shell
$ curl -X POST 'http://127.0.0.1:3001/v1/data/stream/testStream00/consumer/testConsumer00/ack' --header 'Content-Type: application/json' --data-raw '{"consumer": 1,"stream": 1}'
{"success":true}
```

The `consumer` and `stream` fields are the sequence numbers which arrived with the message.

If an acknowledgement is not sent within the consumer's configured max ACK wait duration, the message will be sent through this consumer's subscription again. This time, the `stream` sequence number is unchanged, but the `consumer` sequence number is increased by one.

```shell
{"success":true,"stream":"testStream00","subject":"test-subject.01","consumer":"testConsumer00","sequence":{"stream":1,"consumer":2},"b64_msg":"SGVsbG8gV29ybGQK"}
```

When acknowledging this message now, use `'{"consumer": 2,"stream": 1}'` as the payload.

```shell
$ curl -X POST 'http://127.0.0.1:3001/v1/data/stream/testStream00/consumer/testConsumer00/ack' --header 'Content-Type: application/json' --data-raw '{"consumer": 2,"stream": 1}'
{"success":true}
```

# [3. License](#table-of-content)

Unless otherwise noted, the httpmq source files are distributed under the Apache Version 2.0 license found in the LICENSE file.

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Falwitt%2Fhttpmq.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Falwitt%2Fhttpmq?ref=badge_large)
