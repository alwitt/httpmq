# HTTP MQ

HTTP/2 Based Message Broker

---
## Local Testing - Setup

Start the test NATS JetStream server

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

To start the management server

```shell
./httpmq.bin -l debug --nmra 1 management
```

```shell
$ ./httpmq.bin -l debug --nmra 1 management
2021/12/08 10:22:51 debug Starting params
2021/12/08 10:22:51  info Created JetStream client  component=jetstream-backend instance=nats://127.0.0.1:4222 module=core
2021/12/08 10:22:51  info Started HTTP server on http://:3000 component=management instance=dvm-personal module=cmd
```

To start the dataplane server

```shell
./httpmq.bin -l debug --nmra 1 dataplane
```

```shell
$ ./httpmq.bin -l debug --nmra 1 dataplane
2021/12/08 10:24:31 debug Starting params
2021/12/08 10:24:31  info Created JetStream client  component=jetstream-backend instance=nats://127.0.0.1:4222 module=core
2021/12/08 10:24:31  info Started HTTP server on http://:3001 component=management instance=dvm-personal module=cmd
```

---
## Local Testing - Management

Start by defining a JetStream stream

```shell
curl --location --request POST 'http://127.0.0.1:3000/v1/admin/stream' \
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

Define a consumer for the stream

```shell
curl --location --request POST 'http://127.0.0.1:3000/v1/admin/stream/test-stream-00/consumer' \
--header 'Content-Type: application/json' \
--data-raw '{
    "max_inflight": 4,
    "mode": "push",
    "name": "test-consumer-00",
    "filter_subject": "test-subject.01"
}'
```

Verify the consumer is ready for use

```shell
curl --location --request GET 'http://127.0.0.1:3000/v1/admin/stream/test-stream-00/consumer/test-consumer-00'
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
            "max_deliver": 4,
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
## Local Testing - Publishing Messages

To publish a message for a subject

```shell
curl --location --request POST 'http://127.0.0.1:3001/v1/data/subject/test-subject.01' --header 'Content-Type: text/plain' --data-raw "$(echo 'Hello World' | base64)"
```

> **IMPORTANT:** The message body is Base64 encoded.
>
> ```shell
> $ echo "Hello World" | base64
> SGVsbG8gV29ybGQK
> ```

---
## Local Testing - Reading Messages

To subscribe to messages for a consumer on a stream

```shell
curl http://127.0.0.1:3001/v1/data/stream/test-stream-00/consumer/test-consumer-00?subject_name=test-subject.01 --http2-prior-knowledge
```

After receiving a message, ACK that message with

```shell
curl --location --request POST 'http://127.0.0.1:3001/v1/data/stream/test-stream-00/consumer/test-consumer-00/ack' --header 'Content-Type: application/json' --data-raw '{"consumer": 1,"stream": 1}'
```
