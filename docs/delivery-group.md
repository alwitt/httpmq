# Consumer Delivery Group

> **NOTE:** For JetStream, it is called delivery group in the official Golang SDK.

[NATS Queue Group / JetStream Delivery Group](https://docs.nats.io/nats-concepts/core-nats/queue) allows messages directed at one consumer to be directed towards multiple connected clients in a load-balancing like scheme.

## 1. Defining a Consumer

The consumer create parameters are

```json
{
  "ack_wait": 0,
  "delivery_group": "string",
  "filter_subject": "string",
  "max_inflight": 0,
  "max_retry": 0,
  "mode": "string",
  "name": "string",
  "notes": "string"
}
```

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|ack_wait|integer|false|none|AckWait when specified, the number of ns to wait for ACK before retry|
|delivery_group|string|false|none|DeliveryGroup creates a consumer using a delivery group name.<br /><br />A consumer using delivery group allows multiple clients to subscribe under the same consumer<br />and group name tuple. For subjects this consumer listens to, the messages will be shared<br />amongst the connected clients.|
|filter_subject|string|false|none|FilterSubject sets the consumer to filter for subjects matching this NATs subject string<br /><br />See https://docs.nats.io/nats-concepts/subjects|
|max_inflight|integer|true|none|MaxInflight is max number of un-ACKed message permitted in-flight (must be >= 1)|
|max_retry|integer|false|none|MaxRetry max number of times an un-ACKed message is resent (-1: infinite)|
|mode|string|true|none|Mode whether the consumer is push or pull consumer|
|name|string|true|none|Name is the consumer name|
|notes|string|false|none|Notes are descriptions regarding this consumer|

The `delivery_group` field indicates that the consumer will use a delivery group.

## 2. Subscribing To a Delivery Group

To subscribe to messages as a consumer using a delivery group, the additional parameter `delivery_group` is needed when calling `GET`.

```shell
$ curl 'http://127.0.0.1:3001/v1/data/stream/testStream00/consumer/testConsumer00?subject_name=test-subject.01&delivery_group=testGroup00' --http2-prior-knowledge
```
