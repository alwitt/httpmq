# Message Retransmission

Consumer level message retransmission logic is implemented in [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream), and the control parameters are exposed through `httpmq`'s `management` API group.

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

A consumer's message retransmission behavior is controlled by

* `max_retry`: max number of times an un-ACKed message is resent
* `ack_wait`: the number of ns to wait for ACK before retry

When `httpmq` delivers a message to a consumer and an acknowledgement is not received within the `ack_wait` period, the message is retransmitted if the `max_retry` limit has not been reached.

## 2. Acknowledging Retransmitted Messages

As described [here](../README.md#25-subscribing-for-messages), when a message is retransmitted, the message delivered will retain the original `stream` level sequence number, but the `consumer` level sequence number will be incremented by one.

So to acknowledge the retransmitted message, the caller must send the most recently received `stream` and `consumer` sequence number tuple back to `httpmq` to properly acknowledge that message.

## 3. Stream Level Data Retention Settings And Retransmission

Message retransmission will only continue until the message is removed due to data retention policy set at the stream level.

If a consumer is defined with

* `max_retry` is 10
* `ack_wait` is 60 seconds

allowing a message to be retransmitted 10 times, over the span of 10 minutes.

If the parent stream has a data retention policy of deleting all messages older than 5 minutes, then retransmissions will end after 5 minutes instead of 10 minutes.
