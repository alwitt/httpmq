---
title: httpmq v0.1.0
language_tabs:
  - shell: Shell
  - http: HTTP
  - javascript: JavaScript
  - ruby: Ruby
  - python: Python
  - php: PHP
  - java: Java
  - go: Go
toc_footers: []
includes: []
search: false
code_clipboard: true
highlight_theme: darkula
headingLevel: 2
generator: widdershins v4.0.1

---

<h1 id="httpmq">httpmq v0.1.0</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

HTTP/2 based message broker built around NATS JetStream

Base URLs:

* <a href="//localhost:3000">//localhost:3000</a>

<h1 id="httpmq-management">Management</h1>

## For liveness check

`GET /alive`

Will return success to indicate REST API module is live

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="for-liveness-check-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|string|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

<aside class="success">
This operation does not require authentication
</aside>

## For readiness check

`GET /ready`

Will return success if REST API module is ready for use

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="for-readiness-check-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|string|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

<aside class="success">
This operation does not require authentication
</aside>

## Query for info on all streams

`GET /v1/admin/stream`

Query for the details of all streams

> Body parameter

```json
{
  "max_age": 0,
  "max_bytes": 0,
  "max_consumers": 0,
  "max_msg_size": 0,
  "max_msgs": 0,
  "max_msgs_per_subject": 0,
  "name": "string",
  "subjects": [
    "string"
  ]
}
```

<h3 id="query-for-info-on-all-streams-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[management.JSStreamParam](#schemamanagement.jsstreamparam)|true|JetStream stream setting|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "streams": {
    "property1": {
      "config": {
        "description": "string",
        "max_age": 0,
        "max_bytes": 0,
        "max_consumers": 0,
        "max_msg_size": 0,
        "max_msgs": 0,
        "max_msgs_per_subject": 0,
        "name": "string",
        "subjects": [
          "string"
        ]
      },
      "created": "string",
      "state": {
        "bytes": 0,
        "consumer_count": 0,
        "first_seq": 0,
        "first_ts": "string",
        "last_seq": 0,
        "last_ts": "string",
        "messages": 0
      }
    },
    "property2": {
      "config": {
        "description": "string",
        "max_age": 0,
        "max_bytes": 0,
        "max_consumers": 0,
        "max_msg_size": 0,
        "max_msgs": 0,
        "max_msgs_per_subject": 0,
        "name": "string",
        "subjects": [
          "string"
        ]
      },
      "created": "string",
      "state": {
        "bytes": 0,
        "consumer_count": 0,
        "first_seq": 0,
        "first_ts": "string",
        "last_seq": 0,
        "last_ts": "string",
        "messages": 0
      }
    }
  },
  "success": true
}
```

<h3 id="query-for-info-on-all-streams-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.APIRestRespAllJetStreams](#schemaapis.apirestrespalljetstreams)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Define new stream

`POST /v1/admin/stream`

Define new JetStream stream

> Body parameter

```json
{
  "max_age": 0,
  "max_bytes": 0,
  "max_consumers": 0,
  "max_msg_size": 0,
  "max_msgs": 0,
  "max_msgs_per_subject": 0,
  "name": "string",
  "subjects": [
    "string"
  ]
}
```

<h3 id="define-new-stream-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[management.JSStreamParam](#schemamanagement.jsstreamparam)|true|JetStream stream setting|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="define-new-stream-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Delete a stream

`DELETE /v1/admin/stream/{streamName}`

Delete a stream

<h3 id="delete-a-stream-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="delete-a-stream-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Query for info on one stream

`GET /v1/admin/stream/{streamName}`

Query for the details of one stream

<h3 id="query-for-info-on-one-stream-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "stream": {
    "config": {
      "description": "string",
      "max_age": 0,
      "max_bytes": 0,
      "max_consumers": 0,
      "max_msg_size": 0,
      "max_msgs": 0,
      "max_msgs_per_subject": 0,
      "name": "string",
      "subjects": [
        "string"
      ]
    },
    "created": "string",
    "state": {
      "bytes": 0,
      "consumer_count": 0,
      "first_seq": 0,
      "first_ts": "string",
      "last_seq": 0,
      "last_ts": "string",
      "messages": 0
    }
  },
  "success": true
}
```

<h3 id="query-for-info-on-one-stream-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.APIRestRespOneJetStream](#schemaapis.apirestresponejetstream)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Get all consumers of a stream

`GET /v1/admin/stream/{streamName}/consumer`

Query for the details of all consumers of a stream

<h3 id="get-all-consumers-of-a-stream-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|

> Example responses

> 200 Response

```json
{
  "consumers": {
    "property1": {
      "ack_floor": {
        "consumer_seq": 0,
        "last_active": "string",
        "stream_seq": 0
      },
      "config": {
        "ack_wait": 0,
        "deliver_group": "string",
        "deliver_subject": "string",
        "filter_subject": "string",
        "max_ack_pending": 0,
        "max_deliver": 0,
        "max_waiting": 0,
        "notes": "string"
      },
      "created": "string",
      "delivered": {
        "consumer_seq": 0,
        "last_active": "string",
        "stream_seq": 0
      },
      "name": "string",
      "num_ack_pending": 0,
      "num_pending": 0,
      "num_redelivered": 0,
      "num_waiting": 0,
      "stream_name": "string"
    },
    "property2": {
      "ack_floor": {
        "consumer_seq": 0,
        "last_active": "string",
        "stream_seq": 0
      },
      "config": {
        "ack_wait": 0,
        "deliver_group": "string",
        "deliver_subject": "string",
        "filter_subject": "string",
        "max_ack_pending": 0,
        "max_deliver": 0,
        "max_waiting": 0,
        "notes": "string"
      },
      "created": "string",
      "delivered": {
        "consumer_seq": 0,
        "last_active": "string",
        "stream_seq": 0
      },
      "name": "string",
      "num_ack_pending": 0,
      "num_pending": 0,
      "num_redelivered": 0,
      "num_waiting": 0,
      "stream_name": "string"
    }
  },
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="get-all-consumers-of-a-stream-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.APIRestRespAllJetStreamConsumers](#schemaapis.apirestrespalljetstreamconsumers)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Create a consumer on a stream

`POST /v1/admin/stream/{streamName}/consumer`

Create a new consumer on a stream. The stream must already be defined.

> Body parameter

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

<h3 id="create-a-consumer-on-a-stream-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|
|body|body|[management.JetStreamConsumerParam](#schemamanagement.jetstreamconsumerparam)|true|Consumer parameters|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="create-a-consumer-on-a-stream-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Delete one consumer of a stream

`DELETE /v1/admin/stream/{streamName}/consumer/{consumerName}`

Delete one consumer of a stream

<h3 id="delete-one-consumer-of-a-stream-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|
|consumerName|path|string|true|JetStream consumer name|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="delete-one-consumer-of-a-stream-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Get one consumer of a stream

`GET /v1/admin/stream/{streamName}/consumer/{consumerName}`

Query for the details of a consumer on a stream

<h3 id="get-one-consumer-of-a-stream-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|
|consumerName|path|string|true|JetStream consumer name|

> Example responses

> 200 Response

```json
{
  "consumer": {
    "ack_floor": {
      "consumer_seq": 0,
      "last_active": "string",
      "stream_seq": 0
    },
    "config": {
      "ack_wait": 0,
      "deliver_group": "string",
      "deliver_subject": "string",
      "filter_subject": "string",
      "max_ack_pending": 0,
      "max_deliver": 0,
      "max_waiting": 0,
      "notes": "string"
    },
    "created": "string",
    "delivered": {
      "consumer_seq": 0,
      "last_active": "string",
      "stream_seq": 0
    },
    "name": "string",
    "num_ack_pending": 0,
    "num_pending": 0,
    "num_redelivered": 0,
    "num_waiting": 0,
    "stream_name": "string"
  },
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="get-one-consumer-of-a-stream-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.APIRestRespOneJetStreamConsumer](#schemaapis.apirestresponejetstreamconsumer)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Change limits a stream

`PUT /v1/admin/stream/{streamName}/limit`

Change the data retention limits of a stream

> Body parameter

```json
{
  "max_age": 0,
  "max_bytes": 0,
  "max_consumers": 0,
  "max_msg_size": 0,
  "max_msgs": 0,
  "max_msgs_per_subject": 0
}
```

<h3 id="change-limits-a-stream-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|
|body|body|[management.JSStreamLimits](#schemamanagement.jsstreamlimits)|true|New stream limits|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="change-limits-a-stream-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Change subjects of a stream

`PUT /v1/admin/stream/{streamName}/subject`

Change the list of subjects of interest for a stream

> Body parameter

```json
{
  "subjects": [
    "string"
  ]
}
```

<h3 id="change-subjects-of-a-stream-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|
|body|body|[apis.APIRestReqStreamSubjects](#schemaapis.apirestreqstreamsubjects)|true|List of new subjects|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="change-subjects-of-a-stream-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

<h1 id="httpmq-dataplane">Dataplane</h1>

## Establish a pull subscribe session

`GET /v1/data/stream/{streamName}/consumer/{consumerName}`

Establish a JetStream pull subscribe session for a client. This is a long lived

<h3 id="establish-a-pull-subscribe-session-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|
|consumerName|path|string|true|JetStream consumer name|
|subject_name|query|string|true|JetStream subject to subscribe to|
|max_msg_inflight|query|integer|false|Max number of inflight messages (DEFAULT: 1)|
|delivery_group|query|string|false|Needed if consumer uses delivery groups|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="establish-a-pull-subscribe-session-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Handle ACK for message

`POST /v1/data/stream/{streamName}/consumer/{consumerName}/ack`

Process JetStream message ACK for a stream / consumer

> Body parameter

```json
{
  "consumer": 0,
  "stream": 0
}
```

<h3 id="handle-ack-for-message-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|streamName|path|string|true|JetStream stream name|
|consumerName|path|string|true|JetStream consumer name|
|body|body|[dataplane.AckSeqNum](#schemadataplane.ackseqnum)|true|Message message sequence numbers|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="handle-ack-for-message-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

## Publish a message

`POST /v1/data/subject/{subjectName}`

Publish a Base64 encoded message to a JetStream subject

> Body parameter

```
string

```

<h3 id="publish-a-message-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|subjectName|path|string|true|JetStream subject to publish under|
|body|body|string|true|Message to publish in Base64 encoding|

> Example responses

> 200 Response

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}
```

<h3 id="publish-a-message-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|success|[apis.StandardResponse](#schemaapis.standardresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|error|string|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|error|[apis.StandardResponse](#schemaapis.standardresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Httpmq-Request-ID|string||Request ID to match against logs|
|400|Httpmq-Request-ID|string||Request ID to match against logs|
|500|Httpmq-Request-ID|string||Request ID to match against logs|

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_apis.APIRestReqStreamSubjects">apis.APIRestReqStreamSubjects</h2>

<a id="schemaapis.apirestreqstreamsubjects"></a>
<a id="schema_apis.APIRestReqStreamSubjects"></a>
<a id="tocSapis.apirestreqstreamsubjects"></a>
<a id="tocsapis.apirestreqstreamsubjects"></a>

```json
{
  "subjects": [
    "string"
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|subjects|[string]|true|none|Subjects the list of new subject this stream will listen to|

<h2 id="tocS_apis.APIRestRespAllJetStreamConsumers">apis.APIRestRespAllJetStreamConsumers</h2>

<a id="schemaapis.apirestrespalljetstreamconsumers"></a>
<a id="schema_apis.APIRestRespAllJetStreamConsumers"></a>
<a id="tocSapis.apirestrespalljetstreamconsumers"></a>
<a id="tocsapis.apirestrespalljetstreamconsumers"></a>

```json
{
  "consumers": {
    "property1": {
      "ack_floor": {
        "consumer_seq": 0,
        "last_active": "string",
        "stream_seq": 0
      },
      "config": {
        "ack_wait": 0,
        "deliver_group": "string",
        "deliver_subject": "string",
        "filter_subject": "string",
        "max_ack_pending": 0,
        "max_deliver": 0,
        "max_waiting": 0,
        "notes": "string"
      },
      "created": "string",
      "delivered": {
        "consumer_seq": 0,
        "last_active": "string",
        "stream_seq": 0
      },
      "name": "string",
      "num_ack_pending": 0,
      "num_pending": 0,
      "num_redelivered": 0,
      "num_waiting": 0,
      "stream_name": "string"
    },
    "property2": {
      "ack_floor": {
        "consumer_seq": 0,
        "last_active": "string",
        "stream_seq": 0
      },
      "config": {
        "ack_wait": 0,
        "deliver_group": "string",
        "deliver_subject": "string",
        "filter_subject": "string",
        "max_ack_pending": 0,
        "max_deliver": 0,
        "max_waiting": 0,
        "notes": "string"
      },
      "created": "string",
      "delivered": {
        "consumer_seq": 0,
        "last_active": "string",
        "stream_seq": 0
      },
      "name": "string",
      "num_ack_pending": 0,
      "num_pending": 0,
      "num_redelivered": 0,
      "num_waiting": 0,
      "stream_name": "string"
    }
  },
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|consumers|object|false|none|Consumers the set of consumer details mapped against consumer name|
|» **additionalProperties**|[apis.APIRestRespConsumerInfo](#schemaapis.apirestrespconsumerinfo)|false|none|none|
|error|[apis.ErrorDetail](#schemaapis.errordetail)|false|none|Error are details in case of errors|
|success|boolean|false|none|Success indicates whether the request was successful|

<h2 id="tocS_apis.APIRestRespAllJetStreams">apis.APIRestRespAllJetStreams</h2>

<a id="schemaapis.apirestrespalljetstreams"></a>
<a id="schema_apis.APIRestRespAllJetStreams"></a>
<a id="tocSapis.apirestrespalljetstreams"></a>
<a id="tocsapis.apirestrespalljetstreams"></a>

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "streams": {
    "property1": {
      "config": {
        "description": "string",
        "max_age": 0,
        "max_bytes": 0,
        "max_consumers": 0,
        "max_msg_size": 0,
        "max_msgs": 0,
        "max_msgs_per_subject": 0,
        "name": "string",
        "subjects": [
          "string"
        ]
      },
      "created": "string",
      "state": {
        "bytes": 0,
        "consumer_count": 0,
        "first_seq": 0,
        "first_ts": "string",
        "last_seq": 0,
        "last_ts": "string",
        "messages": 0
      }
    },
    "property2": {
      "config": {
        "description": "string",
        "max_age": 0,
        "max_bytes": 0,
        "max_consumers": 0,
        "max_msg_size": 0,
        "max_msgs": 0,
        "max_msgs_per_subject": 0,
        "name": "string",
        "subjects": [
          "string"
        ]
      },
      "created": "string",
      "state": {
        "bytes": 0,
        "consumer_count": 0,
        "first_seq": 0,
        "first_ts": "string",
        "last_seq": 0,
        "last_ts": "string",
        "messages": 0
      }
    }
  },
  "success": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|error|[apis.ErrorDetail](#schemaapis.errordetail)|false|none|Error are details in case of errors|
|streams|object|false|none|Streams the set of stream details mapped against its names|
|» **additionalProperties**|[apis.APIRestRespStreamInfo](#schemaapis.apirestrespstreaminfo)|false|none|none|
|success|boolean|false|none|Success indicates whether the request was successful|

<h2 id="tocS_apis.APIRestRespConsumerConfig">apis.APIRestRespConsumerConfig</h2>

<a id="schemaapis.apirestrespconsumerconfig"></a>
<a id="schema_apis.APIRestRespConsumerConfig"></a>
<a id="tocSapis.apirestrespconsumerconfig"></a>
<a id="tocsapis.apirestrespconsumerconfig"></a>

```json
{
  "ack_wait": 0,
  "deliver_group": "string",
  "deliver_subject": "string",
  "filter_subject": "string",
  "max_ack_pending": 0,
  "max_deliver": 0,
  "max_waiting": 0,
  "notes": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|ack_wait|integer|false|none|AckWait duration (ns) to wait for an ACK for the delivery of a message|
|deliver_group|string|false|none|DeliverGroup is the delivery group if this consumer uses delivery group<br /><br />A consumer using delivery group allows multiple clients to subscribe under the same consumer<br />and group name tuple. For subjects this consumer listens to, the messages will be shared<br />amongst the connected clients.|
|deliver_subject|string|false|none|DeliverSubject subject this consumer is listening on|
|filter_subject|string|false|none|FilterSubject sets the consumer to filter for subjects matching this NATs subject string<br /><br />See https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/naming|
|max_ack_pending|integer|false|none|MaxAckPending controls the max number of un-ACKed messages permitted in-flight|
|max_deliver|integer|false|none|MaxDeliver max number of times a message can be deliveried (including retry) to this consumer|
|max_waiting|integer|false|none|MaxWaiting NATS JetStream does not clearly document this|
|notes|string|false|none|Description an optional description of the consumer|

<h2 id="tocS_apis.APIRestRespConsumerInfo">apis.APIRestRespConsumerInfo</h2>

<a id="schemaapis.apirestrespconsumerinfo"></a>
<a id="schema_apis.APIRestRespConsumerInfo"></a>
<a id="tocSapis.apirestrespconsumerinfo"></a>
<a id="tocsapis.apirestrespconsumerinfo"></a>

```json
{
  "ack_floor": {
    "consumer_seq": 0,
    "last_active": "string",
    "stream_seq": 0
  },
  "config": {
    "ack_wait": 0,
    "deliver_group": "string",
    "deliver_subject": "string",
    "filter_subject": "string",
    "max_ack_pending": 0,
    "max_deliver": 0,
    "max_waiting": 0,
    "notes": "string"
  },
  "created": "string",
  "delivered": {
    "consumer_seq": 0,
    "last_active": "string",
    "stream_seq": 0
  },
  "name": "string",
  "num_ack_pending": 0,
  "num_pending": 0,
  "num_redelivered": 0,
  "num_waiting": 0,
  "stream_name": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|ack_floor|[apis.APIRestRespSequenceInfo](#schemaapis.apirestrespsequenceinfo)|false|none|AckFloor is the sequence number of the last received ACKed<br /><br />For messages which failed to be ACKed (retry limit reached), the floor moves up to<br />include these message sequence numbers indicating these messages will not be retried.|
|config|[apis.APIRestRespConsumerConfig](#schemaapis.apirestrespconsumerconfig)|false|none|Config are the consumer config parameters|
|created|string|false|none|Created is when this consumer was defined|
|delivered|[apis.APIRestRespSequenceInfo](#schemaapis.apirestrespsequenceinfo)|false|none|Delivered is the sequence number of the last message delivered|
|name|string|false|none|Name is the name of the consumer|
|num_ack_pending|integer|false|none|NumAckPending is the number of ACK pending / messages in-flight|
|num_pending|integer|false|none|NumPending is the number of message to be delivered for this consumer|
|num_redelivered|integer|false|none|NumRedelivered is the number of messages redelivered|
|num_waiting|integer|false|none|NumWaiting is the number of message in-flight / ACK pending|
|stream_name|string|false|none|Stream is the name of the stream|

<h2 id="tocS_apis.APIRestRespOneJetStream">apis.APIRestRespOneJetStream</h2>

<a id="schemaapis.apirestresponejetstream"></a>
<a id="schema_apis.APIRestRespOneJetStream"></a>
<a id="tocSapis.apirestresponejetstream"></a>
<a id="tocsapis.apirestresponejetstream"></a>

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "stream": {
    "config": {
      "description": "string",
      "max_age": 0,
      "max_bytes": 0,
      "max_consumers": 0,
      "max_msg_size": 0,
      "max_msgs": 0,
      "max_msgs_per_subject": 0,
      "name": "string",
      "subjects": [
        "string"
      ]
    },
    "created": "string",
    "state": {
      "bytes": 0,
      "consumer_count": 0,
      "first_seq": 0,
      "first_ts": "string",
      "last_seq": 0,
      "last_ts": "string",
      "messages": 0
    }
  },
  "success": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|error|[apis.ErrorDetail](#schemaapis.errordetail)|false|none|Error are details in case of errors|
|stream|[apis.APIRestRespStreamInfo](#schemaapis.apirestrespstreaminfo)|false|none|Stream the details for this stream|
|success|boolean|false|none|Success indicates whether the request was successful|

<h2 id="tocS_apis.APIRestRespOneJetStreamConsumer">apis.APIRestRespOneJetStreamConsumer</h2>

<a id="schemaapis.apirestresponejetstreamconsumer"></a>
<a id="schema_apis.APIRestRespOneJetStreamConsumer"></a>
<a id="tocSapis.apirestresponejetstreamconsumer"></a>
<a id="tocsapis.apirestresponejetstreamconsumer"></a>

```json
{
  "consumer": {
    "ack_floor": {
      "consumer_seq": 0,
      "last_active": "string",
      "stream_seq": 0
    },
    "config": {
      "ack_wait": 0,
      "deliver_group": "string",
      "deliver_subject": "string",
      "filter_subject": "string",
      "max_ack_pending": 0,
      "max_deliver": 0,
      "max_waiting": 0,
      "notes": "string"
    },
    "created": "string",
    "delivered": {
      "consumer_seq": 0,
      "last_active": "string",
      "stream_seq": 0
    },
    "name": "string",
    "num_ack_pending": 0,
    "num_pending": 0,
    "num_redelivered": 0,
    "num_waiting": 0,
    "stream_name": "string"
  },
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|consumer|[apis.APIRestRespConsumerInfo](#schemaapis.apirestrespconsumerinfo)|false|none|Consumer the details regarding this consumer|
|error|[apis.ErrorDetail](#schemaapis.errordetail)|false|none|Error are details in case of errors|
|success|boolean|false|none|Success indicates whether the request was successful|

<h2 id="tocS_apis.APIRestRespSequenceInfo">apis.APIRestRespSequenceInfo</h2>

<a id="schemaapis.apirestrespsequenceinfo"></a>
<a id="schema_apis.APIRestRespSequenceInfo"></a>
<a id="tocSapis.apirestrespsequenceinfo"></a>
<a id="tocsapis.apirestrespsequenceinfo"></a>

```json
{
  "consumer_seq": 0,
  "last_active": "string",
  "stream_seq": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|consumer_seq|integer|false|none|Consumer is consumer level sequence number|
|last_active|string|false|none|Last timestamp when these values updated|
|stream_seq|integer|false|none|Stream is stream level sequence number|

<h2 id="tocS_apis.APIRestRespStreamConfig">apis.APIRestRespStreamConfig</h2>

<a id="schemaapis.apirestrespstreamconfig"></a>
<a id="schema_apis.APIRestRespStreamConfig"></a>
<a id="tocSapis.apirestrespstreamconfig"></a>
<a id="tocsapis.apirestrespstreamconfig"></a>

```json
{
  "description": "string",
  "max_age": 0,
  "max_bytes": 0,
  "max_consumers": 0,
  "max_msg_size": 0,
  "max_msgs": 0,
  "max_msgs_per_subject": 0,
  "name": "string",
  "subjects": [
    "string"
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|description|string|false|none|Description is an optional description of the stream|
|max_age|integer|false|none|MaxBytes is the max duration (ns) the stream will store a message<br /><br />Messages breaching the limit will be removed.|
|max_bytes|integer|false|none|MaxBytes is the max number of message bytes the stream will store.<br /><br />Oldest messages are removed once limit breached.|
|max_consumers|integer|false|none|MaxConsumers is the max number of consumers allowed on the stream|
|max_msg_size|integer|false|none|MaxMsgSize is the max size of a message allowed in this stream|
|max_msgs|integer|false|none|MaxMsgs is the max number of messages the stream will store.<br /><br />Oldest messages are removed once limit breached.|
|max_msgs_per_subject|integer|false|none|MaxMsgsPerSubject is the maximum number of subjects allowed on this stream|
|name|string|false|none|Name is the stream name|
|subjects|[string]|false|none|Subjects is the list subjects this stream is listening on|

<h2 id="tocS_apis.APIRestRespStreamInfo">apis.APIRestRespStreamInfo</h2>

<a id="schemaapis.apirestrespstreaminfo"></a>
<a id="schema_apis.APIRestRespStreamInfo"></a>
<a id="tocSapis.apirestrespstreaminfo"></a>
<a id="tocsapis.apirestrespstreaminfo"></a>

```json
{
  "config": {
    "description": "string",
    "max_age": 0,
    "max_bytes": 0,
    "max_consumers": 0,
    "max_msg_size": 0,
    "max_msgs": 0,
    "max_msgs_per_subject": 0,
    "name": "string",
    "subjects": [
      "string"
    ]
  },
  "created": "string",
  "state": {
    "bytes": 0,
    "consumer_count": 0,
    "first_seq": 0,
    "first_ts": "string",
    "last_seq": 0,
    "last_ts": "string",
    "messages": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|config|[apis.APIRestRespStreamConfig](#schemaapis.apirestrespstreamconfig)|false|none|Config is the stream config parameters|
|created|string|false|none|Created is the stream creation timestamp|
|state|[apis.APIRestRespStreamState](#schemaapis.apirestrespstreamstate)|false|none|State is the stream current state|

<h2 id="tocS_apis.APIRestRespStreamState">apis.APIRestRespStreamState</h2>

<a id="schemaapis.apirestrespstreamstate"></a>
<a id="schema_apis.APIRestRespStreamState"></a>
<a id="tocSapis.apirestrespstreamstate"></a>
<a id="tocsapis.apirestrespstreamstate"></a>

```json
{
  "bytes": 0,
  "consumer_count": 0,
  "first_seq": 0,
  "first_ts": "string",
  "last_seq": 0,
  "last_ts": "string",
  "messages": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|bytes|integer|false|none|Bytes is the number of message bytes in the stream|
|consumer_count|integer|false|none|Consumers number of consumers on the stream|
|first_seq|integer|false|none|FirstSeq is the oldest message sequence number on the stream|
|first_ts|string|false|none|FirstTime is the oldest message timestamp on the stream|
|last_seq|integer|false|none|LastSeq is the newest message sequence number on the stream|
|last_ts|string|false|none|LastTime is the newest message timestamp on the stream|
|messages|integer|false|none|Msgs is the number of messages in the stream|

<h2 id="tocS_apis.ErrorDetail">apis.ErrorDetail</h2>

<a id="schemaapis.errordetail"></a>
<a id="schema_apis.ErrorDetail"></a>
<a id="tocSapis.errordetail"></a>
<a id="tocsapis.errordetail"></a>

```json
{
  "code": 0,
  "message": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|code|integer|false|none|Code is the response code|
|message|string|false|none|Msg is an optional descriptive message|

<h2 id="tocS_apis.StandardResponse">apis.StandardResponse</h2>

<a id="schemaapis.standardresponse"></a>
<a id="schema_apis.StandardResponse"></a>
<a id="tocSapis.standardresponse"></a>
<a id="tocsapis.standardresponse"></a>

```json
{
  "error": {
    "code": 0,
    "message": "string"
  },
  "success": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|error|[apis.ErrorDetail](#schemaapis.errordetail)|false|none|Error are details in case of errors|
|success|boolean|false|none|Success indicates whether the request was successful|

<h2 id="tocS_dataplane.AckSeqNum">dataplane.AckSeqNum</h2>

<a id="schemadataplane.ackseqnum"></a>
<a id="schema_dataplane.AckSeqNum"></a>
<a id="tocSdataplane.ackseqnum"></a>
<a id="tocsdataplane.ackseqnum"></a>

```json
{
  "consumer": 0,
  "stream": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|consumer|integer|true|none|Consumer is the JetStream message sequence number for this consumer|
|stream|integer|true|none|Stream is the JetStream message sequence number for this stream|

<h2 id="tocS_management.JSStreamLimits">management.JSStreamLimits</h2>

<a id="schemamanagement.jsstreamlimits"></a>
<a id="schema_management.JSStreamLimits"></a>
<a id="tocSmanagement.jsstreamlimits"></a>
<a id="tocsmanagement.jsstreamlimits"></a>

```json
{
  "max_age": 0,
  "max_bytes": 0,
  "max_consumers": 0,
  "max_msg_size": 0,
  "max_msgs": 0,
  "max_msgs_per_subject": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|max_age|integer|false|none|MaxBytes is the max duration (ns) the stream will store a message<br /><br />Messages breaching the limit will be removed.|
|max_bytes|integer|false|none|MaxBytes is the max number of message bytes the stream will store.<br /><br />Oldest messages are removed once limit breached.|
|max_consumers|integer|false|none|MaxConsumers is the max number of consumers allowed on the stream|
|max_msg_size|integer|false|none|MaxMsgSize is the max size of a message allowed in this stream|
|max_msgs|integer|false|none|MaxMsgs is the max number of messages the stream will store.<br /><br />Oldest messages are removed once limit breached.|
|max_msgs_per_subject|integer|false|none|MaxMsgsPerSubject is the maximum number of subjects allowed on this stream|

<h2 id="tocS_management.JSStreamParam">management.JSStreamParam</h2>

<a id="schemamanagement.jsstreamparam"></a>
<a id="schema_management.JSStreamParam"></a>
<a id="tocSmanagement.jsstreamparam"></a>
<a id="tocsmanagement.jsstreamparam"></a>

```json
{
  "max_age": 0,
  "max_bytes": 0,
  "max_consumers": 0,
  "max_msg_size": 0,
  "max_msgs": 0,
  "max_msgs_per_subject": 0,
  "name": "string",
  "subjects": [
    "string"
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|max_age|integer|false|none|MaxBytes is the max duration (ns) the stream will store a message<br /><br />Messages breaching the limit will be removed.|
|max_bytes|integer|false|none|MaxBytes is the max number of message bytes the stream will store.<br /><br />Oldest messages are removed once limit breached.|
|max_consumers|integer|false|none|MaxConsumers is the max number of consumers allowed on the stream|
|max_msg_size|integer|false|none|MaxMsgSize is the max size of a message allowed in this stream|
|max_msgs|integer|false|none|MaxMsgs is the max number of messages the stream will store.<br /><br />Oldest messages are removed once limit breached.|
|max_msgs_per_subject|integer|false|none|MaxMsgsPerSubject is the maximum number of subjects allowed on this stream|
|name|string|true|none|Name is the stream name|
|subjects|[string]|false|none|Subjects is the list of subjects of interest for this stream|

<h2 id="tocS_management.JetStreamConsumerParam">management.JetStreamConsumerParam</h2>

<a id="schemamanagement.jetstreamconsumerparam"></a>
<a id="schema_management.JetStreamConsumerParam"></a>
<a id="tocSmanagement.jetstreamconsumerparam"></a>
<a id="tocsmanagement.jetstreamconsumerparam"></a>

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

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|ack_wait|integer|false|none|AckWait when specified, the number of ns to wait for ACK before retry|
|delivery_group|string|false|none|DeliveryGroup creates a consumer using a delivery group name.<br /><br />A consumer using delivery group allows multiple clients to subscribe under the same consumer<br />and group name tuple. For subjects this consumer listens to, the messages will be shared<br />amongst the connected clients.|
|filter_subject|string|false|none|FilterSubject sets the consumer to filter for subjects matching this NATs subject string<br /><br />See https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/naming|
|max_inflight|integer|true|none|MaxInflight is max number of un-ACKed message permitted in-flight (must be >= 1)|
|max_retry|integer|false|none|MaxRetry max number of times an un-ACKed message is resent (-1: infinite)|
|mode|string|true|none|Mode whether the consumer is push or pull consumer|
|name|string|true|none|Name is the consumer name|
|notes|string|false|none|Notes are descriptions regarding this consumer|
