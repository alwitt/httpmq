// Copyright 2021-2022 The httpmq Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/httpmq/common"
	"github.com/alwitt/httpmq/core"
	"github.com/alwitt/httpmq/management"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestStreamManagement(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-api-mgmt-stream"

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "apis_test",
		"component": "Management",
		"instance":  "manage_stream",
	}

	// Define NATS connection params
	natsParam := core.NATSConnectParams{
		ServerURI:           common.GetUnitTestNatsURI(),
		ConnectTimeout:      time.Second,
		MaxReconnectAttempt: 0,
		ReconnectWait:       time.Second,
		OnDisconnectCallback: func(_ *nats.Conn, e error) {
			if e != nil {
				log.WithError(e).WithFields(logTags).Error(
					"Disconnect callback triggered with failure",
				)
			}
		},
		OnReconnectCallback: func(_ *nats.Conn) {
			log.WithFields(logTags).Debug("Reconnected with NATs server")
		},
		OnCloseCallback: func(_ *nats.Conn) {
			log.WithFields(logTags).Debug("Disconnected from NATs server")
		},
	}

	js, err := core.GetJetStream(natsParam)
	assert.Nil(err)
	defer js.Close(utCtxt)

	mgmt, err := management.GetJetStreamController(js, testName)
	assert.Nil(err)

	uut, err := GetAPIRestJetStreamManagementHandler(mgmt, &common.HTTPConfig{
		Logging: common.HTTPRequestLogging{
			StartOfRequestMessage: "UT Request Start",
			EndOfRequestMessage:   "UT Request Complete",
		},
	})
	assert.Nil(err)

	// Case 0: check ready
	{
		req, err := http.NewRequest("GET", "/v1/admin/ready", nil)
		assert.Nil(err)

		respRecorder := httptest.NewRecorder()
		handler := uut.ReadyHandler()
		handler.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
	}

	checkHeader := func(w http.ResponseWriter, reqID string) {
		assert.Equal(reqID, w.Header().Get("Httpmq-Request-ID"))
		assert.Equal("application/json", w.Header().Get("content-type"))
	}

	// Case 1: define a new stream
	stream1 := uuid.NewString()
	maxMsgAge1 := time.Minute
	subjects1 := []string{uuid.NewString(), uuid.NewString()}
	{
		testReqID := uuid.NewString()
		params := management.JSStreamParam{
			Name:           stream1,
			Subjects:       subjects1,
			JSStreamLimits: management.JSStreamLimits{MaxAge: &maxMsgAge1},
		}
		t, err := json.Marshal(&params)
		assert.Nil(err)
		req, err := http.NewRequest("POST", "/v1/admin/stream", bytes.NewReader(t))
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		respRecorder := httptest.NewRecorder()
		handler := uut.CreateStreamHandler()
		handler.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
	}

	// Case 2: fetch that stream
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", "/v1/admin/stream", nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		respRecorder := httptest.NewRecorder()
		handler := uut.GetAllStreamsHandler()
		handler.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg APIRestRespAllJetStreams
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		info, ok := msg.Streams[stream1]
		assert.True(ok)
		assert.Equal(stream1, info.Config.Name)
		assert.EqualValues(subjects1, info.Config.Subjects)
	}
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", fmt.Sprintf("/v1/admin/stream/%s", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}", uut.GetStreamHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg APIRestRespOneJetStream
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Equal(stream1, msg.Stream.Config.Name)
		assert.EqualValues(subjects1, msg.Stream.Config.Subjects)
	}

	// Case 3: fetch an unknown stream
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", fmt.Sprintf("/v1/admin/stream/%s", uuid.NewString()), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}", uut.GetStreamHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusInternalServerError, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.False(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Equal(http.StatusInternalServerError, msg.Error.Code)
	}

	// Case 4: change target subject of the stream
	subjects4 := []string{uuid.NewString(), uuid.NewString()}
	{
		testReqID := uuid.NewString()
		reqMsg := APIRestReqStreamSubjects{Subjects: subjects4}
		t, err := json.Marshal(&reqMsg)
		assert.Nil(err)
		req, err := http.NewRequest(
			"PUT", fmt.Sprintf("/v1/admin/stream/%s/subject", stream1), bytes.NewReader(t),
		)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}/subject", uut.ChangeStreamSubjectsHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
	}
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", fmt.Sprintf("/v1/admin/stream/%s", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}", uut.GetStreamHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg APIRestRespOneJetStream
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Equal(stream1, msg.Stream.Config.Name)
		assert.EqualValues(subjects4, msg.Stream.Config.Subjects)
	}

	// Case 5: change data retention of the stream
	maxMsgAge5 := time.Minute * 10
	{
		testReqID := uuid.NewString()
		reqMsg := management.JSStreamLimits{MaxAge: &maxMsgAge5}
		t, err := json.Marshal(&reqMsg)
		assert.Nil(err)
		req, err := http.NewRequest(
			"PUT", fmt.Sprintf("/v1/admin/stream/%s/limit", stream1), bytes.NewReader(t),
		)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}/limit", uut.UpdateStreamLimitsHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
	}
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", fmt.Sprintf("/v1/admin/stream/%s", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}", uut.GetStreamHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg APIRestRespOneJetStream
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Equal(stream1, msg.Stream.Config.Name)
		assert.Equal(maxMsgAge5, msg.Stream.Config.MaxAge)
	}

	// Case 6: delete the stream
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("DELETE", fmt.Sprintf("/v1/admin/stream/%s", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}", uut.DeleteStreamHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
	}
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", fmt.Sprintf("/v1/admin/stream/%s", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}", uut.GetStreamHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusInternalServerError, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.False(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Equal(http.StatusInternalServerError, msg.Error.Code)
	}
}

func TestConsumerManagement(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	testName := "ut-api-mgmt-consumer"

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, utCtxtCancel := context.WithCancel(context.Background())
	defer utCtxtCancel()

	logTags := log.Fields{
		"module":    "apis_test",
		"component": "Management",
		"instance":  "manage_consumer",
	}

	// Define NATS connection params
	natsParam := core.NATSConnectParams{
		ServerURI:           common.GetUnitTestNatsURI(),
		ConnectTimeout:      time.Second,
		MaxReconnectAttempt: 0,
		ReconnectWait:       time.Second,
		OnDisconnectCallback: func(_ *nats.Conn, e error) {
			if e != nil {
				log.WithError(e).WithFields(logTags).Error(
					"Disconnect callback triggered with failure",
				)
			}
		},
		OnReconnectCallback: func(_ *nats.Conn) {
			log.WithFields(logTags).Debug("Reconnected with NATs server")
		},
		OnCloseCallback: func(_ *nats.Conn) {
			log.WithFields(logTags).Debug("Disconnected from NATs server")
		},
	}

	js, err := core.GetJetStream(natsParam)
	assert.Nil(err)
	defer js.Close(utCtxt)

	mgmt, err := management.GetJetStreamController(js, testName)
	assert.Nil(err)

	uut, err := GetAPIRestJetStreamManagementHandler(mgmt, &common.HTTPConfig{
		Logging: common.HTTPRequestLogging{
			StartOfRequestMessage: "UT Request Start",
			EndOfRequestMessage:   "UT Request Complete",
		},
	})
	assert.Nil(err)

	// Case 0: check ready
	{
		req, err := http.NewRequest("GET", "/v1/admin/ready", nil)
		assert.Nil(err)

		respRecorder := httptest.NewRecorder()
		handler := uut.ReadyHandler()
		handler.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
	}

	checkHeader := func(w http.ResponseWriter, reqID string) {
		assert.Equal(reqID, w.Header().Get("Httpmq-Request-ID"))
		assert.Equal("application/json", w.Header().Get("content-type"))
	}

	// Case 1: define a new stream
	stream1 := uuid.NewString()
	maxMsgAge1 := time.Minute
	subjects1 := []string{uuid.NewString(), uuid.NewString()}
	{
		testReqID := uuid.NewString()
		params := management.JSStreamParam{
			Name:           stream1,
			Subjects:       subjects1,
			JSStreamLimits: management.JSStreamLimits{MaxAge: &maxMsgAge1},
		}
		t, err := json.Marshal(&params)
		assert.Nil(err)
		req, err := http.NewRequest("POST", "/v1/admin/stream", bytes.NewReader(t))
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		respRecorder := httptest.NewRecorder()
		handler := uut.CreateStreamHandler()
		handler.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
	}

	// Case 2: fetch that stream
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", fmt.Sprintf("/v1/admin/stream/%s", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}", uut.GetStreamHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg APIRestRespOneJetStream
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Equal(stream1, msg.Stream.Config.Name)
		assert.EqualValues(subjects1, msg.Stream.Config.Subjects)
	}

	// Case 3: fetch consumers for this stream
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", fmt.Sprintf("/v1/admin/stream/%s/consumer", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}/consumer", uut.GetAllConsumersHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg APIRestRespAllJetStreamConsumers
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Empty(msg.Consumers)
	}

	// Case 4: create new consumer
	consumer4 := uuid.NewString()
	{
		testReqID := uuid.NewString()
		params := management.JetStreamConsumerParam{Name: consumer4, Mode: "push", MaxInflight: 2}
		t, err := json.Marshal(&params)
		assert.Nil(err)
		req, err := http.NewRequest(
			"POST", fmt.Sprintf("/v1/admin/stream/%s/consumer", stream1), bytes.NewReader(t),
		)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}/consumer", uut.CreateConsumerHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
	}

	// Case 5: read consumer back
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", fmt.Sprintf("/v1/admin/stream/%s/consumer", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}/consumer", uut.GetAllConsumersHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg APIRestRespAllJetStreamConsumers
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Len(msg.Consumers, 1)
		info, ok := msg.Consumers[consumer4]
		assert.True(ok)
		assert.Equal(consumer4, info.Name)
		assert.Equal(2, info.Config.MaxAckPending)
	}
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest(
			"GET", fmt.Sprintf("/v1/admin/stream/%s/consumer/%s", stream1, consumer4), nil,
		)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc(
			"/v1/admin/stream/{streamName}/consumer/{consumerName}", uut.GetConsumerHandler(),
		)
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg APIRestRespOneJetStreamConsumer
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Equal(consumer4, msg.Consumer.Name)
		assert.Equal(2, msg.Consumer.Config.MaxAckPending)
	}

	// Case 6: delete the consumer
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest(
			"DELETE", fmt.Sprintf("/v1/admin/stream/%s/consumer/%s", stream1, consumer4), nil,
		)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc(
			"/v1/admin/stream/{streamName}/consumer/{consumerName}", uut.DeleteConsumerHandler(),
		)
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
	}
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest(
			"GET", fmt.Sprintf("/v1/admin/stream/%s/consumer/%s", stream1, consumer4), nil,
		)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc(
			"/v1/admin/stream/{streamName}/consumer/{consumerName}", uut.GetConsumerHandler(),
		)
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusInternalServerError, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.False(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Equal(http.StatusInternalServerError, msg.Error.Code)
	}

	// Case 7: delete the stream
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("DELETE", fmt.Sprintf("/v1/admin/stream/%s", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}", uut.DeleteStreamHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.True(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
	}
	{
		testReqID := uuid.NewString()
		req, err := http.NewRequest("GET", fmt.Sprintf("/v1/admin/stream/%s", stream1), nil)
		req.Header.Add("Httpmq-Request-ID", testReqID)
		assert.Nil(err)

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/v1/admin/stream/{streamName}", uut.GetStreamHandler())
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusInternalServerError, respRecorder.Code)
		respMsg := respRecorder.Body.Bytes()
		var msg StandardResponse
		assert.Nil(json.Unmarshal(respMsg, &msg))
		assert.False(msg.Success)
		assert.Equal(testReqID, msg.RequestID)
		checkHeader(respRecorder, testReqID)
		assert.Equal(http.StatusInternalServerError, msg.Error.Code)
	}
}
