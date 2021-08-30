package subscription

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/apex/log"
	"github.com/go-playground/validator"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/storage"
)

// ClientSubscription entry detailing a client subscription session
type ClientSubscription struct {
	ClientName     string    `json:"client_name" validate:"required"`
	ServingNode    string    `json:"serving_node" validate:"required"`
	StatusUpdateAt time.Time `json:"updated_at"`
	EstablishedAt  time.Time `json:"established_at"`
}

// SubscriptionRecords record of subscriptions active at the moment
type SubscriptionRecords struct {
	ActiveSessions map[string]ClientSubscription `json:"active_sessions" validate:"required,dive"`
}

// Scan implements the sql.Scanner interface
func (r *SubscriptionRecords) Scan(src interface{}) error {
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("src is not []byte")
	}
	return json.Unmarshal(bytes, r)
}

// Value implements the sql/driver.Valuer interface
func (r SubscriptionRecords) Value() (driver.Value, error) {
	return json.Marshal(&r)
}

// ========================================================================================
// Controller operating the active subscription records

// SubscriptionRecorder manage the active subscription records
type SubscriptionRecorder interface {
	ReadySessionRecords() error
	LogClientSession(clientName string, node string, timestamp time.Time) (ClientSubscription, error)
	RefreshClientSession(clientName string, node string, timestamp time.Time) error
	ClearClientSession(clientName string, node string, timestamp time.Time) error
	ClearInactiveSessions(maxInactivePeriod time.Duration, timestamp time.Time) error
}

// subscriptionRecorderImpl implements SubscriptionRecorder
type subscriptionRecorderImpl struct {
	common.Component
	store              storage.KeyValueStore
	tp                 common.TaskProcessor
	storeKey           string
	kvStoreCallTimeout time.Duration
	validate           *validator.Validate
}

// DefineSubscriptionRecorder create new subscription recorder
func DefineSubscriptionRecorder(
	dataStore storage.KeyValueStore,
	tp common.TaskProcessor,
	keyPrefix string,
	timeout time.Duration,
) (SubscriptionRecorder, error) {
	logTags := log.Fields{
		"module": "subscription", "component": "manager",
	}
	instance := subscriptionRecorderImpl{
		Component:          common.Component{LogTags: logTags},
		store:              dataStore,
		tp:                 tp,
		storeKey:           fmt.Sprintf("%s/active-sessions", keyPrefix),
		kvStoreCallTimeout: timeout,
		validate:           validator.New(),
	}
	// Add handlers
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(subRecorderLogClientReq{}),
		instance.processLogClientRequest,
	); err != nil {
		return nil, err
	}
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(subRecorderRefreshClientReq{}),
		instance.processRefreshClientRequest,
	); err != nil {
		return nil, err
	}
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(subRecorderDeleteClientReq{}),
		instance.processClearClientSessRequest,
	); err != nil {
		return nil, err
	}
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(subRecorderReadyRecordReq{}),
		instance.processReadySessionRecordsRequest,
	); err != nil {
		return nil, err
	}
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(subRecorderClearInactiveReq{}),
		instance.processClearInactiveRequest,
	); err != nil {
		return nil, err
	}
	return &instance, nil
}

func (r *subscriptionRecorderImpl) readCurrentActiveSessions() (SubscriptionRecords, error) {
	var records SubscriptionRecords
	useContext, cancel := context.WithTimeout(context.Background(), r.kvStoreCallTimeout)
	err := r.store.Get(r.storeKey, &records, useContext)
	cancel()
	if err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Unable to fetch active session records %s", r.storeKey,
		)
		return SubscriptionRecords{}, err
	}
	if err := r.validate.Struct(&records); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Active session records %s is not valid", r.storeKey,
		)
		// TODO FIXME: Reset the active session records??
		return SubscriptionRecords{}, err
	}
	return records, nil
}

func (r *subscriptionRecorderImpl) storeActiveSessionRecords(records SubscriptionRecords) error {
	useContext, cancel := context.WithTimeout(context.Background(), r.kvStoreCallTimeout)
	defer cancel()
	if err := r.store.Set(r.storeKey, records, useContext); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to update active session records %s", r.storeKey,
		)
		return err
	}
	return nil
}

// ----------------------------------------------------------------------------------------

type subRecorderReadyRecordReq struct {
	resultCB func(error)
}

// ReadySessionRecords ready the subscription session records
func (r *subscriptionRecorderImpl) ReadySessionRecords() error {
	complete := make(chan bool, 1)
	var processError error
	// Handler core processing result
	handler := func(err error) {
		processError = err
		complete <- true
	}

	// Make the request
	request := subRecorderReadyRecordReq{
		resultCB: handler,
	}

	if err := r.tp.Submit(request, context.Background()); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to submit ready-session-records request",
		)
		return err
	}

	// Wait for completion
	<-complete

	return processError
}

func (r *subscriptionRecorderImpl) processReadySessionRecordsRequest(param interface{}) error {
	request, ok := param.(subRecorderReadyRecordReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for ready session records",
			reflect.TypeOf(param),
		)
	}
	err := r.ProcessReadySessionRecordsRequest()
	request.resultCB(err)
	return err
}

// ProcessReadySessionRecordsRequest ready the subscription session records
func (r *subscriptionRecorderImpl) ProcessReadySessionRecordsRequest() error {
	// Fetch the current active session records
	_, err := r.readCurrentActiveSessions()
	if err != nil {
		log.WithError(err).WithFields(r.LogTags).Error("Unable to fetch active session records")
		// Create new records
		newRecords := SubscriptionRecords{ActiveSessions: make(map[string]ClientSubscription)}
		if err := r.storeActiveSessionRecords(newRecords); err != nil {
			log.WithError(err).WithFields(r.LogTags).Errorf(
				"Failed to initialized active session records %s", r.storeKey,
			)
			return err
		}
		log.WithFields(r.LogTags).Infof("Initialized active session records %s", r.storeKey)
	}
	return nil
}

// ----------------------------------------------------------------------------------------

type subRecorderLogClientReq struct {
	timestamp  time.Time
	clientName string
	nodeName   string
	resultCB   func(ClientSubscription, error)
}

// LogClientSession record a new client subscription session
func (r *subscriptionRecorderImpl) LogClientSession(
	clientName string, node string, timestamp time.Time,
) (ClientSubscription, error) {
	complete := make(chan bool, 1)
	var sessionRecord ClientSubscription
	var processError error
	// Handler core processing result
	handler := func(record ClientSubscription, err error) {
		sessionRecord = record
		processError = err
		complete <- true
	}

	// Make the request
	request := subRecorderLogClientReq{
		timestamp:  timestamp,
		clientName: clientName,
		nodeName:   node,
		resultCB:   handler,
	}

	if err := r.tp.Submit(request, context.Background()); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to submit log-client-session request",
		)
		return ClientSubscription{}, err
	}

	// Wait for completion
	<-complete

	return sessionRecord, processError
}

// processLogClientRequest support task processor, deal with log client request
func (r *subscriptionRecorderImpl) processLogClientRequest(param interface{}) error {
	request, ok := param.(subRecorderLogClientReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for log client subscription session",
			reflect.TypeOf(param),
		)
	}
	existingRecord, err := r.ProcessLogClientRequest(
		request.clientName, request.nodeName, request.timestamp,
	)
	request.resultCB(existingRecord, err)
	return err
}

// ProcessLogClientRequest record a new client subscription session
func (r *subscriptionRecorderImpl) ProcessLogClientRequest(
	clientName string, node string, timestamp time.Time,
) (ClientSubscription, error) {
	// Fetch the current active session records
	records, err := r.readCurrentActiveSessions()
	if err != nil {
		log.WithError(err).WithFields(r.LogTags).Error("Unable to fetch active session records")
		return ClientSubscription{}, err
	}

	// Check whether the client is already known
	existing, ok := records.ActiveSessions[clientName]
	if ok {
		// client is already known
		return existing, fmt.Errorf(
			"client %s already has an active subscription session", clientName,
		)
	}

	// Create new entry
	newRecord := ClientSubscription{
		ClientName:     clientName,
		ServingNode:    node,
		StatusUpdateAt: timestamp,
		EstablishedAt:  timestamp,
	}
	records.ActiveSessions[clientName] = newRecord

	// Store the updated version
	if err := r.storeActiveSessionRecords(records); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to update active session records %s with new client %s",
			r.storeKey,
			clientName,
		)
		return ClientSubscription{}, err
	}

	log.WithFields(r.LogTags).Infof(
		"Added active subscription session record for new client %s with %s @ %s to %s",
		clientName,
		node,
		timestamp.Format(time.RFC3339),
		r.storeKey,
	)
	return newRecord, nil
}

// ----------------------------------------------------------------------------------------

type subRecorderRefreshClientReq struct {
	timestamp  time.Time
	clientName string
	nodeName   string
	resultCB   func(error)
}

// RefreshClientSession refresh an existing subscription session
func (r *subscriptionRecorderImpl) RefreshClientSession(
	clientName string, node string, timestamp time.Time,
) error {
	complete := make(chan bool, 1)
	var processError error
	// Handler core processing result
	handler := func(err error) {
		processError = err
		complete <- true
	}

	// Make the request
	request := subRecorderRefreshClientReq{
		timestamp:  timestamp,
		clientName: clientName,
		nodeName:   node,
		resultCB:   handler,
	}

	if err := r.tp.Submit(request, context.Background()); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to submit refresh-client-session request",
		)
		return err
	}

	// Wait for completion
	<-complete

	return processError
}

// processRefreshClientRequest support task processor, deal with refresh client request
func (r *subscriptionRecorderImpl) processRefreshClientRequest(param interface{}) error {
	request, ok := param.(subRecorderRefreshClientReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for refresh client subscription session",
			reflect.TypeOf(param),
		)
	}
	err := r.ProcessRefreshClientRequest(
		request.clientName, request.nodeName, request.timestamp,
	)
	request.resultCB(err)
	return nil
}

// ProcessRefreshClientRequest refresh an existing subscription session
func (r *subscriptionRecorderImpl) ProcessRefreshClientRequest(
	clientName string, node string, timestamp time.Time,
) error {
	// Fetch the current active session records
	records, err := r.readCurrentActiveSessions()
	if err != nil {
		log.WithError(err).WithFields(r.LogTags).Error("Unable to fetch active session records")
		return err
	}

	// Check whether the client is already known
	existing, ok := records.ActiveSessions[clientName]
	if !ok {
		return fmt.Errorf(
			"client %s has no active subscription session", clientName,
		)
	}

	// Verify that node is still the same
	if existing.ServingNode != node {
		return fmt.Errorf(
			"client %s has an active session with node %s, but new refresh info is from %s",
			clientName,
			existing.ServingNode,
			node,
		)
	}

	// Update the timestamp
	existing.StatusUpdateAt = timestamp
	records.ActiveSessions[clientName] = existing

	// Store the updated version
	if err := r.storeActiveSessionRecords(records); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to update active session records %s to refresh client %s",
			r.storeKey,
			clientName,
		)
		return err
	}

	log.WithFields(r.LogTags).Debugf(
		"Refresh subscription session record for client %s with %s @ %s to %s",
		clientName,
		node,
		timestamp.Format(time.RFC3339),
		r.storeKey,
	)
	return nil
}

// ----------------------------------------------------------------------------------------

type subRecorderDeleteClientReq struct {
	timestamp  time.Time
	clientName string
	node       string
	resultCB   func(error)
}

// ClearClientSession clear a client subscription record
func (r *subscriptionRecorderImpl) ClearClientSession(
	clientName string, node string, timestamp time.Time,
) error {
	complete := make(chan bool, 1)
	var processError error
	// Handler core processing result
	handler := func(err error) {
		processError = err
		complete <- true
	}

	// Make the request
	request := subRecorderDeleteClientReq{
		timestamp:  timestamp,
		clientName: clientName,
		node:       node,
		resultCB:   handler,
	}

	if err := r.tp.Submit(request, context.Background()); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to submit clear-client-session request",
		)
		return err
	}

	// Wait for completion
	<-complete

	return processError
}

// processClearClientSessRequest support task processor, deal with clear client session request
func (r *subscriptionRecorderImpl) processClearClientSessRequest(param interface{}) error {
	request, ok := param.(subRecorderDeleteClientReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for clear client subscription session",
			reflect.TypeOf(param),
		)
	}
	err := r.ProcessClearClientSessRequest(
		request.clientName, request.node, request.timestamp,
	)
	request.resultCB(err)
	return err
}

// ProcessClearClientSessRequest clear a client subscription record
func (r *subscriptionRecorderImpl) ProcessClearClientSessRequest(
	clientName string, node string, timestamp time.Time,
) error {
	// Fetch the current active session records
	records, err := r.readCurrentActiveSessions()
	if err != nil {
		log.WithError(err).WithFields(r.LogTags).Error("Unable to fetch active session records")
		return err
	}

	// Check whether the client is already known
	existing, ok := records.ActiveSessions[clientName]
	if !ok {
		return fmt.Errorf(
			"client %s has no active subscription session", clientName,
		)
	}

	// Verify that node is still the same
	if existing.ServingNode != node {
		return fmt.Errorf(
			"client %s has an active session with node %s, but clear session info is from %s",
			clientName,
			existing.ServingNode,
			node,
		)
	}

	delete(records.ActiveSessions, clientName)

	// Store the updated version
	if err := r.storeActiveSessionRecords(records); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to update active session records %s to clear client %s session",
			r.storeKey,
			clientName,
		)
		return err
	}

	log.WithFields(r.LogTags).Infof(
		"Cleared subscription session record for client %s with %s @ %s to %s",
		clientName,
		node,
		timestamp.Format(time.RFC3339),
		r.storeKey,
	)
	return nil
}

// ----------------------------------------------------------------------------------------

type subRecorderClearInactiveReq struct {
	timestamp   time.Time
	inactiveFor time.Duration
	resultCB    func(error)
}

// ClearInactiveSessions clear out inactive sessions which have not been refreshed with
// in the max allowed inactive period.
func (r *subscriptionRecorderImpl) ClearInactiveSessions(
	maxInactivePeriod time.Duration, timestamp time.Time,
) error {
	complete := make(chan bool, 1)
	var processError error
	// Handler core processing result
	handler := func(err error) {
		processError = err
		complete <- true
	}

	// Make the request
	request := subRecorderClearInactiveReq{
		timestamp: timestamp, inactiveFor: maxInactivePeriod, resultCB: handler,
	}

	if err := r.tp.Submit(request, context.Background()); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to submit clear-inactive-sessions request",
		)
		return err
	}

	// Wait for completion
	<-complete

	return processError
}

func (r *subscriptionRecorderImpl) processClearInactiveRequest(param interface{}) error {
	request, ok := param.(subRecorderClearInactiveReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for clear inactive sessions",
			reflect.TypeOf(param),
		)
	}
	err := r.ProcessClearInactiveRequest(request.inactiveFor, request.timestamp)
	request.resultCB(err)
	return err
}

// ProcessClearInactiveRequest clear out inactive sessions which have not been refreshed with
// in the max allowed inactive period.
func (r *subscriptionRecorderImpl) ProcessClearInactiveRequest(
	maxInactivePeriod time.Duration, timestamp time.Time,
) error {
	// Fetch the current active session records
	records, err := r.readCurrentActiveSessions()
	if err != nil {
		log.WithError(err).WithFields(r.LogTags).Error("Unable to fetch active session records")
		return err
	}

	// Filter out the sessions which have not been refreshed
	removeSessions := []string{}
	for client, session := range records.ActiveSessions {
		timePassed := timestamp.Sub(session.StatusUpdateAt)
		if timePassed > maxInactivePeriod {
			removeSessions = append(removeSessions, client)
			log.WithFields(r.LogTags).Infof(
				"Client %s session lasted refreshed at %s. Timeout @ %s",
				client,
				session.StatusUpdateAt.Format(time.RFC3339),
				timePassed,
			)
		}
	}

	log.WithFields(r.LogTags).Infof(
		"Following client subscription sessions have become inactive %v", removeSessions,
	)

	// Remove them from record
	for _, client := range removeSessions {
		delete(records.ActiveSessions, client)
	}

	// Store the updated version
	if err := r.storeActiveSessionRecords(records); err != nil {
		log.WithError(err).WithFields(r.LogTags).Errorf(
			"Failed to update active session records %s",
			r.storeKey,
		)
		return err
	}

	log.WithFields(r.LogTags).Infof(
		"Following client subscription sessions are removed from record %v", removeSessions,
	)
	return nil
}
