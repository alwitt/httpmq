package dispatch

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apex/log"
	"github.com/go-playground/validator"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/storage"
)

const (
	STATE_INIT = iota
	STATE_RUNNING
)

// ========================================================================================
// Controller controls the operation of the message dispatcher
type Controller interface {
	Start(ctxt context.Context) error
	ReceivedACKs(msgIndexes []int64, ctxt context.Context) error
	RegisterInflight(index int64, timestamp time.Time, ctxt context.Context, blocking bool) error
	TriggerMessageRetransmit(
		timestamp time.Time, timerInterval time.Duration, ctxt context.Context,
	) error
	StopOperation() error
}

// controllerImpl implements Controller
type controllerImpl struct {
	common.Component
	store            storage.KeyValueStore
	tp               common.TaskProcessor
	storeKey         string
	maxRetries       int
	validate         *validator.Validate
	operatingState   int
	operationContext context.Context
	contextCancel    context.CancelFunc
	// Callbacks to hook into other components
	requestMsgReTX requestRestransmit
	sendACKToReTX  indicateReceivedACKs
	sendACKToDisp  indicateReceivedACKs
	startReader    startQueueRead
}

// DefineController create new dispatch controller
func DefineController(
	client string,
	queue string,
	dataStore storage.KeyValueStore,
	tp common.TaskProcessor,
	keyPrefix string,
	maxRetries int,
	// Callbacks
	reqMsgReTX requestRestransmit,
	ackToReTX indicateReceivedACKs,
	ackToDisp indicateReceivedACKs,
	startReader startQueueRead,
) (Controller, error) {
	logTags := log.Fields{
		"module": "dispatch", "component": "controller", "client": client, "queue": queue,
	}
	ctxt, cancel := context.WithCancel(context.Background())
	instance := controllerImpl{
		Component:        common.Component{LogTags: logTags},
		store:            dataStore,
		tp:               tp,
		storeKey:         fmt.Sprintf("%s/client/%s/queue/%s", keyPrefix, client, queue),
		maxRetries:       maxRetries,
		validate:         validator.New(),
		operatingState:   STATE_INIT,
		operationContext: ctxt,
		contextCancel:    cancel,
		requestMsgReTX:   reqMsgReTX,
		sendACKToReTX:    ackToReTX,
		sendACKToDisp:    ackToDisp,
		startReader:      startReader,
	}
	// Add handlers
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(ctrlStartReq{}), instance.processStartRequest,
	); err != nil {
		return nil, err
	}
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(ctrlACKMsgReq{}), instance.processACKMsgRequest,
	); err != nil {
		return nil, err
	}
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(ctrlRegisterInflightReq{}), instance.processInflightRegisterRequest,
	); err != nil {
		return nil, err
	}
	if err := tp.AddToTaskExecutionMap(
		reflect.TypeOf(ctrlTriggerRetransmit{}), instance.processTriggerRetransmitRequest,
	); err != nil {
		return nil, err
	}
	return &instance, nil
}

func (c *controllerImpl) msgACKFanOut(indexes []int64) error {
	if err := c.sendACKToReTX(indexes, c.operationContext); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Error(
			"Unable to send ACKs to retransmit component",
		)
		return err
	}
	if err := c.sendACKToDisp(indexes, c.operationContext); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Error(
			"Unable to send ACKs to dispatch component",
		)
		return err
	}
	return nil
}

// retransmitMessageCore helper function to perform the core of retansmission
func (c *controllerImpl) retransmitMessageCore(
	currentState QueueSubInfo, timestamp time.Time, timerInterval time.Duration,
) error {
	// For each inflight message
	// * If inflight message maxed retry attempts, discard it
	// * If message sent within the timer interval, ignore it
	// * Submit for retransmit
	ackToSend := []int64{}
	msgToReTX := []int64{}
	for msgIndex, msgInfo := range currentState.Inflight {
		if msgInfo.MaxRetry <= msgInfo.RetryCount {
			log.WithFields(c.LogTags).WithField("state", c.operatingState).Debugf(
				"%s maxed out retransmit attempts", msgInfo,
			)
			ackToSend = append(ackToSend, msgIndex)
			continue
		}
		if timestamp.Sub(msgInfo.FirstSentAt) < timerInterval {
			log.WithFields(c.LogTags).WithField("state", c.operatingState).Debugf(
				"Ignoring %s as it was just sent", msgInfo,
			)
			continue
		}
		log.WithFields(c.LogTags).WithField("state", c.operatingState).Debugf(
			"Will restransmit %s", msgInfo,
		)
		msgToReTX = append(msgToReTX, msgIndex)
	}

	// Send out any ACKs
	if len(ackToSend) > 0 {
		if err := c.msgACKFanOut(ackToSend); err != nil {
			log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
				"Unable to distribute ACKs %v", ackToSend,
			)
			return err
		}
	}

	// Send out the retransmit request
	if len(msgToReTX) > 0 {
		if err := c.requestMsgReTX(msgToReTX, c.operationContext); err != nil {
			log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
				"Unable to request msg retransmit %v", msgToReTX,
			)
			return err
		}
	}

	// Update the state
	if len(ackToSend) > 0 || len(msgToReTX) > 0 {
		for _, index := range ackToSend {
			delete(currentState.Inflight, index)
		}
		for _, index := range msgToReTX {
			tmp := currentState.Inflight[index]
			tmp.RetryCount += 1
			currentState.Inflight[index] = tmp
		}
		// Record the updated state
		if err := c.store.Set(c.storeKey, currentState, c.operationContext); err != nil {
			log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
				"State %s update failed", c.storeKey,
			)
			return err
		}
	}
	return nil
}

// ----------------------------------------------------------------------------------------

type ctrlStartReq struct {
	resultCB func(err error)
}

// Start start the controller from initial state
func (c *controllerImpl) Start(ctxt context.Context) error {
	processError := make(chan error, 1)
	handler := func(err error) {
		processError <- err
	}

	// Make the request
	request := ctrlStartReq{
		resultCB: handler,
	}

	if err := c.tp.Submit(request, ctxt); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf(
			"Failed to submit controller-start request",
		)
		return err
	}

	// Wait for completion
	select {
	case err := <-processError:
		return err
	case <-ctxt.Done():
		return ctxt.Err()
	}
}

func (c *controllerImpl) processStartRequest(param interface{}) error {
	request, ok := param.(ctrlStartReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for controller-start request",
			reflect.TypeOf(param),
		)
	}
	err := c.ProcessStartRequest()
	request.resultCB(err)
	return err
}

// ProcessStartRequest start the controller from initial state
func (c *controllerImpl) ProcessStartRequest() error {
	// Steps
	// * Fetch state from storage
	// * If there are inflight messages left over, request retransmit for
	//   them if possible.
	// * Start queue data fetcher

	if c.operatingState != STATE_INIT {
		err := fmt.Errorf(
			"controll will only perform start processing when in STATE_INIT",
		)
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Error(
			"Unable to process start request",
		)
		return err
	}

	// Fetch state
	var currentState QueueSubInfo
	if err := c.store.Get(c.storeKey, &currentState, c.operationContext); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to process start request. Unable to read current state from %s",
			c.storeKey,
		)
		return err
	}

	// There are message still in flight from previous run
	if currentState.Inflight != nil && len(currentState.Inflight) > 0 {
		if err := c.retransmitMessageCore(currentState, time.Now(), 0); err != nil {
			log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Error(
				"Unable to process start request. Leftover inflight message processing failed",
			)
			return err
		}
	}

	// Start the queue data fetcher
	if err := c.startReader(currentState.NewestACKedIndex + 1); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Error(
			"Unable to process start request. Starting queue reader failed",
		)
		return err
	}

	c.operatingState = STATE_RUNNING
	log.WithFields(c.LogTags).WithField("state", c.operatingState).Infof("Starting operations")

	return nil
}

// ----------------------------------------------------------------------------------------

type ctrlACKMsgReq struct {
	indexes  []int64
	resultCB func(error)
}

// ReceivedACKs received ACK for certain messages
func (c *controllerImpl) ReceivedACKs(msgIndexes []int64, ctxt context.Context) error {
	processError := make(chan error, 1)
	handler := func(err error) {
		processError <- err
	}

	// Make the request
	request := ctrlACKMsgReq{
		indexes:  msgIndexes,
		resultCB: handler,
	}

	if err := c.tp.Submit(request, ctxt); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf(
			"Failed to submit acked-messages request",
		)
		return err
	}

	// Wait for completion
	select {
	case err := <-processError:
		return err
	case <-ctxt.Done():
		return ctxt.Err()
	}
}

func (c *controllerImpl) processACKMsgRequest(param interface{}) error {
	request, ok := param.(ctrlACKMsgReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for ACK msg request",
			reflect.TypeOf(param),
		)
	}
	err := c.ProcessACKMsgRequest(request.indexes)
	request.resultCB(err)
	return err
}

// ProcessACKMsgRequest received ACK for certain messages
func (c *controllerImpl) ProcessACKMsgRequest(msgIndexes []int64) error {
	// Steps
	// * Fetch state from storage
	// * Filter the indexes which are not actually inflight
	// * Notify the other components that ACKs were received
	// * Update state in storage

	if c.operatingState != STATE_RUNNING {
		err := fmt.Errorf(
			"controll will only perform msg ACK processing when in STATE_RUNNING",
		)
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to process msg ACKs %v", msgIndexes,
		)
		return err
	}

	// Fetch state
	var currentState QueueSubInfo
	if err := c.store.Get(c.storeKey, &currentState, c.operationContext); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to process msg ACKs %v. Unable to read current state from %s",
			msgIndexes,
			c.storeKey,
		)
		return err
	}

	// Filter out only message indexes which are actually in inflight
	realIndexes := []int64{}
	for _, index := range msgIndexes {
		if _, ok := currentState.Inflight[index]; ok {
			realIndexes = append(realIndexes, index)
		}
	}

	// Fan out the ACKs
	if err := c.msgACKFanOut(realIndexes); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to process msg ACKs %v. Unable to distribute ACKs", msgIndexes,
		)
		return err
	}

	// Update the state in storage
	for _, index := range realIndexes {
		delete(currentState.Inflight, index)
	}
	if err := c.store.Set(c.storeKey, currentState, c.operationContext); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to process msg ACKs %v. State %s update failed", msgIndexes, c.storeKey,
		)
		return err
	}

	return nil
}

// ----------------------------------------------------------------------------------------

type ctrlRegisterInflightReq struct {
	msgIndex  int64
	blocking  bool
	timestamp time.Time
	resultCB  func(err error)
}

// RegisterInflight register a message is in flight
func (c *controllerImpl) RegisterInflight(
	index int64, timestamp time.Time, ctxt context.Context, blocking bool,
) error {
	processError := make(chan error, 1)
	handler := func(err error) {
		processError <- err
	}

	// Make the request
	request := ctrlRegisterInflightReq{
		msgIndex: index, blocking: blocking, timestamp: timestamp, resultCB: handler,
	}

	if err := c.tp.Submit(request, ctxt); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf(
			"Failed to submit register-inflight request",
		)
		return err
	}

	// Wait for completion
	select {
	case err := <-processError:
		return err
	case <-ctxt.Done():
		return ctxt.Err()
	}
}

func (c *controllerImpl) processInflightRegisterRequest(param interface{}) error {
	request, ok := param.(ctrlRegisterInflightReq)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for register-inflight request",
			reflect.TypeOf(param),
		)
	}
	if !request.blocking {
		request.resultCB(nil)
	}
	err := c.ProcessInflightRegisterRequest(request.msgIndex, request.timestamp)
	if request.blocking {
		request.resultCB(err)
	}
	return err
}

// RegisterInflight register a message is in flight
func (c *controllerImpl) ProcessInflightRegisterRequest(index int64, timestamp time.Time) error {
	// Steps
	// * Fetch state from storage
	// * Verify the message is not already inflight
	// * Update state in storage

	if c.operatingState != STATE_RUNNING {
		err := fmt.Errorf(
			"controll will only perform inflight msg register when in STATE_RUNNING",
		)
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to register inflight message %d", index,
		)
		return err
	}

	// Fetch state
	var currentState QueueSubInfo
	if err := c.store.Get(c.storeKey, &currentState, c.operationContext); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to register inflight message %d. Unable to read current state from %s",
			index,
			c.storeKey,
		)
		return err
	}

	// Verify the message is not already inflight
	if _, ok := currentState.Inflight[index]; ok {
		log.WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to register inflight message %d. Message is already inflight", index,
		)
		return nil
	}

	// Update the state in storage
	currentState.Inflight[index] = InfightMessageInfo{
		Index: index, FirstSentAt: timestamp, RetryCount: 0, MaxRetry: c.maxRetries,
	}
	if err := c.store.Set(c.storeKey, currentState, c.operationContext); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to register inflight message %d. State %s update failed", index, c.storeKey,
		)
		return err
	}

	return nil
}

// ----------------------------------------------------------------------------------------

type ctrlTriggerRetransmit struct {
	timestamp         time.Time
	kickTimerInterval time.Duration
}

// TriggerMessageRetransmit trigger periodic message retransmission check
func (c *controllerImpl) TriggerMessageRetransmit(
	timestamp time.Time, timerInterval time.Duration, ctxt context.Context,
) error {
	// Make the request
	request := ctrlTriggerRetransmit{
		timestamp: timestamp, kickTimerInterval: timerInterval,
	}

	if err := c.tp.Submit(request, ctxt); err != nil {
		log.WithError(err).WithFields(c.LogTags).Errorf(
			"Failed to submit retransmit-check request",
		)
		return err
	}

	return nil
}

func (c *controllerImpl) processTriggerRetransmitRequest(param interface{}) error {
	request, ok := param.(ctrlTriggerRetransmit)
	if !ok {
		return fmt.Errorf(
			"can not process unknown type %s for retransmit-check request",
			reflect.TypeOf(param),
		)
	}
	return c.ProcessTriggerRetransmitRequest(request.timestamp, request.kickTimerInterval)
}

// ProcessTriggerRetransmitRequest trigger periodic message retransmission check
func (c *controllerImpl) ProcessTriggerRetransmitRequest(
	timestamp time.Time, timerInterval time.Duration,
) error {
	// Steps
	// * Fetch state from storage
	// * For each inflight message
	//   * If inflight message maxed retry attempts, discard it
	//   * If message sent within the timer interval, ignore it
	//   * Submit for retransmit

	if c.operatingState != STATE_RUNNING {
		err := fmt.Errorf(
			"controll will only execute retransmit check when in STATE_RUNNING",
		)
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Error(
			"Unable to execute retransmit check",
		)
		return err
	}

	// Fetch state
	var currentState QueueSubInfo
	if err := c.store.Get(c.storeKey, &currentState, c.operationContext); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Errorf(
			"Unable to execute retransmit check. Unable to read current state from %s", c.storeKey,
		)
		return err
	}

	// Run the retransmission logic
	if err := c.retransmitMessageCore(currentState, timestamp, timerInterval); err != nil {
		log.WithError(err).WithFields(c.LogTags).WithField("state", c.operatingState).Error(
			"Unable to execute retransmit check. Retransmission core logic failed",
		)
		return err
	}
	return nil
}

// ----------------------------------------------------------------------------------------

// StopOperation stop controller
func (c *controllerImpl) StopOperation() error {
	c.contextCancel()
	return nil
}
