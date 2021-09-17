package dispatch

import (
	"context"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/go-playground/validator"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/storage"
)

// Dispatcher a message dispatch unit responsible for a particular message queue
type Dispatcher interface {
	Start() error
	Stop() error
	ReceivedACKs(msgIndexes []int64, ctxt context.Context) error
}

// dispatcherImpl implements Dispatcher
type dispatcherImpl struct {
	common.Component
	wg               *sync.WaitGroup
	operationContext context.Context
	contextCancel    context.CancelFunc
	// Components
	controlMod    Controller
	dispatchMod   MessageDispatch
	retransmitMod MessageRetransmit
	fetchMod      MessageFetch
	// Support task processors
	controlModTP    common.TaskProcessor
	dispatchModTP   common.TaskProcessor
	retransmitModTP common.TaskProcessor
	// Support message retransmit
	retransmitTimer common.IntervalTimer
	checkInterval   time.Duration
}

// DispatcherInitParam package up parameters needed to initialize a dispatcher
type DispatcherInitParam struct {
	// Info regarding connection
	Client      string `validate:"required"`
	Queue       string `validate:"required"`
	KeyPrefix   string `validate:"required"`
	MaxInflight int
	// Pipeline
	MessageForward              SubmitMessage
	DispatchUseBlockingRegister bool
	ReadQueueTimeout            time.Duration
	// Message retries
	MaxRetry           int
	RetryCheckInterval time.Duration
	// Operations
	WG                         *sync.WaitGroup
	RootContext                context.Context
	QueueInterface             storage.MessageQueues
	QueueReadFailureMaxRetries int
	StoreInterface             storage.KeyValueStore
}

// DefineDispatcher create new dispatcher unit
func DefineDispatcher(param DispatcherInitParam) (Dispatcher, error) {
	// validate input
	{
		validate := validator.New()
		if err := validate.Struct(&param); err != nil {
			log.WithError(err).WithField("module", "dispatch").Error(
				"Dispatcher init params not valid",
			)
			return nil, err
		}
	}

	logTags := log.Fields{
		"module": "dispatch", "client": param.Client, "queue": param.Queue,
	}
	ctxt, cancel := context.WithCancel(param.RootContext)

	// Define controller
	ctrlTP, err := common.GetNewTaskProcessorInstance(
		"DISP-CTRL", param.MaxInflight*2, ctxt,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error(
			"Failed to define controller task-processor",
		)
		cancel()
		return nil, err
	}
	ctrl, err := DefineController(
		param.Client,
		param.Queue,
		param.StoreInterface,
		param.QueueReadFailureMaxRetries,
		ctrlTP,
		param.KeyPrefix,
		param.MaxRetry,
		ctxt,
		nil,
		nil,
		nil,
		nil,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to define controller")
		cancel()
		return nil, err
	}

	// Define message dispatch
	dispatchTP, err := common.GetNewTaskProcessorInstance(
		"DISP-MSG-DISP", param.MaxInflight*2, ctxt,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error(
			"Failed to define dispatch task-processor",
		)
		cancel()
		return nil, err
	}
	dispatch, err := DefineMessageDispatch(
		param.Queue,
		dispatchTP,
		make(chan []int64, param.MaxInflight*2),
		make(chan MessageInFlight, param.MaxInflight),
		make(chan MessageInFlight, param.MaxInflight),
		0,
		param.MaxInflight,
		param.MessageForward,
		func(msgIdx int64, timestamp time.Time, useContext context.Context) error {
			return ctrl.RegisterInflight(
				msgIdx, timestamp, useContext, param.DispatchUseBlockingRegister,
			)
		},
		ctxt,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to define dispatch")
		cancel()
		return nil, err
	}

	// Define message retransmit
	retransmitTP, err := common.GetNewTaskProcessorInstance(
		"DISP-MSG-RETX", param.MaxInflight*2, ctxt,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error(
			"Failed to define retransmit task-processor",
		)
		cancel()
		return nil, err
	}
	retransmit, err := DefineMessageRetransmit(
		param.Queue,
		retransmitTP,
		param.QueueInterface,
		param.ReadQueueTimeout,
		dispatch.SubmitRetransmit,
		ctxt,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to define retransmit")
		cancel()
		return nil, err
	}
	retxTimer, err := common.GetIntervalTimerInstance("DISP-RETX", ctxt, param.WG)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to define retransmit-timer")
		cancel()
		return nil, err
	}

	// Define message fetch
	fetch, err := DefineMessageFetcher(
		param.Queue, param.WG, param.QueueInterface, dispatch.SubmitMessageToDeliver, ctxt,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to define fetch")
		cancel()
		return nil, err
	}

	// Update callback in controller
	ctrl.SetCallbacks(
		retransmit.RetransmitMessages,
		retransmit.ReceivedACKs,
		dispatch.SubmitMessageACK,
		fetch.StartReading,
	)

	return &dispatcherImpl{
		Component:        common.Component{LogTags: logTags},
		wg:               param.WG,
		operationContext: ctxt,
		contextCancel:    cancel,
		controlMod:       ctrl,
		dispatchMod:      dispatch,
		retransmitMod:    retransmit,
		fetchMod:         fetch,
		controlModTP:     ctrlTP,
		dispatchModTP:    dispatchTP,
		retransmitModTP:  retransmitTP,
		retransmitTimer:  retxTimer,
		checkInterval:    param.RetryCheckInterval,
	}, nil
}

// ----------------------------------------------------------------------------------------

// Start starts the various components of the dispatcher
func (d *dispatcherImpl) Start() error {
	log.WithFields(d.LogTags).Info("Starting all components")

	// Start dispatch
	if err := d.dispatchModTP.StartEventLoop(d.wg); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error(
			"Failed to start dispatch task-processor",
		)
		return err
	}

	// Start retransmit
	if err := d.retransmitModTP.StartEventLoop(d.wg); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error(
			"Failed to start retransmit task-processor",
		)
		return err
	}

	// Start controller
	if err := d.controlModTP.StartEventLoop(d.wg); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error(
			"Failed to start controller task-processor",
		)
		return err
	}

	// Initialize system
	if err := d.controlMod.Start(d.operationContext); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error(
			"Failed to perform initialization",
		)
		return err
	}

	// Start the timer
	if err := d.retransmitTimer.Start(d.checkInterval, func() error {
		return d.controlMod.TriggerMessageRetransmit(
			time.Now(), d.checkInterval, d.operationContext,
		)
	}, false); err != nil {
		log.WithError(err).WithFields(d.LogTags).Error(
			"Failed to start retransmit check timer",
		)
		return err
	}

	log.WithFields(d.LogTags).Info("All components running")
	return nil
}

// ----------------------------------------------------------------------------------------

// Stop stops the various components of the dispatcher
func (d *dispatcherImpl) Stop() error {
	log.WithFields(d.LogTags).Info("Stopping all components")
	d.contextCancel()
	return nil
}

// ----------------------------------------------------------------------------------------

// ReceivedACKs received ACK for certain messages
func (d *dispatcherImpl) ReceivedACKs(msgIndexes []int64, ctxt context.Context) error {
	return d.controlMod.ReceivedACKs(msgIndexes, ctxt)
}
