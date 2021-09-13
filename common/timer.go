package common

import (
	"context"
	"sync"
	"time"

	"github.com/apex/log"
)

// TimeoutHandler handler callback on timeout
type TimeoutHandler func() error

// IntervalTimer support class for triggering events at specific intervals
type IntervalTimer interface {
	Start(interval time.Duration, handler TimeoutHandler, oneShort bool) error
	Stop() error
}

// intervalTimerImpl implements IntervalTimer
type intervalTimerImpl struct {
	Component
	rootContext      context.Context
	operationContext context.Context
	contextCancel    context.CancelFunc
	wg               *sync.WaitGroup
}

// GetIntervalTimerInstance create new interval timer instance
func GetIntervalTimerInstance(
	name string, rootCtxt context.Context, wg *sync.WaitGroup,
) (IntervalTimer, error) {
	logTags := log.Fields{
		"module": "common", "component": "interval-timer", "instance": name,
	}
	return &intervalTimerImpl{
		Component:        Component{LogTags: logTags},
		rootContext:      rootCtxt,
		operationContext: nil,
		contextCancel:    nil,
		wg:               wg,
	}, nil
}

// Start start the interval timer
func (t *intervalTimerImpl) Start(
	interval time.Duration, handler TimeoutHandler, oneShot bool,
) error {
	log.WithFields(t.LogTags).Infof("Starting with int %s", interval)
	t.wg.Add(1)
	ctxt, cancel := context.WithCancel(t.rootContext)
	t.operationContext = ctxt
	t.contextCancel = cancel
	go func() {
		defer t.wg.Done()
		defer log.WithFields(t.LogTags).Info("Timer loop exiting")
		finished := false
		for !finished {
			select {
			case <-t.operationContext.Done():
				finished = true
			case <-time.After(interval):
				log.WithFields(t.LogTags).Debug("Calling handler")
				if err := handler(); err != nil {
					log.WithError(err).WithFields(t.LogTags).Error("Handler failed")
				}
				if oneShot {
					return
				}
			}
		}
	}()
	return nil
}

// Stop stop the interval timer
func (t *intervalTimerImpl) Stop() error {
	if t.contextCancel != nil {
		log.WithFields(t.LogTags).Info("Stopping timer loop")
		t.contextCancel()
	}
	return nil
}
