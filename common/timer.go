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

package common

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apex/log"
)

// TimeoutHandler callback function signature called timer timeout
type TimeoutHandler func() error

// IntervalTimer is a support interface for triggering events at specific intervals
type IntervalTimer interface {
	// Start starts timer with a specific timeout interval, and the callback to trigger on timeout.
	// If oneShort, cancel after first timeout.
	Start(interval time.Duration, handler TimeoutHandler, oneShort bool) error
	// Stop stops the timer
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
	if rootCtxt.Value(RequestParam{}) != nil {
		v, ok := rootCtxt.Value(RequestParam{}).(RequestParam)
		if ok {
			v.UpdateLogTags(logTags)
		}
	}
	return &intervalTimerImpl{
		Component:        Component{LogTags: logTags},
		rootContext:      rootCtxt,
		operationContext: nil,
		contextCancel:    nil,
		wg:               wg,
	}, nil
}

// Start starts timer with a specific timeout interval, and the callback to trigger on timeout.
// If oneShort, cancel after first timeout.
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

// Stop stops the timer
func (t *intervalTimerImpl) Stop() error {
	if t.contextCancel != nil {
		log.WithFields(t.LogTags).Info("Stopping timer loop")
		t.contextCancel()
	}
	return nil
}

// ========================================================================================

// Sequencer is a helper interface for returning a sequence of numbers
type Sequencer interface {
	// NextValue returns the next value in the sequence
	NextValue() float64
}

// exponentialSequence is a helper interface to get an exponential sequence from a
// starting value
type exponentialSequence struct {
	current    float64
	growthRate float64
}

// NextValue returns the next value in the sequence
func (s *exponentialSequence) NextValue() float64 {
	nextValue := s.current * s.growthRate
	s.current = nextValue
	return nextValue
}

// GetExponentialSeq define an exponential sequencer
func GetExponentialSeq(initial float64, growthRate float64) (Sequencer, error) {
	if growthRate < 1.0 {
		return nil, fmt.Errorf("growth rate of exponential sequence must be > 1.0")
	}
	return &exponentialSequence{current: initial, growthRate: growthRate}, nil
}
