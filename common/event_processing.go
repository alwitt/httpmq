package common

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/apex/log"
)

// TaskHandler is the function signature of callback used to process an user task
type TaskHandler func(taskParam interface{}) error

// TaskProcessor implements an event loop model where tasks are processed by a daemon thread
type TaskProcessor interface {
	// Submit submits a task-parameter to be processed
	Submit(newTaskParam interface{}, ctx context.Context) error
	// ProcessNewTaskParam execute a submitted task-parameter
	ProcessNewTaskParam(newTaskParam interface{}) error
	// SetTaskExecutionMap update the mapping between task-parameter object and its associated
	// handler function.
	//
	// The task-parameter object contains information need to execute a particular task. When
	// a user wants to execute a task, the user is submitting a task-parameter object via Submit.
	// The module finds the associated handler function and calls it with the task-parameter object.
	SetTaskExecutionMap(newMap map[reflect.Type]TaskHandler) error
	// AddToTaskExecutionMap add new (task-parameter, handler function) mapping to the existing set.
	AddToTaskExecutionMap(theType reflect.Type, handler TaskHandler) error
	// StartEventLoop starts the daemon thread for processing the submitted task-parameters
	StartEventLoop(wg *sync.WaitGroup) error
	// StopEventLoop stops the daemon thread
	StopEventLoop() error
}

// taskProcessorImpl implements TaskProcessor which uses only one daemon thread
type taskProcessorImpl struct {
	Component
	name             string
	operationContext context.Context
	contextCancel    context.CancelFunc
	newTasks         chan interface{}
	executionMap     map[reflect.Type]TaskHandler
}

// GetNewTaskProcessorInstance get instance of taskProcessorImpl
func GetNewTaskProcessorInstance(
	name string, taskBuffer int, ctxt context.Context,
) (TaskProcessor, error) {
	logTags := log.Fields{
		"module": "common", "component": "task-processor", "instance": name,
	}
	if ctxt.Value(RequestParam{}) != nil {
		v, ok := ctxt.Value(RequestParam{}).(RequestParam)
		if ok {
			v.UpdateLogTags(logTags)
		}
	}
	optCtxt, cancel := context.WithCancel(ctxt)
	return &taskProcessorImpl{
		Component:        Component{LogTags: logTags},
		name:             name,
		operationContext: optCtxt,
		contextCancel:    cancel,
		newTasks:         make(chan interface{}, taskBuffer),
		executionMap:     make(map[reflect.Type]TaskHandler),
	}, nil
}

// Submit submits a task to be processed
func (p *taskProcessorImpl) Submit(newTaskParam interface{}, ctx context.Context) error {
	select {
	case p.newTasks <- newTaskParam:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.operationContext.Done():
		return p.operationContext.Err()
	}
}

// SetTaskExecutionMap update the mapping between task-parameter object and it associated
// handler function.
//
// The task-parameter object contains information need to execute a particular task. When
// a user wants to execute a task, the user is submitting a task-parameter object via Submit.
// The module finds the associated handler function and calls it with the task-parameter object.
func (p *taskProcessorImpl) SetTaskExecutionMap(newMap map[reflect.Type]TaskHandler) error {
	log.WithFields(p.LogTags).Debug("Changing task execution mapping")
	p.executionMap = newMap
	return nil
}

// AddToTaskExecutionMap add new (task-parameter, handler function) mapping to the existing set.
func (p *taskProcessorImpl) AddToTaskExecutionMap(theType reflect.Type, handler TaskHandler) error {
	log.WithFields(p.LogTags).Debugf("Appending to task execution mapping for %s", theType)
	p.executionMap[theType] = handler
	return nil
}

// StartEventLoop starts the daemon thread for processing the submitted task-parameters
func (p *taskProcessorImpl) ProcessNewTaskParam(newTaskParam interface{}) error {
	if p.executionMap != nil && len(p.executionMap) > 0 {
		log.WithFields(p.LogTags).Debugf("Processing new %s", reflect.TypeOf(newTaskParam))
		// Process task based on the parameter type
		if theHandler, ok := p.executionMap[reflect.TypeOf(newTaskParam)]; ok {
			return theHandler(newTaskParam)
		}
		return fmt.Errorf(
			"[TP %s] No matching handler found for %s", p.name, reflect.TypeOf(newTaskParam),
		)
	}
	return fmt.Errorf("[TP %s] No task execution mapping set", p.name)
}

// StartEventLoop starts the daemon thread for processing the submitted task-parameters
func (p *taskProcessorImpl) StartEventLoop(wg *sync.WaitGroup) error {
	log.WithFields(p.LogTags).Info("Starting event loop")
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.WithFields(p.LogTags).Info("Event loop exiting")
		finished := false
		for !finished {
			select {
			case <-p.operationContext.Done():
				finished = true
			case newTaskParam, ok := <-p.newTasks:
				if !ok {
					log.WithFields(p.LogTags).Error(
						"Event loop terminating. Failed to read new task param",
					)
					return
				}
				if err := p.ProcessNewTaskParam(newTaskParam); err != nil {
					log.WithError(err).WithFields(p.LogTags).Error("Failed to process new task param")
				}
			}
		}
	}()
	return nil
}

// StopEventLoop stops the daemon thread
func (p *taskProcessorImpl) StopEventLoop() error {
	p.contextCancel()
	return nil
}

// ==============================================================================

// taskDemuxProcessorImpl implement TaskProcessor but support multiple parallel workers
type taskDemuxProcessorImpl struct {
	Component
	name             string
	input            TaskProcessor
	workers          []TaskProcessor
	routeIdx         int
	operationContext context.Context
	contextCancel    context.CancelFunc
}

// GetNewTaskDemuxProcessorInstance get instance of taskDemuxProcessorImpl
func GetNewTaskDemuxProcessorInstance(
	name string,
	taskBuffer int,
	workerNum int,
	passTimeout time.Duration,
	ctxt context.Context,
) (TaskProcessor, error) {
	inputTP, err := GetNewTaskProcessorInstance(
		fmt.Sprintf("%s.input", name), taskBuffer, ctxt,
	)
	if err != nil {
		return nil, err
	}
	optCtxt, cancel := context.WithCancel(ctxt)
	workers := make([]TaskProcessor, workerNum)
	for itr := 0; itr < workerNum; itr++ {
		workerTP, err := GetNewTaskProcessorInstance(
			fmt.Sprintf("%s.worker.%d", name, itr), taskBuffer, optCtxt,
		)
		if err != nil {
			cancel()
			return nil, err
		}
		workers[itr] = workerTP
	}
	logTags := log.Fields{
		"module": "common", "component": "task-demux-processor/%s", "instance": name,
	}
	if ctxt.Value(RequestParam{}) != nil {
		v, ok := ctxt.Value(RequestParam{}).(RequestParam)
		if ok {
			v.UpdateLogTags(logTags)
		}
	}
	return &taskDemuxProcessorImpl{
		name:             name,
		input:            inputTP,
		workers:          workers,
		routeIdx:         0,
		operationContext: optCtxt,
		contextCancel:    cancel,
		Component:        Component{LogTags: logTags},
	}, nil
}

// Submit submits a task-parameter to be processed
func (p *taskDemuxProcessorImpl) Submit(newTaskParam interface{}, ctx context.Context) error {
	return p.input.Submit(newTaskParam, ctx)
}

// ProcessNewTaskParam execute a submitted task-parameter
func (p *taskDemuxProcessorImpl) ProcessNewTaskParam(newTaskParam interface{}) error {
	if p.workers != nil && len(p.workers) > 0 {
		log.WithFields(p.LogTags).Debugf("Processing new %s", reflect.TypeOf(newTaskParam))
		defer func() { p.routeIdx = (p.routeIdx + 1) % len(p.workers) }()
		return p.workers[p.routeIdx].Submit(newTaskParam, p.operationContext)
	}
	return fmt.Errorf("[TDP %s] No workers defined", p.name)
}

// SetTaskExecutionMap update the mapping between task-parameter object and its associated
// handler function.
//
// The task-parameter object contains information need to execute a particular task. When
// a user wants to execute a task, the user is submitting a task-parameter object via Submit.
// The module finds the associated handler function and calls it with the task-parameter object.
func (p *taskDemuxProcessorImpl) SetTaskExecutionMap(newMap map[reflect.Type]TaskHandler) error {
	for _, worker := range p.workers {
		_ = worker.SetTaskExecutionMap(newMap)
	}
	// Create a different version of the input to route to worker
	inputMap := map[reflect.Type]TaskHandler{}
	for msgType := range newMap {
		inputMap[msgType] = p.ProcessNewTaskParam
	}
	return p.input.SetTaskExecutionMap(inputMap)
}

// AddToTaskExecutionMap add new (task-parameter, handler function) mapping to the existing set.
func (p *taskDemuxProcessorImpl) AddToTaskExecutionMap(
	theType reflect.Type, handler TaskHandler,
) error {
	for _, worker := range p.workers {
		_ = worker.AddToTaskExecutionMap(theType, handler)
	}
	// Do the same for input
	return p.input.AddToTaskExecutionMap(theType, p.ProcessNewTaskParam)
}

// StartEventLoop starts the daemon thread for processing the submitted task-parameters
func (p *taskDemuxProcessorImpl) StartEventLoop(wg *sync.WaitGroup) error {
	log.WithFields(p.LogTags).Info("Starting event loops")
	// Start the worker loops first
	for _, worker := range p.workers {
		_ = worker.StartEventLoop(wg)
	}
	// Start the input loop
	return p.input.StartEventLoop(wg)
}

// StopEventLoop stops the daemon thread
func (p *taskDemuxProcessorImpl) StopEventLoop() error {
	p.contextCancel()
	return nil
}
