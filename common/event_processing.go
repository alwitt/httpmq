package common

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/apex/log"
)

// TaskHandler a handler function which execute a task based on parameters
type TaskHandler func(taskParam interface{}) error

// TaskProcessor processing module for implementing an event loop model
type TaskProcessor interface {
	Submit(newTaskParam interface{}) error
	ProcessNewTaskParam(newTaskParam interface{}) error
	SetTaskExecutionMap(newMap map[reflect.Type]TaskHandler) error
	AddToTaskExecutionMap(theType reflect.Type, handler TaskHandler) error
	StartEventLoop(wg *sync.WaitGroup) error
	StopEventLoop() error
}

// taskProcessorImpl implement TaskProcessor
type taskProcessorImpl struct {
	Component
	name         string
	done         chan bool
	newTasks     chan interface{}
	executionMap map[reflect.Type]TaskHandler
}

// GetNewTaskProcessorInstance get instance of TaskProcessor
func GetNewTaskProcessorInstance(name string, taskBuffer int) (TaskProcessor, error) {
	logTags := log.Fields{
		"module": "common", "component": fmt.Sprintf("task-processor/%s", name),
	}
	return &taskProcessorImpl{
		Component:    Component{LogTags: logTags},
		name:         name,
		done:         make(chan bool),
		newTasks:     make(chan interface{}, taskBuffer),
		executionMap: make(map[reflect.Type]TaskHandler),
	}, nil
}

// Submit submit a new task parameter for processing
func (p *taskProcessorImpl) Submit(newTaskParam interface{}) error {
	log.WithFields(p.LogTags).Debug("Accepting new task param")
	p.newTasks <- newTaskParam
	return nil
}

// SetTaskExecutionMap update the task param to execution mapping
func (p *taskProcessorImpl) SetTaskExecutionMap(newMap map[reflect.Type]TaskHandler) error {
	log.WithFields(p.LogTags).Debug("Changing task execution mapping")
	p.executionMap = newMap
	return nil
}

// AddToTaskExecutionMap add a new entry to the task param to execution mapping
func (p *taskProcessorImpl) AddToTaskExecutionMap(theType reflect.Type, handler TaskHandler) error {
	log.WithFields(p.LogTags).Debugf("Appending to task execution mapping for %s", theType)
	p.executionMap[theType] = handler
	return nil
}

// StopEventLoop stop the task param processing event loop
func (p *taskProcessorImpl) StopEventLoop() error {
	log.WithFields(p.LogTags).Info("Stopping event loop")
	p.done <- true
	return nil
}

// ProcessNewTaskParam process a new task param
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

// StartEventLoop start the event loop
func (p *taskProcessorImpl) StartEventLoop(wg *sync.WaitGroup) error {
	log.WithFields(p.LogTags).Info("Starting event loop")
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.WithFields(p.LogTags).Info("Event loop exiting")
		finished := false
		for !finished {
			select {
			case complete, ok := <-p.done:
				if !ok {
					log.WithFields(p.LogTags).Error(
						"Event loop terminating. Failed to read done flag",
					)
					return
				}
				finished = complete
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

// ==============================================================================

// taskDemuxProcessorImpl implement TaskProcessor but support multiple parallel workers
type taskDemuxProcessorImpl struct {
	Component
	name     string
	input    TaskProcessor
	workers  []TaskProcessor
	routeIdx int
}

// GetNewTaskDemuxProcessorInstance get instance of TaskDemuxProcessor
func GetNewTaskDemuxProcessorInstance(
	name string, taskBuffer int, workerNum int,
) (TaskProcessor, error) {
	inputTP, err := GetNewTaskProcessorInstance(fmt.Sprintf("%s.input", name), taskBuffer)
	if err != nil {
		return nil, err
	}
	workers := make([]TaskProcessor, workerNum)
	for itr := 0; itr < workerNum; itr++ {
		workerTP, err := GetNewTaskProcessorInstance(
			fmt.Sprintf("%s.worker.%d", name, itr), taskBuffer,
		)
		if err != nil {
			return nil, err
		}
		workers[itr] = workerTP
	}
	logTags := log.Fields{
		"module": "common", "component": fmt.Sprintf("task-demux-processor/%s", name),
	}
	return &taskDemuxProcessorImpl{
		name:      name,
		input:     inputTP,
		workers:   workers,
		routeIdx:  0,
		Component: Component{LogTags: logTags},
	}, nil
}

// Submit submit a new task parameter for processing
func (p *taskDemuxProcessorImpl) Submit(newTaskParam interface{}) error {
	log.WithFields(p.LogTags).Debug("Accepting new task param")
	return p.input.Submit(newTaskParam)
}

// ProcessNewTaskParam given a new task, process task parameter
func (p *taskDemuxProcessorImpl) ProcessNewTaskParam(newTaskParam interface{}) error {
	if p.workers != nil && len(p.workers) > 0 {
		log.WithFields(p.LogTags).Debugf("Processing new %s", reflect.TypeOf(newTaskParam))
		defer func() { p.routeIdx = (p.routeIdx + 1) % len(p.workers) }()
		return p.workers[p.routeIdx].Submit(newTaskParam)
	}
	return fmt.Errorf("[TDP %s] No workers defined", p.name)
}

// SetTaskExecutionMap update the task execution map for all workers
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

// AddToTaskExecutionMap add a new entry to the task param to execution mapping
func (p *taskDemuxProcessorImpl) AddToTaskExecutionMap(
	theType reflect.Type, handler TaskHandler,
) error {
	for _, worker := range p.workers {
		_ = worker.AddToTaskExecutionMap(theType, handler)
	}
	// Do the same for input
	return p.input.AddToTaskExecutionMap(theType, p.ProcessNewTaskParam)
}

// StartEventLoop start the event loop
func (p *taskDemuxProcessorImpl) StartEventLoop(wg *sync.WaitGroup) error {
	log.WithFields(p.LogTags).Info("Starting event loops")
	// Start the worker loops first
	for _, worker := range p.workers {
		_ = worker.StartEventLoop(wg)
	}
	// Start the input loop
	return p.input.StartEventLoop(wg)
}

// StopEventLoop stop the task param processing event loop
func (p *taskDemuxProcessorImpl) StopEventLoop() error {
	log.WithFields(p.LogTags).Info("Stopping event loop")
	// Stop the input loop
	_ = p.input.StopEventLoop()
	// Stop the worker loops
	for _, worker := range p.workers {
		_ = worker.StopEventLoop()
	}
	return nil
}
