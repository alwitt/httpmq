package common

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
)

func TestTaskParamProcessing(t *testing.T) {
	assert := assert.New(t)

	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()
	uut, err := GetNewTaskProcessorInstance("testing", 4, ctxt)
	defer func() {
		assert.Nil(uut.StopEventLoop())
	}()
	assert.Nil(err)

	// Case 1: no executor map
	{
		assert.NotNil(uut.ProcessNewTaskParam("hello"))
	}

	type testStruct1 struct{}
	type testStruct2 struct{}
	type testStruct3 struct{}

	executorMap := map[reflect.Type]TaskHandler{
		reflect.TypeOf(testStruct1{}): func(p interface{}) error {
			return nil
		},
	}

	// Case 2: define a executor map
	{
		assert.Nil(uut.SetTaskExecutionMap(executorMap))
		assert.Nil(uut.ProcessNewTaskParam(testStruct1{}))
		assert.NotNil(uut.ProcessNewTaskParam(testStruct2{}))
		assert.NotNil(uut.ProcessNewTaskParam(&testStruct3{}))
	}

	executorMap = map[reflect.Type]TaskHandler{
		reflect.TypeOf(testStruct1{}): func(p interface{}) error { return nil },
		reflect.TypeOf(testStruct3{}): func(p interface{}) error { return fmt.Errorf("Dummy error") },
	}

	// Case 3: change executor map
	{
		assert.Nil(uut.SetTaskExecutionMap(executorMap))
		assert.Nil(uut.ProcessNewTaskParam(testStruct1{}))
		assert.NotNil(uut.ProcessNewTaskParam(&testStruct2{}))
		assert.NotNil(uut.ProcessNewTaskParam(testStruct3{}))
	}

	// Case 4: append to existing map
	{
		assert.Nil(uut.AddToTaskExecutionMap(
			reflect.TypeOf(&testStruct2{}), func(p interface{}) error { return nil },
		))
		assert.Nil(uut.ProcessNewTaskParam(testStruct1{}))
		assert.Nil(uut.ProcessNewTaskParam(&testStruct2{}))
		assert.NotNil(uut.ProcessNewTaskParam(testStruct3{}))
	}
}

func TestTaskDemuxProcessing(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()
	uut, err := GetNewTaskDemuxProcessorInstance("testing", 4, 3, time.Second, ctxt)
	defer func() {
		assert.Nil(uut.StopEventLoop())
	}()
	assert.Nil(err)

	// recast to source
	uutc := uut.(*taskDemuxProcessorImpl)
	assert.Equal(0, uutc.routeIdx)

	// start the built in processes
	assert.Nil(uut.StartEventLoop(&wg))

	path1 := 0
	path2 := 0
	path3 := 0

	type testStruct1 struct{}
	type testStruct2 struct{}
	type testStruct3 struct{}

	testWG := sync.WaitGroup{}
	pathCB1 := func(p interface{}) error {
		path1++
		testWG.Done()
		return nil
	}
	pathCB2 := func(p interface{}) error {
		path2++
		testWG.Done()
		return nil
	}
	pathCB3 := func(p interface{}) error {
		path3++
		testWG.Done()
		return nil
	}

	executorMap := map[reflect.Type]TaskHandler{
		reflect.TypeOf(testStruct1{}): pathCB1,
		reflect.TypeOf(testStruct2{}): pathCB2,
		reflect.TypeOf(testStruct3{}): pathCB3,
	}

	assert.Nil(uut.SetTaskExecutionMap(executorMap))

	// Case 1: trigger
	{
		testWG.Add(1)
		useContext, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Submit(testStruct1{}, useContext))
		cancel()
		testWG.Wait()
		assert.Equal(1, path1)
		assert.Equal(1, uutc.routeIdx)
	}

	// Case 2: trigger
	{
		testWG.Add(1)
		useContext, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Submit(testStruct1{}, useContext))
		cancel()
		testWG.Wait()
		assert.Equal(2, path1)
		assert.Equal(2, uutc.routeIdx)
	}

	// Case 3: trigger back to back
	{
		testWG.Add(2)
		useContext, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Submit(testStruct2{}, useContext))
		cancel()
		useContext, cancel = context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Submit(testStruct3{}, useContext))
		cancel()
		testWG.Wait()
		assert.Equal(1, path2)
		assert.Equal(1, path3)
		assert.Equal(1, uutc.routeIdx)
	}
}
