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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIntervalTimerOneShot(t *testing.T) {
	assert := assert.New(t)

	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()
	uut, err := GetIntervalTimerInstance(ctxt, "testing", &wg)
	assert.Nil(err)

	value := 0
	callback := func() error {
		value++
		return nil
	}

	assert.Nil(uut.Start(time.Millisecond*100, callback, true))
	time.Sleep(time.Millisecond * 150)
	assert.Equal(1, value)

	time.Sleep(time.Millisecond * 100)
	assert.Equal(1, value)

	assert.Nil(uut.Start(time.Millisecond*50, callback, true))
	time.Sleep(time.Millisecond * 60)
	assert.Equal(2, value)
}

func TestExponentialSeq(t *testing.T) {
	assert := assert.New(t)

	uut, err := GetExponentialSeq(float64(time.Second), 1.25)
	assert.Nil(err)
	uutCast, ok := uut.(*exponentialSequence)
	assert.True(ok)

	assert.InDelta(float64(time.Second), uutCast.current, 1e-3)
	val1 := uut.NextValue()
	assert.Greater(val1, float64(time.Second))
	val2 := uut.NextValue()
	assert.Greater(val2, val1)
}
