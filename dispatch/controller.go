package dispatch

import (
	"fmt"
	"time"

	"github.com/apex/log"
	"github.com/go-playground/validator"
	"gitlab.com/project-nan/httpmq/common"
	"gitlab.com/project-nan/httpmq/storage"
)

// ========================================================================================
// Controller controls the operation of the message dispatcher
type Controller interface {
}

// controllerImpl implements Controller
type controllerImpl struct {
	common.Component
	store              storage.KeyValueStore
	tp                 common.TaskProcessor
	client             string
	queue              string
	storeKey           string
	kvStoreCallTimeout time.Duration
	validate           *validator.Validate
}

// DefineController create new dispatch controller
func DefineController(
	client string,
	queue string,
	dataStore storage.KeyValueStore,
	tp common.TaskProcessor,
	keyPrefix string,
	timeout time.Duration,
) (Controller, error) {
	logTags := log.Fields{
		"module": "dispatch", "component": "controller", "client": client, "queue": queue,
	}
	instance := controllerImpl{
		Component:          common.Component{LogTags: logTags},
		store:              dataStore,
		tp:                 tp,
		client:             client,
		queue:              queue,
		storeKey:           fmt.Sprintf("%s/client/%s/queue/%s", keyPrefix, client, queue),
		kvStoreCallTimeout: timeout,
		validate:           validator.New(),
	}
	return &instance, nil
}
