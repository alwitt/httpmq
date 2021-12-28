package common

import (
	"bytes"
	"testing"

	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestViperConfigParsing(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	validate := validator.New()

	// Case 0: parse config with no defaults in place
	{
		var cfg SystemConfig
		assert.Nil(viper.Unmarshal(&cfg))
		assert.NotNil(validate.Struct(&cfg))
	}

	// Case 1: load the configs
	{
		var cfg SystemConfig
		InstallDefaultConfigValues()
		assert.Nil(viper.Unmarshal(&cfg))
		assert.Nil(validate.Struct(&cfg))
	}

	// Case 2: invalid config
	{
		config := []byte(`---
dataplane:
  api_server:
    server_config:
      listen_on: 1243`)
		viper.SetConfigType("yaml")
		assert.Nil(viper.ReadConfig(bytes.NewBuffer(config)))
		var cfg SystemConfig
		assert.Nil(viper.Unmarshal(&cfg))
		assert.NotNil(validate.Struct(&cfg))
	}

	// Case 3: invalid config
	{
		config := []byte(`---
dataplane:
  api_server:
    server_config:
      write_timeout_sec: -10`)
		viper.SetConfigType("yaml")
		assert.Nil(viper.ReadConfig(bytes.NewBuffer(config)))
		var cfg SystemConfig
		assert.Nil(viper.Unmarshal(&cfg))
		assert.NotNil(validate.Struct(&cfg))
	}
}
