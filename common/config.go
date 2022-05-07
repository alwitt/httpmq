package common

import "github.com/spf13/viper"

// ===============================================================================
// NATS Related Config

// NATSReconnectConfig defines reconnect parameters
type NATSReconnectConfig struct {
	// MaxAttempts sets the max number of reconnect attempts (-1 is unlimited)
	MaxAttempts int `mapstructure:"max_attempts" json:"max_attempts" validate:"gte=-1"`
	// WaitInterval is the duration between reconnect attempts in seconds
	WaitInterval int `mapstructure:"wait_interval_sec" json:"wait_interval_sec" validate:"gte=1"`
}

// NATSConfig defines parameters for connecting to NATS server
type NATSConfig struct {
	// ServerURI is the NATS connection URI
	ServerURI string `mapstructure:"server_uri" json:"server_uri" validate:"required,uri"`
	// ConnectTimeout is the max duration for connecting to NATS server in seconds
	ConnectTimeout int `mapstructure:"connect_timeout_sec" json:"connect_timeout_sec" validate:"gte=1"`
	// Reconnect defines reconnect parameters
	Reconnect NATSReconnectConfig `mapstructure:"reconnect" json:"reconnect" validate:"required,dive"`
}

// ===============================================================================
// HTTP Related Config

// HTTPServerConfig defines the HTTP server parameters
type HTTPServerConfig struct {
	// ListenOn is the interface the HTTP server will listen on
	ListenOn string `mapstructure:"listen_on" json:"listen_on" validate:"required,ip"`
	// Port is the port the HTTP server will listen on
	Port uint16 `mapstructure:"listen_port" json:"listen_port" validate:"required,gt=0,lt=65536"`
	// ReadTimeout is the maximum duration for reading the entire
	// request, including the body in seconds. A zero or negative
	// value means there will be no timeout.
	ReadTimeout int `mapstructure:"read_timeout_sec" json:"read_timeout_sec" validate:"gte=0"`
	// WriteTimeout is the maximum duration before timing out
	// writes of the response in seconds. A zero or negative value
	// means there will be no timeout.
	WriteTimeout int `mapstructure:"write_timeout_sec" json:"write_timeout_sec" validate:"gte=0"`
	// IdleTimeout is the maximum amount of time to wait for the
	// next request when keep-alives are enabled in seconds. If
	// IdleTimeout is zero, the value of ReadTimeout is used. If
	// both are zero, there is no timeout.
	IdleTimeout int `mapstructure:"idle_timeout_sec" json:"idle_timeout_sec" validate:"gte=0"`
}

// HTTPRequestLogging defines HTTP request logging parameters
type HTTPRequestLogging struct {
	// RequestIDHeader is the HTTP header containing the API request ID
	RequestIDHeader string `mapstructure:"request_id_header" json:"request_id_header"`
	// DoNotLogHeaders is the list of headers to not include in logging metadata
	DoNotLogHeaders []string `mapstructure:"do_not_log_headers" json:"do_not_log_headers"`
}

// HTTPConfig defines HTTP API / server parameters
type HTTPConfig struct {
	// Server defines HTTP server parameters
	Server HTTPServerConfig `mapstructure:"server_config" json:"server_config" validate:"required,dive"`
	// Logging defines operation logging parameters
	Logging HTTPRequestLogging `mapstructure:"logging_config" json:"logging_config" validate:"required,dive"`
}

// ===============================================================================
// Management Server Related Config

// ManagementEndpointConfig defines management API endpoint config
type ManagementEndpointConfig struct {
	// PathPrefix is the end-point path prefix for the management APIs
	PathPrefix string `mapstructure:"path_prefix" json:"path_prefix" validate:"required"`
}

// ManagementServerConfig defines configuration for the management API server
type ManagementServerConfig struct {
	// HTTPSetting is the HTTP API / server parameters for the management API server
	HTTPSetting HTTPConfig `mapstructure:"api_server" json:"api_server" validate:"required,dive"`
	// Endpoints is the API endpoint config parameters for the management API server
	Endpoints ManagementEndpointConfig `mapstructure:"endpoint_config" json:"endpoint_config" validate:"required,dive"`
}

// ===============================================================================
// Dataplane Server Related Config

// DataplaneEndpointConfig defines dataplane API endpoint config
type DataplaneEndpointConfig struct {
	// PathPrefix is the end-point path prefix for the dataplane APIs
	PathPrefix string `mapstructure:"path_prefix" json:"path_prefix" validate:"required"`
}

// DataplaneServerConfig defines configuration for the dataplane API server
type DataplaneServerConfig struct {
	// HTTPSetting is the HTTP API / server parameters for the dataplane API server
	HTTPSetting HTTPConfig `mapstructure:"api_server" json:"api_server" validate:"required,dive"`
	// Endpoints is the API endpoint config parameters for the dataplane API server
	Endpoints DataplaneEndpointConfig `mapstructure:"endpoint_config" json:"endpoint_config" validate:"required,dive"`
}

// ===============================================================================
// Complete Config

// SystemConfig defines the complete system config used by either management or dataplane server
type SystemConfig struct {
	// NATS are the NATS related config parameters
	NATS NATSConfig `mapstructure:"nats" json:"nats" validate:"required,dive"`
	// Management are the management API server configs
	Management *ManagementServerConfig `mapstructure:"management,omitempty" json:"management,omitempty" validate:"omitempty,dive"`
	// Dataplane are the dataplane API server configs
	Dataplane *DataplaneServerConfig `mapstructure:"dataplane,omitempty" json:"dataplane,omitempty" validate:"omitempty,dive"`
}

// ===============================================================================

// InstallDefaultConfigValues installs default config parameters in viper
func InstallDefaultConfigValues() {
	// Default NATS settings
	viper.SetDefault("nats.server_uri", "nats://127.0.0.1:4222")
	viper.SetDefault("nats.connect_timeout_sec", 30)
	viper.SetDefault("nats.reconnect.max_attempts", -1)
	viper.SetDefault("nats.reconnect.wait_interval_sec", 15)

	// Default Management server settings
	viper.SetDefault("management.endpoint_config.path_prefix", "/")
	viper.SetDefault("management.api_server.server_config.listen_on", "0.0.0.0")
	viper.SetDefault("management.api_server.server_config.listen_port", 3000)
	viper.SetDefault("management.api_server.server_config.read_timeout_sec", 60)
	viper.SetDefault("management.api_server.server_config.write_timeout_sec", 60)
	viper.SetDefault("management.api_server.server_config.idle_timeout_sec", 600)
	viper.SetDefault(
		"management.api_server.logging_config.request_id_header", "Httpmq-Request-ID",
	)
	viper.SetDefault(
		"management.api_server.logging_config.do_not_log_headers", []string{
			"WWW-Authenticate", "Authorization", "Proxy-Authenticate", "Proxy-Authorization",
		},
	)

	// Default Dataplane server settings
	viper.SetDefault("dataplane.endpoint_config.path_prefix", "/")
	viper.SetDefault("dataplane.api_server.server_config.listen_on", "0.0.0.0")
	viper.SetDefault("dataplane.api_server.server_config.listen_port", 3001)
	viper.SetDefault("dataplane.api_server.server_config.read_timeout_sec", 60)
	viper.SetDefault("dataplane.api_server.server_config.write_timeout_sec", 60)
	viper.SetDefault("dataplane.api_server.server_config.idle_timeout_sec", 600)
	viper.SetDefault(
		"dataplane.api_server.logging_config.request_id_header", "Httpmq-Request-ID",
	)
	viper.SetDefault(
		"dataplane.api_server.logging_config.do_not_log_headers", []string{
			"WWW-Authenticate", "Authorization", "Proxy-Authenticate", "Proxy-Authorization",
		},
	)
}
