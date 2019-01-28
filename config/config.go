package config

import (
	"github.com/caarlos0/env"
)

// Config is kafka-firehose-nozzle configuration.
type Config struct {
	SubscriptionID        string `env:"SUBSCRIPTION_ID"`
	InsecureSSLSkipVerify bool   `env:"INSECURE_SSL_SKIP_VERIFY"`
	CF                    CF
	Kafka                 Kafka
	GoRedisClient         GoRedisClient
	GoCfClient            GoCfClient
}

// CF holds CloudFoundry related configuration.
type CF struct {
	// dopplerAddr is doppler firehose address.
	// It must start with `ws://` or `wss://` schema because this is websocket.
	DopplerAddr string `env:"CF_DOPPLER_ADDRESS"`

	// UAAAddr is UAA server address.
	UAAAddr string `env:"CF_UAA_ADDRESS"`

	// Username is the username which can has scope of `doppler.firehose`.
	Username string `env:"CF_USERNAME"`
	Password string `env:"CF_PASSWORD"`
	Token    string `env:"CF_TOKEN"`

	// Firehose configuration
	IdleTimeout int `env:"CF_FIREHOSE_IDLE_TIMEOUT"` // seconds
}

type GoRedisClient struct {
	Addrs    []string `env:"REDIS_HOSTS"`
	Port     string   `env:"REDIS_PORT"`
	Password string   `env:"REDIS_PASSWORD"`
	DB       int      `env:"REDIS_DB"`
}

type GoCfClient struct {
	Api      string `env:"GO_CF_API"`
	Username string `env:"GO_CF_USERNAME"`
	Password string `env:"GO_CF_PASSWORD"`
}

// Kafka holds Kafka related configuration
type Kafka struct {
	Brokers []string `env:"KAFKA_HOSTS"`
	Port    string   `env:"KAFKA_PORT"`
	Topic   Topic

	RetryMax       int `env:"KAFKA_RETRY_MAX"`
	RetryBackoff   int `env:"KAFKA_RETRY_BACKOFF_MS"`
	RepartitionMax int `env:"KAFKA_REPARTITION_MAX"`

	Secure       bool   `env:"KAFKA_SECURE"`
	SaslUsername string `env:"KAFKA_SASL_USERNAME"`
	SaslPassword string `env:"KAFKA_SASL_PASSWORD"`

	SslCa          string `env:"KAFKA_SSL_CA"`
	SslCertificate string `env:"KAFKA_SSL_CERTIFICATE"`
	SslKey         string `env:"KAFKA_SSL_KEY"`
}

type Topic struct {
	BindingsTopic             string `env:"KAFKA_BINDINGS_TOPIC"`
	LogMessage                string `env:"KAFKA_TOPIC_LOG_MESSAGE"`
	AutoscalerContainerMetric string `env:"KAFKA_TOPIC_AUTOSCALER_CONTAINER_METRIC"`
	LogMetricContainerMetric  string `env:"KAFKA_TOPIC_LOG_METRIC_CONTAINER_METRIC"`
	HttpMetric                string `env:"KAFKA_TOPIC_HTTP_METRIC"`
}

// LoadConfig reads configuration file
func LoadConfig(path string) (*Config, error) {

	cf := CF{}
	env.Parse(&cf)

	goredisclient := GoRedisClient{}
	env.Parse(&goredisclient)

	gocfclient := GoCfClient{}
	env.Parse(&gocfclient)

	topic := Topic{}
	env.Parse(&topic)

	kafka := Kafka{
		Topic: topic,
	}
	env.Parse(&kafka)

	config := Config{
		CF:            cf,
		Kafka:         kafka,
		GoRedisClient: goredisclient,
		GoCfClient:    gocfclient,
	}

	env.Parse(&config)

	return &config, nil
}
