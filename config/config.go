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
}

// Kafka holds Kafka related configuration
type Kafka struct {
	Brokers []string `env:"KAFKA_HOSTS"`
	Port    string   `env:"KAFKA_PORT"`
	Topic   Topic

	RetryMax       int `env:"KAFKA_RETRY_MAX"`
	RetryBackoff   int `env:"KAFKA_RETRY_BACKOFF_MS"`
	RepartitionMax int `env:"KAFKA_REPARTITION_MAX"`
}

type Topic struct {
	LogMessage    string `env:"KAFKA_TOPIC_LOG_MESSAGE"`
	LogMessageFmt string `env:"KAFKA_TOPIC_LOG_MESSAGE_FMT"`
	ValueMetric   string `env:"KAFKA_TOPIC_VALUE_METRIC"`
}

// LoadConfig reads configuration file
func LoadConfig(path string) (*Config, error) {

	cf := CF{}
	env.Parse(&cf)

	goredisclient := GoRedisClient{}
	env.Parse(&goredisclient)

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
	}

	env.Parse(&config)

	return &config, nil
}
