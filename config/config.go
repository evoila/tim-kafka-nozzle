package config

import (
	"github.com/caarlos0/env"
)

// Config is kafka-firehose-nozzle configuration.
type Config struct {
	SubscriptionID        string `env:"SUBSCRIPTION_ID"`
	InsecureSSLSkipVerify bool   `env:"INSECURE_SSL_SKIP_VERIFY"`
	Kafka                 Kafka
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
	// Create me
}

// LoadConfig reads configuration file
func LoadConfig(path string) (*Config, error) {

	topic := Topic{}
	env.Parse(&topic)

	kafka := Kafka{
		Topic: topic,
	}
	env.Parse(&kafka)

	config := Config{
		Kafka: kafka,
	}

	env.Parse(&config)

	return &config, nil
}
