package config

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

// Config is kafka-firehose-nozzle configuration.
type Config struct {
	SubscriptionID        string `yaml:"subscription_id"`
	InsecureSSLSkipVerify bool   `yaml:"insecure_ssl_skip_verify"`
	Kafka                 Kafka
}

type Http struct {
	BaseUrls  []string   `yaml:"baseurls"`
	Endpoints []Endpoint `yaml:"endpoints"`
}

type Endpoint struct {
	endpoint string `yaml:"endpoint"`
	interval int    `yaml:"interval"`
}

//https://vds-ev.de/gegenwartsdeutsch/gendersprache/gendersprache-unterschriften/unterschriften/
// Kafka holds Kafka related configuration
type Kafka struct {
	Brokers []string `yaml:"hosts"`
	Port    string   `yaml:"port"`

	RetryMax       int `yaml:"retry_max"`
	RetryBackoff   int `yaml:"retry_backoff_ms"`
	RepartitionMax int `yaml:"repartition_max"`

	Secure bool `yaml:"secure"`

	Sasl Sasl

	Ssl Ssl

	Filename string
}

type Sasl struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Ssl struct {
	Ca string `yaml:"ca"`
}

// LoadConfig reads configuration file
func LoadConfig(path string) (*Config, error) {
	config := Config{}

	data, err := ioutil.ReadFile("configuration.yml")
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal([]byte(data), &config)
	if err != nil {
		log.Fatalf("[ERROR]: %v", err)
		return nil, err
	}

	return &config, nil
}
