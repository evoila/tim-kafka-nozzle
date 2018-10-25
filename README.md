# osb-autoscaler-kafka-nozzle 


This project is a fork from [kafka-firehose-nozzle](https://github.com/rakutentech/kafka-firehose-nozzle) and changes the output from JSON to protobuf. It is part of the [osb-autoscaler-framework](https://github.com/evoila/osb-autoscaler-core).

Unlike the nozzle from rakutentech this nozzle publishes `ContainerMetric` and `HttpStartStop` events to two seperate topics for all applications.

*NOTE*: Currently kafka topics are hardcoded into the kafka.go file in the method `input(event *events.Envelope)`.

*NOTE*: For debugging purposes the nozzle is additionaly publishing the container metrics on the topic `containerMetricAsJSON`as a JSON string.

`autoscaler-nozzle ` is written in go and built with [rakutentech/go-nozzle](https://github.com/rakutentech/go-nozzle) package. 


## Preparation

Before using the nozzle, you have to setup a correct environment:

1. Create a folder named "go" anywhere on your computer. For example move to
    * `cd /Users/<username>/Development`
    * `mkdir go`
2. Move to the "go" folder and create the folders "src" and "bin
    * `cd /Users/<username>/Development/go`
    * `mkdir src`
    * `mkdir bin`
3. Move to the "src" folder and clone the repository
    * `cd src`
    * `git clone https://github.com/evoila/osb-autoscaler-kafka-nozzle --branch develop`
4. Set your $GOPATH
    * `vi ~/.bash-profile` and add
    
    * `export GOPATH=/Users/<username>/Development/go`
    * `export PATH=$GOPATH/bin:$PATH`

    Save your changes, exit VI and source your "bash-profile"

    * `source ~/.bash-profile`
5. Install "glide" as via
    * `curl https://glide.sh/get | sh`
6. Move to the "osb-autoscaler-kafka-nozzle" folder and run update glide
    * `cd osb-autoscaler-kafka-nozzle`
    * `glide update`

Now you are ready to roll. Build the project to get an executable.

## Usage

Basic usage is,

```bash
$ osb-autoscaler-kafka-nozzle [options]
```

The following are available options,

```bash
-worker NUM        Number of producer worker. Default is number of CPU core
-debug             Output event to stdout instead of producing message to kafka
-log-level LEVEL   Log level. Default level is INFO (DEBUG|INFO|ERROR)
```

## Configuration

You can configure it via environment variables

```bash
GOPACKAGENAME:              Name of the go main package
SUBSCRIPTION_ID:            Subscription ID
CF_DOPPLER_ADDRESS:         Cloud Foundry doppler address # starts with wss://
CF_UAA_ADDRESS:             Cloud Foundry UAA address
CF_USERNAME:                Cloud Foundry username
CF_PASSWORD:                Cloud Foundry password
REDIS_HOSTS:                Redis hosts # comma seperated list
REDIS_PORT:                 Redis port
REDIS_PASSWORD:             Redis password
REDIS_DATABASE:             Number of the Redis db
KAFKA_HOSTS:                Kafka hosts # comma seperated list
KAFKA_PORT:                 Kafka port
KAFKA_RETRY_MAX:            Max attempts to connect to kafka # integer
KAFKA_RETRY_BACKOFF_MS:     Time to wait before attempting to retry a failed request to a given topic partition # in ms
GO_CF_API:                  Cloud Foundry API address
GO_CF_USERNAME:             Cloud Foundry username for API calls
GO_CF_PASSWORD:             Cloud Foundry password for API calls
```

You can find an example manifest.yml file for Cloud Foundry in [example-manifest.yml](example-manifest.yml)

## Install

You can deploy this as Cloud Foundry application with [go-buildpack](https://github.com/cloudfoundry/go-buildpack). 
