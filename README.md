# autoscaler-nozzle 


This project is a fork from [kafka-firehose-nozzle](https://github.com/rakutentech/kafka-firehose-nozzle) and changes the output from JSON to protobuf.

Unlike the nozzle from rakutentech this nozzle publishes `ContainerMetric` and `HttpStartStop` events to two seperate topics for all applications.

*NOTE*: Currently kafka topics are hardcoded into the kafka.go file in the method `input(event *events.Envelope)`.

`autoscaler-nozzle ` is written by Golang and built with [rakutentech/go-nozzle](https://github.com/rakutentech/go-nozzle) package. 


## Usage

Basic usage is,

```bash
$ autoscaler-nozzle [options]
```

The following are available options,

```bash
-config PATH       Path to configuraiton file
-username NAME     username to grant access token to connect firehose
-password PASS     password to grant access token to connect firehose
-worker NUM        Number of producer worker. Default is number of CPU core
-subscription ID   Subscription ID for firehose. Default is 'kafka-firehose-nozzle'
-debug             Output event to stdout instead of producing message to kafka
-log-level LEVEL   Log level. Default level is INFO (DEBUG|INFO|ERROR)
```

You can set `password` via `UAA_PASSWORD` environmental variable.

## Configuration

You can configure it via `.toml` file. You can see the example and description of this configuration file in [example](/example) directory. 

## Install

You can deploy this as Cloud Foundry application with [go-buildpack](https://github.com/cloudfoundry/go-buildpack). 

## Contribution

1. Fork ([https://github.com/rakutentech/kafka-firehose-nozzle/fork](https://github.com/rakutentech/kafka-firehose-nozzle/fork))
1. Create a feature branch
1. Commit your changes
1. Rebase your local changes against the master branch
1. Run test suite with the `make test-all` command and confirm that it passes
1. Create a new Pull Request