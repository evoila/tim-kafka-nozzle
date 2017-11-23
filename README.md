# osb-autoscaler-kafka-nozzle 


This project is a fork from [kafka-firehose-nozzle](https://github.com/rakutentech/kafka-firehose-nozzle) and changes the output from JSON to protobuf. It is part of the [osb-autoscaler-framework](https://github.com/evoila/osb-autoscaler-core).

Unlike the nozzle from rakutentech this nozzle publishes `ContainerMetric` and `HttpStartStop` events to two seperate topics for all applications.

*NOTE*: Currently kafka topics are hardcoded into the kafka.go file in the method `input(event *events.Envelope)`.

*NOTE*: For debugging purposes the nozzle is additionaly publishing the container metrics on the topic `containerMetricAsJSON`as a JSON string.

`autoscaler-nozzle ` is written by Golang and built with [rakutentech/go-nozzle](https://github.com/rakutentech/go-nozzle) package. 


## Usage

Basic usage is,

```bash
$ autoscaler-nozzle [options]
```

The following are available options,

```bash
-config PATH       Path to configuration file
-username NAME     username to grant access token to connect firehose
-password PASS     password to grant access token to connect firehose
-worker NUM        Number of producer worker. Default is number of CPU core
-subscription ID   Subscription ID for firehose. Default is 'kafka-firehose-nozzle'
-debug             Output event to stdout instead of producing message to kafka
-log-level LEVEL   Log level. Default level is INFO (DEBUG|INFO|ERROR)
```

You can set `password` via `UAA_PASSWORD` environmental variable.

## Configuration

You can configure it via `.toml` file. You can see the example and description of this configuration file in [example](/example) directory. The default name for the configuration file is `kafka-firehose-nozzle.toml`.

## Install

You can deploy this as Cloud Foundry application with [go-buildpack](https://github.com/cloudfoundry/go-buildpack). 
