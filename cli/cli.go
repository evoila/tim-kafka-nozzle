package cli

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/evoila/tim-kafka-nozzle/config"
	"github.com/evoila/tim-kafka-nozzle/kafka"
	statsServer "github.com/evoila/tim-kafka-nozzle/server"
	stats "github.com/evoila/tim-kafka-nozzle/stats"
	"github.com/hashicorp/logutils"
	"golang.org/x/net/context"
)

//go:generate ./bin/kafka-firehose-nozzle -gen-godoc

// Exit codes are int values that represent an exit code for a particular error.
const (
	ExitCodeOK    int = 0
	ExitCodeError int = 1 + iota
)

const (
	Consume stats.StatsType = iota
	ConsumeFail
	Publish
	PublishFail
	SlowConsumerAlert
	SubInputBuffer
)

const (
	// DefaultCfgPath is default config file path
	DefaultCfgPath = "example/kafka-firehose-nozzle.toml"

	// DefaultStatsInterval is default interval of displaying
	// stats info to console
	DefaultStatsInterval = 10 * time.Second

	// DefaultUsername to grant access token for firehose
	DefaultUsername = "admin"

	// DefaultUAATimeout is default timeout for requesting
	// auth token to UAA server.
	DefaultUAATimeout = 20 * time.Second

	// DefaultSubscriptionID is default subscription ID for
	// loggregagor firehose.
	DefaultSubscriptionID = "debug-kafka-firehose-nozzle"

	// DefaultIdleTimeout is the default timeout for receiving a single message
	// from the firehose
	DefaultIdleTimeout = 60 * time.Second
)

const (
	EnvPassword = "UAA_PASSWORD"
)

// godocFile is file name for godoc
const (
	godocFile = "doc.go"
)

// CLI is the command line object
type CLI struct {
	// outStream and errStream are the stdout and stderr
	// to write message from the CLI.
	OutStream, ErrStream io.Writer
}

// Run invokes the CLI with the given arguments.
func (cli *CLI) Run(args []string) int {
	var (
		cfgPath        string
		username       string
		password       string
		subscriptionID string
		logLevel       string

		worker int

		statsInterval time.Duration

		server   bool
		debug    bool
		version  bool
		genGodoc bool
	)

	// Define option flag parsing
	flags := flag.NewFlagSet(Name, flag.ContinueOnError)
	flags.SetOutput(cli.ErrStream)
	flags.Usage = func() {
		fmt.Fprintf(cli.ErrStream, helpText)
	}

	flags.StringVar(&cfgPath, "config", DefaultCfgPath, "")
	flags.StringVar(&subscriptionID, "subscription", "", "")
	flags.StringVar(&username, "username", "", "")
	flags.StringVar(&password, "password", os.Getenv(EnvPassword), "")
	flags.StringVar(&logLevel, "log-level", "INFO", "")
	flags.IntVar(&worker, "worker", runtime.NumCPU(), "")
	flags.DurationVar(&statsInterval, "stats-interval", DefaultStatsInterval, "")
	flags.BoolVar(&server, "server", false, "")
	flags.BoolVar(&debug, "debug", false, "")
	flags.BoolVar(&version, "version", false, "")

	// -gen-godoc flag is only for developers of this nozzle.
	// It generates godoc.
	flags.BoolVar(&genGodoc, "gen-godoc", false, "")

	// Parse commandline flag
	if err := flags.Parse(args[1:]); err != nil {
		return ExitCodeError
	}

	// Generate godoc
	if genGodoc {
		if err := godoc(); err != nil {
			fmt.Fprintf(cli.ErrStream, "Faild to generate godoc %s\n", err)
			return ExitCodeError
		}

		fmt.Fprintf(cli.OutStream, "Successfully generated godoc\n")
		return ExitCodeOK
	}

	// Show version
	if version {
		fmt.Fprintf(cli.ErrStream, "%s version %s\n", Name, Version)
		return ExitCodeOK
	}

	// Setup logger with level Filtering
	logger := log.New(&logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "ERROR"},
		MinLevel: (logutils.LogLevel)(strings.ToUpper(logLevel)),
		Writer:   cli.OutStream,
	}, "", log.LstdFlags)
	logger.Printf("[INFO] LogLevel: %s", logLevel)

	// Show basic infomation
	logger.Printf("[INFO] %s version: %s", Name, Version)
	logger.Printf("[INFO] Go version: %s (%s/%s)",
		runtime.Version(), runtime.GOOS, runtime.GOARCH)
	logger.Printf("[INFO] Num of CPU: %d", runtime.NumCPU())

	// Load configuration
	config, err := config.LoadConfig(cfgPath)
	if err != nil {
		logger.Printf("[ERROR] Failed to load configuration file: %s", err)
		return ExitCodeError
	}
	logger.Printf("[DEBUG] %#v", config)

	if subscriptionID != "" {
		config.SubscriptionID = subscriptionID
	} else if config.SubscriptionID == "" {
		config.SubscriptionID = DefaultSubscriptionID
	}

	// Create cert files for kafka
	if config.Kafka.Secure {
		createCertificateFiles(config, logger)
	}

	// Initialize stats collector
	stats := stats.NewStats()

	// Start server.
	if server {
		Server := statsServer.Server{
			Logger: logger,
			Stats:  stats,
		}
		go Server.Start()
	}

	if config.Kafka.Brokers != nil {

		for i := range config.Kafka.Brokers {
			config.Kafka.Brokers[i] = strings.Trim(config.Kafka.Brokers[i], "\"[]")
			config.Kafka.Brokers[i] += ":"
			config.Kafka.Brokers[i] += config.Kafka.Port
		}

		logger.Printf("[INFO] Kafka Brokers %v", config.Kafka.Brokers)
	}

	// Setup nozzle producer
	var producer kafka.NozzleProducer
	if debug {
		logger.Printf("[INFO] Use LogProducer")
		producer = kafka.NewLogProducer(logger)
	} else {
		logger.Printf("[INFO] Use KafkaProducer")
		var err error
		producer, err = kafka.NewKafkaProducer(logger, stats, config)
		if err != nil {
			logger.Printf("[ERROR] Failed to construct kafka producer: %s", err)
			return ExitCodeError
		}
		go producer.ReadDeliveryChan()
	}

	// Create a ctx for cancelation signal across the goroutined producers.
	ctx, cancel := context.WithCancel(context.Background())

	// Display stats in every x seconds.
	go func() {
		logger.Printf("[INFO] Stats display interval: %s", statsInterval)
		ticker := time.NewTicker(statsInterval)

		statsIntervalAsInt := float64(statsInterval) / 1000 / 1000 / 1000 // Convert from nanoseconds to seconds
		lastConsume, lastPublish := float64(0), float64(0)

		intervalCounter := uint64(0)

		for {
			select {
			case <-ticker.C:
				intervalCounter++

				stats.ConsumePerSec = (float64(stats.Consume) - lastConsume) / statsIntervalAsInt
				stats.PublishPerSec = (float64(stats.Publish) - lastPublish) / statsIntervalAsInt

				lastConsume = float64(stats.Consume)
				lastPublish = float64(stats.Publish)

				logger.Printf("[INFO] ### Current Interval: %d ###", intervalCounter)

				logger.Printf("[INFO] Consume per sec: %.1f", stats.ConsumePerSec)
				logger.Printf("[INFO] Consumed messages: %d", stats.Consume)

				logger.Printf("[INFO] Publish per sec: %.1f", stats.PublishPerSec)
				logger.Printf("[INFO] Published messages: %d", stats.Publish)

				logger.Printf("[INFO] Publish delay: %d", stats.Delay)

				logger.Printf("[INFO] Failed consume: %d", stats.ConsumeFail)
				logger.Printf("[INFO] Failed publish: %d", stats.PublishFail)
				logger.Printf("[INFO] SlowConsumer alerts: %d", stats.SlowConsumerAlert)
			}
		}
	}()

	// Handle producer error
	// TODO(tcnksm): Buffer and restart when it recovers
	go func() {
		// cancel all other producer goroutine
		defer cancel()

		for err := range producer.Errors() {
			logger.Printf("[ERROR] Faield to produce logs: %s", err)
			stats.Inc(PublishFail)
			stats.Inc(PublishFail)
		}
	}()

	// Handle signal of interrupting to stop process safely.
	signalCh := make(chan os.Signal)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	go func() {
		<-signalCh
		logger.Println("[INFO] Interrupt Received: cancel all producers")
		cancel()
	}()

	// Start kafka consumer
	logger.Println("[INFO] Start kafka consumer process")

	// Start multiple produce worker processes.
	// nozzle consumer events will be distributed to each producer.
	// And each producer produces message to kafka.
	//
	// Process will be blocked until all producer process finishes each jobs.
	var wg sync.WaitGroup
	logger.Printf("[INFO] Start %d producer process", worker)
	for i := 0; i < worker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			producer.Produce(ctx)
		}()
	}

	// Wait until all producer process is done.
	wg.Wait()

	// Attempt to close all the things. Not returns soon even if
	// error is happend while closing.
	isError := false

	logger.Printf("[INFO] Closing producer")
	producer.Close()

	logger.Printf("[INFO] Finished kafka firehose nozzle")
	if isError {
		return ExitCodeError
	}
	return ExitCodeOK
}

func createCertificateFiles(config *config.Config, logger *log.Logger) {
	// Create file for ca certificate
	caCertContent := []byte(config.Kafka.SslCa)
	err := ioutil.WriteFile(os.TempDir()+"server.cer.pem", caCertContent, 0666)
	if err != nil {
		log.Fatal("Cannot create ca certificate file", err)
	}
	logger.Printf("[INFO] Ca certificate file created: " + os.TempDir() + "server.cer.pem")

	// Create file for client certificate
	clientCertContent := []byte(config.Kafka.SslCertificate)
	err = ioutil.WriteFile(os.TempDir()+"client.cer.pem", clientCertContent, 0666)
	if err != nil {
		log.Fatal("Cannot create client certificate file", err)
	}
	logger.Printf("[INFO] Client certificate file created: " + os.TempDir() + "client.cer.pem")

	// Create file for client key
	clientKeyContent := []byte(config.Kafka.SslKey)
	err = ioutil.WriteFile(os.TempDir()+"client.key.pem", clientKeyContent, 0666)
	if err != nil {
		log.Fatal("Cannot create client key file", err)
	}
	logger.Printf("[INFO] Client key file created: " + os.TempDir() + "client.key.pem")
}

func godoc() error {
	f, err := os.Create(godocFile)
	if err != nil {
		return err
	}
	defer f.Close()

	tmpl, err := template.New("godoc").Parse(godocTmpl)
	if err != nil {
		return err
	}
	return tmpl.Execute(f, helpText)
}

var godocTmpl = `// THIS FILE IS GENERATED BY GO GENERATE.
// DO NOT EDIT THIS FILE BY HAND.

/*
{{ . }}
*/
package main
`

// helpText is used for flag usage messages.
var helpText = `kafka-firehose-nozzle is Cloud Foundry nozzle which forwards logs from
the loggeregagor firehose to Apache kafka (http://kafka.apache.org/).

Usage:

    kafak-firehose-nozzle [options]

Available options:

    -config PATH          Path to configuraiton file    
    -username NAME        username to grant access token to connect firehose
    -password PASS        password to grant access token to connect firehose
    -worker NUM           Number of producer worker. Default is number of CPU core
    -subscription ID      Subscription ID for firehose. Default is 'kafka-firehose-nozzle'
    -stats-interval TIME  How often display stats info to console  
    -debug                Output event to stdout instead of producing message to kafka
    -log-level LEVEL      Log level. Default level is INFO (DEBUG|INFO|ERROR)
`
