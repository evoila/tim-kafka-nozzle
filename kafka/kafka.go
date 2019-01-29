package kafka

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/evoila/tim-kafka-nozzle/config"
	"github.com/evoila/tim-kafka-nozzle/stats"
	"golang.org/x/net/context"
)

const (
	// TopicAppLogTmpl is Kafka topic name template for LogMessage
	TopicAppLogTmpl = "app-log-%s"

	// TopicCFMetrics is Kafka topic name for ValueMetric
	TopicCFMetric = "cf-metrics"
)

const (
	// Default topic name for each event
	DefaultValueMetricTopic = "value-metric"
	DefaultLogMessageTopic  = "log-message"

	DefaultKafkaRepartitionMax = 5
	DefaultKafkaRetryMax       = 1
	DefaultKafkaRetryBackoff   = 100 * time.Millisecond

	DefaultChannelBufferSize  = 512
	DefaultSubInputBufferSize = 1024
)

func NewKafkaProducer(logger *log.Logger, stats *stats.Stats, config *config.Config) (NozzleProducer, error) {
	brokers := config.Kafka.Brokers
	if len(brokers) < 1 {
		return nil, fmt.Errorf("brokers are not provided")
	}

	producerConfig := kafka.ConfigMap{
		"bootstrap.servers":             strings.Join(brokers, ", "),
		"partition.assignment.strategy": "roundrobin",
		"retries":                       DefaultKafkaRetryMax,
		"retry.backoff.ms":              DefaultKafkaRetryBackoff,
	}

	if config.Kafka.Secure {
		producerConfig.SetKey("security.protocol", "sasl_ssl")
		producerConfig.SetKey("sasl.mechanism", "SCRAM-SHA-256")
		producerConfig.SetKey("sasl.username", config.Kafka.SaslUsername)
		producerConfig.SetKey("sasl.password", config.Kafka.SaslPassword)
		producerConfig.SetKey("ssl.ca.location", os.TempDir()+"/server.cer.pem")
		producerConfig.SetKey("ssl.certificate.location", os.TempDir()+"/client.cer.pem")
		producerConfig.SetKey("ssl.key.location", os.TempDir()+"/client.key.pem")
	}

	if config.Kafka.RetryMax != 0 {
		producerConfig.SetKey("retries", config.Kafka.RetryMax)
	}

	if config.Kafka.RetryBackoff != 0 {
		backoff := time.Duration(config.Kafka.RetryBackoff) * time.Millisecond
		producerConfig.SetKey("retry.backoff.ms", backoff)
	}

	producer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		return nil, err
	}

	repartitionMax := DefaultKafkaRepartitionMax
	if config.Kafka.RepartitionMax != 0 {
		repartitionMax = config.Kafka.RepartitionMax
	}

	return &KafkaProducer{
		Producer:       producer,
		Logger:         logger,
		Stats:          stats,
		repartitionMax: repartitionMax,
		errors:         make(chan *kafka.Error),
		deliveryChan:   make(chan kafka.Event),
	}, nil
}

// KafkaProducer implements NozzleProducer interfaces
type KafkaProducer struct {
	*kafka.Producer

	repartitionMax int
	errors         chan *kafka.Error

	logMessageTopic                string
	autoscalerContainerMetricTopic string
	logMetricContainerMetricTopic  string
	httpMetricTopic                string

	Logger *log.Logger
	Stats  *stats.Stats

	deliveryChan chan kafka.Event

	once sync.Once
}

// metadata is metadata which will be injected to ProducerMessage.Metadata.
// This is used only when publish is failed and re-partitioning by ourself.
type metadata struct {
	// retires is the number of re-partitioning
	retries int
}

// init sets default logger
func (kp *KafkaProducer) init() {
	if kp.Logger == nil {
		kp.Logger = defaultLogger
	}
}

func (kp *KafkaProducer) Errors() <-chan *kafka.Error {
	return kp.errors
}

func (kp *KafkaProducer) Successes() <-chan *kafka.Message {
	msgCh := make(chan *kafka.Message)
	return msgCh
}

// Produce produces event to kafka
func (kp *KafkaProducer) Produce(ctx context.Context) {
	kp.once.Do(kp.init)

	kp.Logger.Printf("[INFO] Start to sub input")

	kp.Logger.Printf("[INFO] Start loop to watch events")

	for {
		select {
		case <-ctx.Done():
			// Stop process immediately
			kp.Logger.Printf("[INFO] Stop kafka producer")
			return

		default:
			client := &http.Client{}
			request, _ := http.NewRequest("GET", "https://kanw92:9090/nwrestapi/v3/global/serverconfig", nil)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Basic cmVzdHVzZXI6Z0RDczBmdHMyMDE1IQ==")
			response, err := client.Do(request)

			if err != nil {
				log.Printf("[ERROR] Failed REST call")

				data := `{
					"acceptNewRecoverSessions": true,
					"acceptNewSessions": true,
					"aclPassthrough": true,
					"administrators": [
					  "user=root,host=kanw92.beast.local",
					  "user=administrator,host=kanw92.beast.local",
					  "user=system,host=kanw92.beast.local",
					  "user=administrator,host=kamgmt",
					  "user=administrator,host=kamgmt.beast.local",
					  "*@*"
					],
					"authenticationProxyPort": 7999,
					"authenticationServiceDatabase": "/nsr/authc/data",
					"authenticationServicePort": 9090,
					"clpRefresh": "No",
					"clpUom": "1",
					"deviceSharingMode": "MaximalSharing",
					"disableRpsClone": true,
					"jobInactivityTimeout": 0,
					"jobsdbRetentionInHours": 72,
					"keepIncompleteBackups": false,
					"manualSaves": true,
					"name": "kanw92.beast.local",
					"nasDevicePolicyAllowed": true,
					"parallelism": 32,
					"publicArchives": false,
					"resourceId": {
					  "id": "3.0.241.60.0.0.0.0.205.38.207.90.172.16.174.95",
					  "sequence": 22
					},
					"saveSessionDistribution": "MaxSessions",
					"serverOSType": "Linux",
					"vmwarePolicyAllowed": true,
					"vmwsEnable": true,
					"vmwsPort": 8080,
					"vmwsUserName": "VMUser",
					"vmwsUserPassword": "*******",
					"volumePriority": "NearLinePriority",
					"wormPoolsOnlyHoldWormTapes": true,
					"wormTapesOnlyInWormPools": true
				  }`

				kp.input([]byte(data), "server_config")
			} else {
				fmt.Println(response.Status)
				data, err := ioutil.ReadAll(response.Body)
				kp.input(data, "server_config")
				fmt.Println(err)
			}

			time.Sleep(10 * 1000 * time.Millisecond)
		}
	}
}

func (kp *KafkaProducer) input(data []byte, topicName string) {

	kp.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Value:          data,
	}, kp.deliveryChan)

	kp.Stats.Inc(stats.Consume)
}

func (kp *KafkaProducer) ReadDeliveryChan() {

	// Only checks if report is received since there are no reports for
	// failed deliveries
	for {
		<-kp.deliveryChan
		kp.Stats.Inc(stats.Publish)
	}
}

func uuidToString(uuid *events.UUID) string {
	var lowerarray = make([]byte, 8)
	var higherarray = make([]byte, 8)
	binary.LittleEndian.PutUint64(lowerarray, uuid.GetLow())
	binary.LittleEndian.PutUint64(higherarray, uuid.GetHigh())

	var bytearray = addBytes(lowerarray, higherarray)
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		bytearray[:4], bytearray[4:6], bytearray[6:8], bytearray[8:10], bytearray[10:])
}

func addBytes(arrayLow []byte, arrayHigh []byte) []byte {
	var bytearray = make([]byte, 16)
	for i := 0; i <= 7; i++ {
		//j := i + 8
		bytearray[0+i] = arrayLow[i]
		bytearray[8+i] = arrayHigh[i]

	}
	return bytearray
}
