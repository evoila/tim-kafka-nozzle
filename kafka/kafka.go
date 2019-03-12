package kafka

import (
	"encoding/binary"
	"fmt"
	"log"
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
		producerConfig.SetKey("sasl.username", config.Kafka.Sasl.Username)
		producerConfig.SetKey("sasl.password", config.Kafka.Sasl.Password)
		producerConfig.SetKey("ssl.ca.location", os.TempDir()+config.Kafka.Filename)
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
			/*tr := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
			client := &http.Client{Transport: tr}

			request, _ := http.NewRequest("GET", "https://kanw92.beast.local:9090/nwrestapi/v3/global/serverconfig", nil)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Basic cmVzdHVzZXI6Z0RDczBmdHMyMDE1IQ==")
			response, err := client.Do(request)

			if err != nil {
				log.Printf("[ERROR] Failed REST call")
				log.Println(err)
			} else {
				data, _ := ioutil.ReadAll(response.Body)
				kp.input(data, "server_config")
			}

			time.Sleep(10 * 1000 * time.Millisecond)*/
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
