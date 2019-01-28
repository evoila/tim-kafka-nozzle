package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/autoscaler"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/cf"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/config"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/redisClient"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/stats"
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

func NewKafkaConsumer(config *config.Config) (*kafka.Consumer, error) {
	brokers := config.Kafka.Brokers
	if len(brokers) < 1 {
		return nil, fmt.Errorf("brokers are not provided")
	}

	topics := []string{config.Kafka.Topic.BindingsTopic}

	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers":           strings.Join(brokers, ", "),
		"group.id":                    "bindings-consumer",
		"auto.offset.reset":           "earliest",
		"retry.backoff.ms":            DefaultKafkaRetryBackoff,
		"socket.receive.buffer.bytes": DefaultChannelBufferSize,
	}

	if config.Kafka.Secure {
		consumerConfig.SetKey("security.protocol", "sasl_ssl")
		consumerConfig.SetKey("sasl.mechanism", "SCRAM-SHA-256")
		consumerConfig.SetKey("sasl.username", config.Kafka.SaslUsername)
		consumerConfig.SetKey("sasl.password", config.Kafka.SaslPassword)
		consumerConfig.SetKey("ssl.ca.location", os.TempDir()+"/server.cer.pem")
		consumerConfig.SetKey("ssl.certificate.location", os.TempDir()+"/client.cer.pem")
		consumerConfig.SetKey("ssl.key.location", os.TempDir()+"/client.key.pem")
	}

	if config.Kafka.RetryBackoff != 0 {
		backoff := time.Duration(config.Kafka.RetryBackoff) * time.Millisecond
		consumerConfig.SetKey("retry.backoff.ms", backoff)
	}

	consumer, err := kafka.NewConsumer(&consumerConfig)

	if err != nil {
		log.Println(err)
		return nil, fmt.Errorf("failed to create kafka consumer")
	} else {
		consumer.SubscribeTopics(topics, nil)
		return consumer, nil
	}
}

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
		Producer:                       producer,
		Logger:                         logger,
		Stats:                          stats,
		logMessageTopic:                config.Kafka.Topic.LogMessage,
		autoscalerContainerMetricTopic: config.Kafka.Topic.AutoscalerContainerMetric,
		logMetricContainerMetricTopic:  config.Kafka.Topic.LogMetricContainerMetric,
		httpMetricTopic:                config.Kafka.Topic.HttpMetric,
		repartitionMax:                 repartitionMax,
		errors:                         make(chan *kafka.Error),
		deliveryChan:                   make(chan kafka.Event),
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

func Consume(ctx context.Context, consumer *kafka.Consumer, logger *log.Logger) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		event := consumer.Poll(100)
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			DecodeMessage(e.Value, logger)
		case kafka.Error:
			// Errors should generally be considered as informational, the client will try to automatically recover
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		}
	}
}

func DecodeMessage(consumerMessage []byte, logger *log.Logger) {
	if consumerMessage != nil {
		var data map[string]string
		json.Unmarshal(consumerMessage, &data)

		logger.Printf("[INFO] Received binding for application with id = %s, collection data...", data["appId"])

		if data["appId"] != "" {
			if data["action"] == "bind" || data["action"] == "load" {
				Bind(data)
			} else if data["action"] == "unbind" {
				Unbind(data)
			} else {
				logger.Printf("[ERROR] Unknown action in a binding message for id = %s : %s", data["appId"], data["action"])
			}
		}
	}
}

func Bind(data map[string]string) {
	redisEntry := getAppEnvironmentAsJson(data["appId"])

	if redisEntry["subscribed"] == true {
		UpdateBinding(data, redisEntry)
	} else {
		environmentAsJson := cf.CreateEnvironmentJson(data["appId"], data["source"])
		redisClient.Set(data["appId"], environmentAsJson, 0)
	}
}

func Unbind(data map[string]string) {
	redisEntry := getAppEnvironmentAsJson(data["appId"])

	if redisEntry["logMetric"] == true && redisEntry["autoscaler"] == true {
		UpdateBinding(data, redisEntry)
	} else {
		redisClient.Delete(data["appId"])
	}
}

func UpdateBinding(data map[string]string, redisEntry map[string]interface{}) {
	if data["action"] == "bind" || data["action"] == "load" {
		redisEntry[data["source"]] = true
	} else {
		redisEntry[data["source"]] = false
	}

	tmp, err := json.Marshal(redisEntry)

	if err == nil {
		environmentAsJson := string(tmp)
		redisClient.Set(data["appId"], environmentAsJson, 0)
	}
}

// Produce produces event to kafka
func (kp *KafkaProducer) Produce(ctx context.Context, eventCh <-chan *events.Envelope) {
	kp.once.Do(kp.init)

	kp.Logger.Printf("[INFO] Start to sub input")

	kp.Logger.Printf("[INFO] Start loop to watch events")

	for {
		select {
		case <-ctx.Done():
			// Stop process immediately
			kp.Logger.Printf("[INFO] Stop kafka producer")
			return

		case event, ok := <-eventCh:
			if !ok {
				kp.Logger.Printf("[ERROR] Nozzle consumer eventCh is closed")
				return
			}

			kp.input(event)
		}
	}
}

func (kp *KafkaProducer) input(event *events.Envelope) {
	switch event.GetEventType() {
	case events.Envelope_HttpStart:
		// Do nothing
	case events.Envelope_HttpStartStop:
		if event.GetHttpStartStop().GetApplicationId() != nil && event.GetHttpStartStop().GetPeerType() == 1 &&
			checkIfPublishIsPossible(uuidToString(event.GetHttpStartStop().GetApplicationId())) {

			latency := event.GetHttpStartStop().GetStopTimestamp() - event.GetHttpStartStop().GetStartTimestamp()

			httpMetric := &autoscaler.HttpMetric{
				Timestamp:   event.GetTimestamp() / 1000 / 1000, //convert to ms
				MetricName:  "HttpMetric",
				AppId:       uuidToString(event.GetHttpStartStop().GetApplicationId()),
				Requests:    1,
				Latency:     int32(latency) / 1000 / 1000, //convert to ms
				Description: "Statuscode: " + strconv.Itoa(int(event.GetHttpStartStop().GetStatusCode())),
			}

			jsonHttpMetric, err := json.Marshal(httpMetric)

			if err == nil {
				kp.Producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &kp.httpMetricTopic, Partition: kafka.PartitionAny},
					Value:          jsonHttpMetric,
				}, kp.deliveryChan)
				kp.Stats.Inc(stats.Consume)
			}
		}
	case events.Envelope_HttpStop:
		// Do nothing
	case events.Envelope_LogMessage:
		var appId string = event.GetLogMessage().GetAppId()
		if appId != "" {
			redisEntry := getAppEnvironmentAsJson(appId)
			if checkIfPublishIsPossible(appId) && checkIfSourceTypeIsValid(event.GetLogMessage().GetSourceType()) {
				logMessage := &autoscaler.LogMessage{
					Timestamp:        event.GetLogMessage().GetTimestamp() / 1000 / 1000,
					LogMessage:       string(event.GetLogMessage().GetMessage()[:]),
					LogMessageType:   event.GetLogMessage().GetMessageType().String(),
					SourceType:       event.GetLogMessage().GetSourceType(),
					AppId:            event.GetLogMessage().GetAppId(),
					AppName:          redisEntry["applicationName"].(string),
					Space:            redisEntry["space"].(string),
					Organization:     redisEntry["organization"].(string),
					OrganizationGuid: redisEntry["organization_guid"].(string),
					SourceInstance:   event.GetLogMessage().GetSourceInstance(),
				}

				jsonLogMessage, err := json.Marshal(logMessage)

				if err == nil {
					kp.Producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &kp.logMessageTopic, Partition: kafka.PartitionAny},
						Value:          jsonLogMessage,
					}, kp.deliveryChan)
					kp.Stats.Inc(stats.Consume)
				}
			}
		}

	case events.Envelope_ValueMetric:
		/*kp.Input() <- &sarama.ProducerMessage{
			Topic:    kp.ValueMetricTopic(),
			Value:    &JsonEncoder{event: event},
			Metadata: metadata{retries: 0},
		}*/
	case events.Envelope_CounterEvent:
		// Do nothing
	case events.Envelope_Error:
		// Do nothing
	case events.Envelope_ContainerMetric:
		var appId string = event.GetContainerMetric().GetApplicationId()
		if appId != "" {

			redisEntry := getAppEnvironmentAsJson(appId)
			if checkIfPublishIsPossibleWithoutRequest(appId, redisEntry) {

				containerMetric := &autoscaler.ContainerMetric{
					Timestamp:        event.GetTimestamp() / 1000 / 1000, //convert to ms
					MetricName:       "InstanceContainerMetric",
					AppId:            appId,
					AppName:          redisEntry["applicationName"].(string),
					Space:            redisEntry["space"].(string),
					OrganizationGuid: redisEntry["organization_guid"].(string),
					Cpu:              int32(event.GetContainerMetric().GetCpuPercentage()), //* 100),
					Ram:              int64(event.GetContainerMetric().GetMemoryBytes()),
					InstanceIndex:    event.GetContainerMetric().GetInstanceIndex(),
					Description:      "",
				}

				jsonContainerMetric, err := json.Marshal(containerMetric)

				if err == nil {
					if checkIfAutoscalerIsBound(appId, redisEntry) {
						kp.Producer.Produce(&kafka.Message{
							TopicPartition: kafka.TopicPartition{Topic: &kp.autoscalerContainerMetricTopic, Partition: kafka.PartitionAny},
							Value:          jsonContainerMetric,
						}, kp.deliveryChan)
					}

					if checkIfLogMetricIsBound(appId, redisEntry) {
						kp.Producer.Produce(&kafka.Message{
							TopicPartition: kafka.TopicPartition{Topic: &kp.logMetricContainerMetricTopic, Partition: kafka.PartitionAny},
							Value:          jsonContainerMetric,
						}, kp.deliveryChan)
					}
					kp.Stats.Inc(stats.Consume)
				}
			}
		}
	}
}

func (kp *KafkaProducer) ReadDeliveryChan() {

	// Only checks if report is received since there are no reports for
	// failed deliveries
	for {
		<-kp.deliveryChan
		kp.Stats.Inc(stats.Publish)
	}
}

func getAppEnvironmentAsJson(appId string) map[string]interface{} {
	var data map[string]interface{}
	json.Unmarshal([]byte(redisClient.Get(appId)), &data)
	return data
}

// This function creates a request to redis
func checkIfPublishIsPossible(appId string) bool {
	if redisClient.Get(appId) == "" {
		redisClient.Set(appId, "{\"subscribed\":false}", 0)
		return false
	}

	return getAppEnvironmentAsJson(appId)["subscribed"].(bool)
}

// This function does NOT creates a request to redis when appId is not empty, but works with the given map
// Use the getAppEnvironmentAsJson(appId) function to get the corresponding map
func checkIfPublishIsPossibleWithoutRequest(appId string, redisEntry map[string]interface{}) bool {
	if redisClient.Get(appId) == "" {
		redisClient.Set(appId, "{\"subscribed\":false}", 0)
		return false
	}
	return redisEntry["subscribed"].(bool)
}

func checkIfAutoscalerIsBound(appId string, redisEntry map[string]interface{}) bool {
	return redisClient.Get(appId) != "" && redisEntry["autoscaler"].(bool)
}

func checkIfLogMetricIsBound(appId string, redisEntry map[string]interface{}) bool {
	return redisClient.Get(appId) != "" && redisEntry["logMetric"].(bool)
}

func checkIfSourceTypeIsValid(sourceType string) bool {
	return sourceType == "RTR" || sourceType == "STG" || sourceType == "APP/PROC/WEB"
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
