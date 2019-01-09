package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/cloudfoundry/sonde-go/events"
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

	DefaultChannelBufferSize  = 512 // Sarama default is 256
	DefaultSubInputBufferSize = 1024
)

func NewKafkaConsumer(config *config.Config) (*cluster.Consumer, error) {
	consumerConfig := cluster.NewConfig()

	// This is the default, but Errors are required for repartitioning
	consumerConfig.Consumer.Return.Errors = true

	consumerConfig.Consumer.Retry.Backoff = DefaultKafkaRetryBackoff
	if config.Kafka.RetryBackoff != 0 {
		backoff := time.Duration(config.Kafka.RetryBackoff) * time.Millisecond
		consumerConfig.Consumer.Retry.Backoff = backoff
	}

	consumerConfig.ChannelBufferSize = DefaultChannelBufferSize

	consumerConfig.Consumer.Fetch.Default = 100000

	brokers := config.Kafka.Brokers
	if len(brokers) < 1 {
		return nil, fmt.Errorf("brokers are not provided")
	}

	//TLS Config temporary disabled

	/*tlsConfig, err := NewTLSConfig("kafka/security/client.cer.pem", "kafka/security/client.key.pem", "kafka/security/server.cer.pem")

	if err != nil {
		log.Fatal(err)
	}

	consumerConfig.Net.TLS.Enable = true
	consumerConfig.Net.TLS.Config = tlsConfig

	consumerConfig.Net.SASL.Enable = true
	consumerConfig.Net.SASL.User = "admin"
	consumerConfig.Net.SASL.Password = "MDVTbq4svBEyyBwpLvus7ZmaAEqsCF"
	consumerConfig.Net.SASL.Handshake = true*/

	topics := []string{config.Kafka.BindingsTopic}

	consumer, err := cluster.NewConsumer(brokers, "bindings-consumer", topics, consumerConfig)

	if err != nil {
		log.Println(err)
		return nil, fmt.Errorf("failed to create kafka consumer")
	} else {
		return consumer, nil
	}
}

func NewKafkaProducer(logger *log.Logger, stats *stats.Stats, config *config.Config) (NozzleProducer, error) {
	// Setup kafka async producer (We must use sync producer)
	// TODO (tcnksm): Enable to configure more properties.
	producerConfig := sarama.NewConfig()

	producerConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll

	// This is the default, but Errors are required for repartitioning
	producerConfig.Producer.Return.Errors = true

	producerConfig.Producer.Retry.Max = DefaultKafkaRetryMax
	if config.Kafka.RetryMax != 0 {
		producerConfig.Producer.Retry.Max = config.Kafka.RetryMax
	}

	producerConfig.Producer.Retry.Backoff = DefaultKafkaRetryBackoff
	if config.Kafka.RetryBackoff != 0 {
		backoff := time.Duration(config.Kafka.RetryBackoff) * time.Millisecond
		producerConfig.Producer.Retry.Backoff = backoff
	}

	producerConfig.ChannelBufferSize = DefaultChannelBufferSize

	//TLS Config temporary disabled

	/*tlsConfig, err := NewTLSConfig("kafka/security/client.cer.pem", "kafka/security/client.key.pem", "kafka/security/server.cer.pem")

	if err != nil {
		log.Fatal(err)
	}

	producerConfig.Net.TLS.Enable = true
	producerConfig.Net.TLS.Config = tlsConfig

	producerConfig.Net.SASL.Enable = true
	producerConfig.Net.SASL.User = "admin"
	producerConfig.Net.SASL.Password = "MDVTbq4svBEyyBwpLvus7ZmaAEqsCF"
	producerConfig.Net.SASL.Handshake = true*/

	brokers := config.Kafka.Brokers
	if len(brokers) < 1 {
		return nil, fmt.Errorf("brokers are not provided")
	}

	asyncProducer, err := sarama.NewAsyncProducer(brokers, producerConfig)
	if err != nil {
		return nil, err
	}

	kafkaTopic := config.Kafka.Topic
	if kafkaTopic.LogMessage == "" {
		kafkaTopic.LogMessage = DefaultLogMessageTopic
	}

	if kafkaTopic.ValueMetric == "" {
		kafkaTopic.ValueMetric = DefaultValueMetricTopic
	}

	repartitionMax := DefaultKafkaRepartitionMax
	if config.Kafka.RepartitionMax != 0 {
		repartitionMax = config.Kafka.RepartitionMax
	}

	subInputBuffer := DefaultSubInputBufferSize
	subInputCh := make(chan *sarama.ProducerMessage, subInputBuffer)

	return &KafkaProducer{
		AsyncProducer:      asyncProducer,
		Logger:             logger,
		Stats:              stats,
		logMessageTopic:    kafkaTopic.LogMessage,
		logMessageTopicFmt: kafkaTopic.LogMessageFmt,
		valueMetricTopic:   kafkaTopic.ValueMetric,
		repartitionMax:     repartitionMax,
		subInputCh:         subInputCh,
		errors:             make(chan *sarama.ProducerError),
	}, nil
}

func NewTLSConfig(clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.InsecureSkipVerify = true
	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}

// KafkaProducer implements NozzleProducer interfaces
type KafkaProducer struct {
	sarama.AsyncProducer

	repartitionMax int
	errors         chan *sarama.ProducerError

	// SubInputCh is buffer for re-partitioning
	subInputCh chan *sarama.ProducerMessage

	logMessageTopic    string
	logMessageTopicFmt string

	valueMetricTopic string

	Logger *log.Logger
	Stats  *stats.Stats

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

func (kp *KafkaProducer) LogMessageTopic(appID string) string {
	if kp.logMessageTopicFmt != "" {
		return fmt.Sprintf(kp.logMessageTopicFmt, appID)
	}

	return kp.logMessageTopic
}

func (kp *KafkaProducer) ValueMetricTopic() string {
	return kp.valueMetricTopic
}

func (kp *KafkaProducer) Errors() <-chan *sarama.ProducerError {
	return kp.errors
}

func Consume(ctx context.Context, messageCh <-chan *sarama.ConsumerMessage, logger *log.Logger) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:
	for {
		select {
		case message := <-messageCh:
			DecodeMessage(message, logger)
		case <-signals:
			break ConsumerLoop
		}
	}
}

func DecodeMessage(consumerMessage *sarama.ConsumerMessage, logger *log.Logger) {
	if consumerMessage.Value != nil {
		var data map[string]string
		json.Unmarshal(consumerMessage.Value, &data)

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

	kp.Logger.Printf("[INFO] Start to watching producer error for re-partition")
	go func() {
		for producerErr := range kp.AsyncProducer.Errors() {
			// Instead of giving up, try to resubmit the message so that it can end up
			// on a different partition (we don't care about order of message)
			// This is a workaround for https://github.com/Shopify/sarama/issues/514
			meta, _ := producerErr.Msg.Metadata.(metadata)
			kp.Logger.Printf("[ERROR] Producer error %+v", producerErr)

			if meta.retries >= kp.repartitionMax {
				kp.errors <- producerErr
				continue
			}

			// NOTE: We need to re-create Message because original message
			// which producer.Error stores internal state (unexported field)
			// and it effect partitioning.
			originalMsg := producerErr.Msg
			msg := &sarama.ProducerMessage{
				Topic: originalMsg.Topic,
				Value: originalMsg.Value,

				// Update retry count
				Metadata: metadata{
					retries: meta.retries + 1,
				},
			}

			// If sarama buffer is full, then input it to nozzle side buffer
			// (subInput) and retry to produce it later. When subInput is
			// full, we drop message.
			//
			// TODO(tcnksm): Monitor subInput buffer.
			select {
			case kp.Input() <- msg:
				kp.Logger.Printf("[DEBUG] Repartitioning")
			default:
				select {
				case kp.subInputCh <- msg:
					kp.Stats.Inc(stats.SubInputBuffer)
				default:
					// If subInput is full, then drop message.....
					kp.errors <- producerErr
				}
			}
		}
	}()

	kp.Logger.Printf("[INFO] Start to sub input (buffer for sarama input)")

	go func() {
		for msg := range kp.subInputCh {
			kp.Input() <- msg
			kp.Logger.Printf("[DEBUG] Repartitioning (from subInput)")
			kp.Stats.Dec(stats.SubInputBuffer)
		}
	}()

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
				var encoder sarama.ByteEncoder = jsonHttpMetric

				kp.Stats.Inc(stats.Consume)
				kp.Input() <- &sarama.ProducerMessage{
					Topic: "metric_http",
					Value: encoder,
				}
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
					var encoder sarama.ByteEncoder = jsonLogMessage

					kp.Stats.Inc(stats.Consume)
					kp.Input() <- &sarama.ProducerMessage{
						Topic: "log_messages",
						Value: encoder,
					}
				}
			}
		}

	case events.Envelope_ValueMetric:
		/*kp.Stats.Inc(Consume)
		kp.Input() <- &sarama.ProducerMessage{
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
					var encoder sarama.ByteEncoder = jsonContainerMetric
					kp.Stats.Inc(stats.Consume)

					if checkIfAutoscalerIsBound(appId, redisEntry) {
						kp.Input() <- &sarama.ProducerMessage{
							Topic: "autoscaler_metric_container",
							Value: encoder,
						}
					}

					if checkIfLogMetricIsBound(appId, redisEntry) {
						kp.Input() <- &sarama.ProducerMessage{
							Topic: "logMetric_metric_container",
							Value: encoder,
						}
					}
				}
			}
		}
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
