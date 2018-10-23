package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/autoscaler"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/cf"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/config"
	jsonEncoder "github.com/evoila/osb-autoscaler-kafka-nozzle/encoder"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/redisClient"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/stats"
	"github.com/gogo/protobuf/proto"
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

	topics := []string{config.Kafka.BindingsTopic}

	consumer, err := cluster.NewConsumer(brokers, "bindings-consumer", topics, consumerConfig)

	if err != nil {
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

func Consume(ctx context.Context, messageCh <-chan *sarama.ConsumerMessage) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:
	for {
		select {
		case message := <-messageCh:
			DecodeMessage(message)
		case <-signals:
			break ConsumerLoop
		}
	}
}

func DecodeMessage(consumerMessage *sarama.ConsumerMessage) {
	if consumerMessage.Value != nil {
		var data map[string]string
		json.Unmarshal(consumerMessage.Value, &data)

		if data["appId"] != "" {
			if data["action"] == "bind" {
				Bind(data)
			} else {
				Unbind(data)
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
		fmt.Println(data["appId"])
		fmt.Println(environmentAsJson)
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
	if data["action"] == "bind" {
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
			protb := &autoscaler.ProtoHttpMetric{
				Timestamp:   event.GetTimestamp() / 1000 / 1000, //convert to ms
				MetricName:  "HttpMetric",
				AppId:       uuidToString(event.GetHttpStartStop().GetApplicationId()),
				Requests:    1,
				Latency:     int32(latency) / 1000 / 1000, //convert to ms
				Description: "Statuscode: " + strconv.Itoa(int(event.GetHttpStartStop().GetStatusCode())),
			}
			out, _ := proto.Marshal(protb)
			var encoder sarama.ByteEncoder = out

			kp.Stats.Inc(stats.Consume)
			kp.Input() <- &sarama.ProducerMessage{
				Topic: "metric_http",
				Value: encoder,
			}
		}
	case events.Envelope_HttpStop:
		// Do nothing
	case events.Envelope_LogMessage:
		if event.GetLogMessage().GetAppId() != "" {
			if checkIfPublishIsPossible(event.GetLogMessage().GetAppId()) && checkIfSourceTypeIsValid(event.GetLogMessage().GetSourceType()) {
				protb := &autoscaler.ProtoLogMessage{
					Timestamp:        event.GetLogMessage().GetTimestamp() / 1000 / 1000,
					LogMessage:       string(event.GetLogMessage().GetMessage()[:]),
					LogMessageType:   event.GetLogMessage().GetMessageType().String(),
					SourceType:       event.GetLogMessage().GetSourceType(),
					AppId:            event.GetLogMessage().GetAppId(),
					AppName:          getAppEnvironmentAsJson(event.GetLogMessage().GetAppId())["applicationName"].(string),
					Space:            getAppEnvironmentAsJson(event.GetLogMessage().GetAppId())["space"].(string),
					Organization:     getAppEnvironmentAsJson(event.GetLogMessage().GetAppId())["organization"].(string),
					OrganizationGuid: getAppEnvironmentAsJson(event.GetLogMessage().GetAppId())["organization_guid"].(string),
					SourceInstance:   event.GetLogMessage().GetSourceInstance(),
				}

				out, _ := proto.Marshal(protb)
				var encoder sarama.ByteEncoder = out

				kp.Stats.Inc(stats.Consume)
				kp.Input() <- &sarama.ProducerMessage{
					Topic: "log_messages",
					Value: encoder,
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
		if event.GetContainerMetric().GetApplicationId() != "" && checkIfPublishIsPossible(event.GetContainerMetric().GetApplicationId()) {
			protb := &autoscaler.ProtoContainerMetric{
				Timestamp:        event.GetTimestamp() / 1000 / 1000, //convert to ms
				MetricName:       "InstanceContainerMetric",
				AppId:            event.GetContainerMetric().GetApplicationId(),
				AppName:          getAppEnvironmentAsJson(event.GetContainerMetric().GetApplicationId())["applicationName"].(string),
				Space:            getAppEnvironmentAsJson(event.GetContainerMetric().GetApplicationId())["space"].(string),
				OrganizationGuid: getAppEnvironmentAsJson(event.GetContainerMetric().GetApplicationId())["organization_guid"].(string),
				Cpu:              int32(event.GetContainerMetric().GetCpuPercentage()), //* 100),
				Ram:              int64(event.GetContainerMetric().GetMemoryBytes()),
				InstanceIndex:    event.GetContainerMetric().GetInstanceIndex(),
				Description:      "",
			}
			out, _ := proto.Marshal(protb)
			var encoder sarama.ByteEncoder = out

			kp.Stats.Inc(stats.Consume)
			kp.Input() <- &sarama.ProducerMessage{
				Topic: "containerMetricAsJSON",
				Value: &jsonEncoder.JSONEncoder{Event: event},
			}
			kp.Input() <- &sarama.ProducerMessage{
				Topic: "metric_container",
				Value: encoder,
			}
		}
	}
}

func getAppEnvironmentAsJson(appId string) map[string]interface{} {
	var data map[string]interface{}
	json.Unmarshal([]byte(redisClient.Get(appId)), &data)
	return data
}

func checkIfPublishIsPossible(appId string) bool {
	if redisClient.Get(appId) == "" {
		redisClient.Set(appId, "{\"subscribed\":false}", 0)
	}

	if getAppEnvironmentAsJson(appId)["subscribed"] == true {
		return true
	}

	return false
}

func checkIfSourceTypeIsValid(sourceType string) bool {
	if sourceType == "RTR" || sourceType == "STG" || sourceType == "APP/PROC/WEB" {
		return true
	}

	return false
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
