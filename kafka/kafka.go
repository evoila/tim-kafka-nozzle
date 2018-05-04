package kafka

import (
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
	"strings"

	"golang.org/x/net/context"

	"github.com/Shopify/sarama"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/autoscaler"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/config"
	jsonEncoder "github.com/evoila/osb-autoscaler-kafka-nozzle/encoder"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/stats"
	"github.com/gogo/protobuf/proto"
	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/go-redis/redis"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/redisClient"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/cf"
)

var goCfClient, _ = cfclient.NewClient(&cfclient.Config{})
var goRedisClient = redis.NewClient(&redis.Options{})
var logMessageStorage []string

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

func NewKafkaProducer(logger *log.Logger, stats *stats.Stats, config *config.Config) (NozzleProducer, error) {
	goCfClient = cf.NewCfClient(config)
	goRedisClient = redisClient.NewRedisClient(config)

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
		if event.GetHttpStartStop().GetApplicationId() != nil && event.GetHttpStartStop().GetPeerType() == 1 {
			appId := uuidToString(event.GetHttpStartStop().GetApplicationId())

			if checkIfPublishIsPossible(appId) {
				latency := event.GetHttpStartStop().GetStopTimestamp() - event.GetHttpStartStop().GetStartTimestamp()
				protb := &autoscaler.ProtoHttpMetric{
					Timestamp:   event.GetTimestamp() / 1000 / 1000, //convert to ms
					MetricName:  "HttpMetric",
					AppId:       appId,
					Requests:    1,
					Latency:     int32(latency) / 1000 / 1000, //convert to ms
					Description: "Statuscode: " + strconv.Itoa(int(event.GetHttpStartStop().GetStatusCode())),
				}
				out, _ := proto.Marshal(protb)
				var encoder sarama.ByteEncoder = out

				kp.Stats.Inc(stats.Consume)
				kp.Input() <- &sarama.ProducerMessage {
					Topic: "metric_http",
					Value: encoder,
				}
			}
		}
	case events.Envelope_HttpStop:
		// Do nothing
	case events.Envelope_LogMessage:
		if event.GetLogMessage().GetAppId() != "" {
			appId := event.GetLogMessage().GetAppId()
			sourceType := event.GetLogMessage().GetSourceType()

			if checkIfPublishIsPossible(appId) && sourceType != "DEA" {
				var logMessageToPublish string
				tmpLogMessage := string(event.GetLogMessage().GetMessage()[:])
				if strings.HasPrefix(tmpLogMessage, "\t") {
					tmpLogMessage := strings.Replace(tmpLogMessage, "\t", "", -1)
					logMessageStorage = append(logMessageStorage, tmpLogMessage)
				} else {
					logMessageToPublish = strings.Join(logMessageStorage, ", ")
					logMessageStorage = nil
					logMessageStorage = append(logMessageStorage, tmpLogMessage)
				}

				if logMessageToPublish != "" {
					protb := &autoscaler.ProtoLogMessage{
						Timestamp:		event.GetLogMessage().GetTimestamp() / 1000 / 1000,
						LogMessage:		logMessageToPublish,
						LogMessageType:	event.GetLogMessage().GetMessageType().String(),
						SourceType:		sourceType,
						AppId:			appId,
						AppName:		getApplicationName(event.GetLogMessage().GetAppId()),
						Space:			getSpaceName(getSpace(event.GetLogMessage().GetAppId())),
						Organization:	getOrganizationName(getSpace(event.GetLogMessage().GetAppId())),
					}
					out, _ := proto.Marshal(protb)
					var encoder sarama.ByteEncoder = out
					
					kp.Stats.Inc(stats.Consume)
					kp.Input() <- &sarama.ProducerMessage{
						Topic:    "log_messages",
						Value:	  encoder,
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
		if event.GetContainerMetric().GetApplicationId() != "" {
			appId := event.GetContainerMetric().GetApplicationId()

			if checkIfPublishIsPossible(appId) {
				protb := &autoscaler.ProtoContainerMetric{
					Timestamp:     event.GetTimestamp() / 1000 / 1000, //convert to ms
					MetricName:    "InstanceContainerMetric",
					AppId:         appId,
					Cpu:           int32(event.GetContainerMetric().GetCpuPercentage()), //* 100),
					Ram:           int64(event.GetContainerMetric().GetMemoryBytes()),
					InstanceIndex: event.GetContainerMetric().GetInstanceIndex(),
					Description:   "",
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
}

func getApplicationName(appId string) string {
	data, _ := goCfClient.GetAppRoutes(appId)

	return data[0].Host
}

func getSpace(appId string) cfclient.Space {
	data, _ := goCfClient.GetAppRoutes(appId)

	var spaceGuid string = data[0].SpaceGuid

	space, _ := goCfClient.GetSpaceByGuid(spaceGuid)

	return space
}

func getSpaceName(space cfclient.Space) string {

	return space.Name
}

func getOrganizationName(space cfclient.Space) string {
	var orgGuid string = space.OrganizationGuid
	
	org, _ := goCfClient.GetOrgByGuid(orgGuid)

	return org.Name
}

func checkIfPublishIsPossible(appId string) bool {
	subscribed := goRedisClient.Get(appId).Val()
	
	if subscribed == "" {
		goRedisClient.Set(appId, "false", 0)
	}

	if subscribed == "true" {
		return true
	} else {
		return false
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
