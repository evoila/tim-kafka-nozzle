package autoscaler

type HttpMetric struct {
	Timestamp   int64  `json:"timestamp"`
	MetricName  string `json:"metricName"`
	AppId       string `json:"appId"`
	Requests    int32  `json:"requests"`
	Latency     int32  `json:"latency"`
	Description string `json:"description"`
}
