package autoscaler

type ContainerMetric struct {
	Timestamp        int64  `json:"timestamp"`
	MetricName       string `json:"metricName"`
	AppId            string `json:"appId"`
	AppName          string `json:"appName"`
	Space            string `json:"space"`
	OrganizationGuid string `json:"organizationGuid"`
	Cpu              int32  `json:"cpu"`
	Ram              int64  `json:"ram"`
	InstanceIndex    int32  `json:"instanceIndex"`
	Description      string `json:"description"`
}
