package autoscaler

type LogMessage struct {
	Timestamp        int64  `json:"timestamp"`
	LogMessage       string `json:"logMessage"`
	LogMessageType   string `json:"logMessageType"`
	SourceType       string `json:"sourceType"`
	AppId            string `json:"appId"`
	AppName          string `json:"appName"`
	Space            string `json:"space"`
	Organization     string `json:"organization"`
	OrganizationGuid string `json:"organizationGuid"`
	SourceInstance   string `json:"sourceInstance"`
}
