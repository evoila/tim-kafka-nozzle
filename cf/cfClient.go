package cf

import (
	"encoding/json"

	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/config"
)

var goCfClient *cfclient.Client
var err error

type Environment struct {
	Subscribed       bool   `json:"subscribed"`
	ApplicationName  string `json:"applicationName"`
	Space            string `json:"space"`
	OrganizationGuid string `json:"organization_guid"`
	Organization     string `json:"organization"`
	LogMetric        bool   `json:"logMetric"`
	Autoscaler       bool   `json:"autoscaler"`
}

func NewCfClient(config *config.Config) {
	goCfClient, err = cfclient.NewClient(&cfclient.Config{
		ApiAddress: config.GoCfClient.Api,
		Username:   config.GoCfClient.Username,
		Password:   config.GoCfClient.Password,
	})

	if err != nil {
		panic(err)
	}
}

func CreateEnvironmentJson(appId string, source string) string {
	logMetric := false
	autoscaler := false

	space := getSpace(appId)

	if source == "logMetric" {
		logMetric = true
	}

	if source == "autoscaler" {
		autoscaler = true
	}

	environment := &Environment{
		Subscribed:       true,
		ApplicationName:  getApplicationName(appId),
		Space:            getSpaceName(space),
		OrganizationGuid: space.OrganizationGuid,
		Organization:     getOrganizationName(space),
		LogMetric:        logMetric,
		Autoscaler:       autoscaler,
	}

	environmentAsJson, err := json.Marshal(environment)

	if err != nil {
		return ""
	} else {
		return string(environmentAsJson)
	}
}

func getApplicationName(appId string) string {
	data, _ := goCfClient.GetAppByGuid(appId)

	return data.Name
}

func getSpace(appId string) cfclient.Space {
	data, _ := goCfClient.GetAppByGuid(appId)

	space, _ := goCfClient.GetSpaceByGuid(data.SpaceGuid)

	return space
}

func getSpaceName(space cfclient.Space) string {

	return space.Name
}

func getOrganizationName(space cfclient.Space) string {

	org, _ := goCfClient.GetOrgByGuid(space.OrganizationGuid)

	return org.Name
}
