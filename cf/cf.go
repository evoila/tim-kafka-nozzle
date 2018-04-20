package cf

import(
	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/config"
)

func NewCfClient(config *config.Config) *cfclient.Client {
	var goCfClient, _ = cfclient.NewClient(&cfclient.Config{
		ApiAddress:		config.GoCfClient.Api,
		Username:		config.GoCfClient.Username,
		Password:		config.GoCfClient.Password,
	})

	return goCfClient
}