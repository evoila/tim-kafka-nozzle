package redisClient

import (
	"github.com/evoila/osb-autoscaler-kafka-nozzle/config"
	"github.com/go-redis/redis"
)

func NewRedisClient(config *config.Config) *redis.ClusterClient {
	var goRedisClient = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.GoRedisClient.Addrs,
		Password: config.GoRedisClient.Password,
	})

	_, err := goRedisClient.Ping().Result()
	if err != nil {
		panic(err)
	}

	return goRedisClient
}
