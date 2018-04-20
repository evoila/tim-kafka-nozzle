package redisClient

import (
	"github.com/go-redis/redis"
	"github.com/evoila/osb-autoscaler-kafka-nozzle/config"
)

func NewRedisClient(config *config.Config) *redis.Client {
	var goRedisClient = redis.NewClient(&redis.Options{
		Addr:		config.GoRedisClient.Addr,
		Password: 	config.GoRedisClient.Password,
		DB:			config.GoRedisClient.DB,
	})

	return goRedisClient
}