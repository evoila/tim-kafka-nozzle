package redisClient

import (
	"strings"
	"time"

	"github.com/evoila/osb-autoscaler-kafka-nozzle/config"
	"github.com/go-redis/redis"
)

var isCluster bool
var goRedisSingleNodeClient *redis.Client
var goRedisClusterClient *redis.ClusterClient

type GenericRedisClient struct {
}

func NewRedisSingleNodeClient(config *config.Config) *redis.Client {
	var goRedisSingleNodeClient = redis.NewClient(&redis.Options{
		Addr:     config.GoRedisClient.Addrs[0],
		Password: config.GoRedisClient.Password,
		DB: 	  config.GoRedisClient.DB,
	})

	_, err := goRedisSingleNodeClient.Ping().Result()
	if err != nil {
		panic(err)
	}

	return goRedisSingleNodeClient
}

func NewRedisClusterClient(config *config.Config) *redis.ClusterClient {
	var goRedisClusterClient = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.GoRedisClient.Addrs,
		Password: config.GoRedisClient.Password,
	})

	_, err := goRedisClusterClient.Ping().Result()
	if err != nil {
		panic(err)
	}

	return goRedisClusterClient
}

func CheckIfCluster(config *config.Config) {
	goRedisSingleNodeClient = NewRedisSingleNodeClient(config)

	if strings.Contains(goRedisSingleNodeClient.Info("Cluster").String(), "cluster_enabled:1") {
		isCluster = true
		goRedisSingleNodeClient.Close()
		goRedisClusterClient = NewRedisClusterClient(config)
	} else {
		isCluster = false
	}
}

func Set(key string, value string, duration time.Duration) {
	if isCluster {
		goRedisClusterClient.Set(key, value, duration)
	} else {
		goRedisSingleNodeClient.Set(key, value, duration)
	}
}

func Get(key string) string {
	if isCluster {
		return goRedisClusterClient.Get(key).Val()
	} else {
		return goRedisSingleNodeClient.Get(key).Val()
	}
}
