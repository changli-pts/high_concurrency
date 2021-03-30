package common

import (
	"log"
	"fmt"
	"github.com/gomodule/redigo/redis"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

var globalCount = 0

func GetFromRedis(request []byte, redisclient redis.Conn) bool { 
	n, err := redisclient.Do("GET", request)
	//log.Printf("send message: %s", request)
	
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	return n == int64(0)
}

func ConnectRedis() redis.Conn {
	
	redisclient, err := redis.Dial("tcp", "localhost:6379")
	failOnError(err, "Failed to connect to redis")

	return redisclient
}

