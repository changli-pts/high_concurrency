package main 

import (
	"github.com/streadway/amqp"
	"log"
	"time"
	"fmt"
	"sync"
	"github.com/gomodule/redigo/redis"
)

var (
	JobNum = 10
	requests = 100
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



func ConsumeToRedis() {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")
	
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name, // queue name
		"",     // routing key
		"logs", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	redisclient, err := redis.Dial("tcp", "localhost:6379")
	failOnError(err, "Failed to connect to redis")
	

	msgs, err := ch.Consume(
		q.Name,  // queue
		"",      // consumer
		true,    // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)

	failOnError(err, "Failed to register a consumer")

	results := make(map[int][]byte)
	start := time.Now()	
  for d := range msgs {
		if globalCount <= requests {		
			GetFromRedis(d.Body, redisclient)
			results[globalCount] = d.Body
			globalCount++	
			failOnError(err, "Failed to get from redis")
			//log.Printf("%s", d.Body)
			//log.Printf("total requests %d", globalCount)
			//log.Printf("%.2fs elapsed\n", time.Since(start).Seconds())
		}else {
			 break;
		}
		
	}
	log.Printf("total requests %d", globalCount)
	log.Printf("%.2fs elapsed\n", time.Since(start).Seconds())
	fmt.Println(results)

}




func main() {
	//forever := make(chan bool)
	
	wg := sync.WaitGroup{}
	wg.Add(JobNum)
	for i := 0; i< JobNum; i++ {
		go func(i int) {
			ConsumeToRedis()
			wg.Done()		
		}(i)
	}
	wg.Wait()
	
	//log.Printf("[*] Consumer Waiting for messages. To exit press CTRL+C")
	//<-forever

}

	
