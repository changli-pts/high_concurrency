package common

import (
	"log"
	"os"
	"strings"
	"github.com/streadway/amqp"
	"time"
)

func failOnError (err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func Produce(requests int) int {
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
	
	var globalcount = 0
	body := bodyForm(os.Args)
	start := time.Now()
	for i := 0; i < requests; i ++ {
		err = ch.Publish(
				"logs",     // exchange
				"",         // routing key
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
				})
		globalcount++
			
		failOnError(err, "Failed to publish a message")
		//log.Printf(" [x] Sent %s", body)
	}	
	
	log.Printf("total requests: %d", globalcount)
	log.Printf("%.2fs elapsed\n", time.Since(start).Seconds())
	
	return globalcount
}

func bodyForm(args []string) string {

	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "request"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}



func Consume(requests int) map[int][]byte {
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

	var globalCount = 0
	results := make(map[int][]byte)
	start := time.Now()	
  for d := range msgs {
		if globalCount <= requests {
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

	return results
}
