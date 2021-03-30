package main

import (
	"log"
	"os"
	"strings"
	"github.com/streadway/amqp"
	"time"
)

var (
	JobNum = 10
	requests = 100 / JobNum
)


func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

var globalcount = 0

func ProduceToMq() {

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
	
}


func main() {
	c := make(chan bool, 10)
	
	for i := 0; i < JobNum; i++ {	
		go ProduceToMq()
	}
	
	<- c
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
