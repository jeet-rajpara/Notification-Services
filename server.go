package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	helper "notification_service/amqp_helper"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	// db, err := database.Connect()
	// if err != nil {
	// 	log.Printf("Error in establishing database connection: %v", err)
	// }

	r := chi.NewRouter()
	// r.Use(database.Middleware(db))
	r.Use(middleware.Logger)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("welcome"))
	})

	conn, ch, err := helper.ConnectRabbitMQ()
	if err != nil {
		log.Printf("Error connecting to RabbitMQ: %v", err)
		return
	}
	defer conn.Close()
	defer ch.Close()

	///start dlx
	// var restingQueue helper.Queue
	restingQueue, err := helper.DeclareQueue(ch, "my-dl-queue", true, false, false, false, map[string]interface{}{
		"x-dead-letter-exchange":    "notificationExchange",
		"x-dead-letter-routing-key": "my-routing-key",
		"x-max-length":              50,
		"x-overflow":                "reject-publish",
		"x-message-ttl":             10000,
	})
	if err != nil {
		panic(err)
	}
	err = helper.DeclareExchange(ch, "my-dlx", "topic", true, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	err = helper.BindQueue(ch, restingQueue.Name, "my-routing-key", "my-dlx", false, nil)
	if err != nil {
		panic(err)
	}
	///end dlx

	err = helper.DeclareExchange(ch, "notificationExchange", "direct", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare an exchange: %v", err)
	}

	q, err := helper.DeclareQueue(ch, "notificationQueue", false, false, false, false, map[string]interface{}{
		"x-max-length":              3,                // Set max length of the queue
		"x-dead-letter-exchange":    "my-dlx",         // Specify the DLX
		"x-dead-letter-routing-key": "my-routing-key", // Specify the routing key for the DLX
	})
	if err != nil {
		log.Fatalf("failed to declare a queue: %w", err)
	}

	err = helper.BindQueue(ch, q.Name, "my-routing-key", "notificationExchange", false, nil)
	if err != nil {
		log.Fatalf("failed to bind a queue: %w", err)
	}

	msgs, err := helper.ConsumeMessages(ch, "notificationQueue")
	if err != nil {
		log.Fatalf("failed to consume message from queue: %w", err)
	}

	go func() {
		for msg := range msgs {
			if err := processMsg(msg); err != nil {
				// Log the error and handle retry logic
				log.Printf("Error processing message: %v", err)
				handleRetry(msg, ch)
			} else {
				msg.Ack(false)
			}
		}
	}()

	// go func() {
	// 	for msg := range msgs {
	// 		if err := processMsg(msg); err != nil {
	// 			if msg.Headers["x-death"] != nil {
	// 				for _, death := range msg.Headers["x-death"].([]interface{}) {
	// 					deathMap := death.(amqp.Table)
	// 					if deathMap["reason"] == "expired" {
	// 						count, ok := deathMap["count"].(int64)
	// 						if ok && count < 3 {
	// 							_ = msg.Nack(false, false)
	// 						} else {
	// 							msg.Ack(true)
	// 							fmt.Printf("maximum retries has been exceeded: %v", msg.MessageId)
	// 						}
	// 						break
	// 					}
	// 				}
	// 			}
	// 		} else {
	// 			msg.Ack(false)
	// 		}
	// 	}
	// }()

	// log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	// <-forever
	dlMsgs, err := helper.ConsumeMessages(ch, "my-dl-queue")
	if err != nil {
		log.Fatalf("failed to consume message from dead-letter queue: %v", err)
	}

	go func() {
		for dlMsg := range dlMsgs {
			log.Printf("DLQ Message: %s", dlMsg.Body)
			handleRetry(dlMsg, ch)
			// dlMsg.Ack(false)
		}
	}()

	var port = ":3600"
	srv := &http.Server{
		Addr:        port,
		Handler:     r,
		IdleTimeout: 2 * time.Minute,
	}
	log.Fatal(srv.ListenAndServe())

}

func processMsg(msg amqp.Delivery) error {
	fmt.Println(string(msg.Body))
	return nil
}

func handleRetry(msg amqp.Delivery, ch *amqp.Channel) {
	if msg.Headers["x-death"] != nil {
		for _, death := range msg.Headers["x-death"].([]interface{}) {
			deathMap := death.(amqp.Table)
			if count, ok := deathMap["count"].(int64); ok && count < 3 {
				log.Printf("Retrying message %s, retry count: %d", msg.MessageId, count)
				err := helper.RepublishMessage(ch, msg, "notificationExchange", "my-routing-key")
				if err != nil {
					log.Printf("Failed to republish message: %v", err)
				} else {
					log.Printf("Republished message: %s", msg.MessageId)
				}
				return
			}
		}
	}
	log.Printf("Message %s exceeded max retries, acknowledging", msg.MessageId)
	msg.Ack(false)
}