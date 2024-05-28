package amqp_helper

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ConnectRabbitMQ() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	return conn, ch, nil
}

func DeclareExchange(ch *amqp.Channel, name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return ch.ExchangeDeclare(
		name,       // name
		kind,       // type
		durable,    // durable
		autoDelete, // auto-deleted
		internal,   // internal
		noWait,     // no-wait
		args,       // arguments
	)
}

func DeclareQueue(ch *amqp.Channel, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return ch.QueueDeclare(
		name,       // name
		durable,    // durable
		autoDelete, // delete when unused
		exclusive,  // exclusive
		noWait,     // no-wait
		args,       // arguments
	)
}


func BindQueue(ch *amqp.Channel, queueName, routingKey, exchangeName string, noWait bool, args amqp.Table) error {
	return ch.QueueBind(
		queueName,  // queue name
		routingKey, // routing key
		exchangeName, // exchange
		noWait,
		args,
	)
}


func ConsumeMessages(ch *amqp.Channel, queueName string) (<-chan amqp.Delivery, error) {
	return ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
}

func RepublishMessage(ch *amqp.Channel, msg amqp.Delivery, exchange, routingKey string) error {
    return ch.Publish(
        exchange,   // Exchange
        routingKey, // Routing key
        false,      // Mandatory
        false,      // Immediate
        amqp.Publishing{
            Headers:         msg.Headers,
            ContentType:     msg.ContentType,
            ContentEncoding: msg.ContentEncoding,
            Body:            msg.Body,
            DeliveryMode:    msg.DeliveryMode,
            Priority:        msg.Priority,
            Expiration:      msg.Expiration,
            MessageId:       msg.MessageId,
            Timestamp:       msg.Timestamp,
            Type:            msg.Type,
            UserId:          msg.UserId,
            AppId:           msg.AppId,
        },
    )
}