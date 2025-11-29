package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	// QueueDurable declares a durable queue (survives broker restart)
	QueueDurable SimpleQueueType = iota
	// QueueTransient declares a transient (non-durable) queue
	QueueTransient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	}); err != nil {
		return err
	}
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// ensure exchange exists (using direct as a reasonable default)
	if err := ch.ExchangeDeclare(
		exchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		_ = ch.Close()
		return nil, amqp.Queue{}, err
	}

	durable := queueType == QueueDurable
	autoDelete := queueType == QueueTransient
	exclusive := queueType == QueueTransient
	q, err := ch.QueueDeclare(
		queueName,
		durable,    // durable
		autoDelete, // autoDelete
		exclusive,  // exclusive
		false,      // noWait
		nil,        // args
	)
	if err != nil {
		_ = ch.Close()
		return nil, amqp.Queue{}, err
	}

	if err := ch.QueueBind(
		q.Name,
		key,
		exchange,
		false,
		nil,
	); err != nil {
		_ = ch.Close()
		return nil, amqp.Queue{}, err
	}

	return ch, q, nil
}
