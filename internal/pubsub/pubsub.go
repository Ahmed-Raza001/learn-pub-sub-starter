package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AckType represents the acknowledgement type.
type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

// PublishJSON publishes a JSON message to an exchange.
func PublishJSON[T any](
	ch *amqp.Channel,
	exchange string,
	key string,
	val T,
) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

	if err != nil {
		return err
	}

	return nil
}

// SimpleQueueType represents the queue type.
type SimpleQueueType string

const (
	SimpleQueueDurable   SimpleQueueType = "durable"
	SimpleQueueTransient SimpleQueueType = "transient"
)

// DeclareAndBind declares a queue and binds it to an exchange.
func DeclareAndBind(
	conn *amqp.Connection,
	exchange string,
	queueName string,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := queueType == SimpleQueueDurable
	autoDelete := queueType == SimpleQueueTransient
	exclusive := queueType == SimpleQueueTransient

	// Configure Dead Letter Exchange.
	args := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}

	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		args,
	)

	if err != nil {
		ch.Close()
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	)

	if err != nil {
		ch.Close()
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

// SubscribeJSON subscribes to a queue and processes JSON messages.
/*func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange string,
	queueName string,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {

	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)

	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		ch.Close()
		return err
	}

	go func() {
		defer ch.Close()

		for delivery := range deliveries {

			var message T

			err := json.Unmarshal(delivery.Body, &message)
			if err != nil {
				log.Println("Failed to unmarshal message:", err)

				if err := delivery.Nack(false, false); err != nil {
					log.Println("Failed to Nack:", err)
				} else {
					log.Println("NackDiscard: Message discarded")
				}

				continue
			}

			ackType := handler(message)

			switch ackType {

			case Ack:
				if err := delivery.Ack(false); err != nil {
					log.Println("Failed to Ack:", err)
				} else {
					log.Println("Ack: Message acknowledged")
				}

			case NackRequeue:
				if err := delivery.Nack(false, true); err != nil {
					log.Println("Failed to Nack:", err)
				} else {
					log.Println("NackRequeue: Message requeued")
				}

			case NackDiscard:
				if err := delivery.Nack(false, false); err != nil {
					log.Println("Failed to Nack:", err)
				} else {
					log.Println("NackDiscard: Message discarded")
				}

			default:
				log.Println("Unknown AckType, discarding message")

				if err := delivery.Nack(false, false); err != nil {
					log.Println("Failed to Nack:", err)
				}
			}
		}
	}()

	return nil
}*/

// SubscribeJSON subscribes to a queue and processes JSON messages.
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange string,
	queueName string,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var message T
			err := json.Unmarshal(data, &message)
			return message, err
		},
	)
}

// SubscribeGob subscribes to a queue and processes Gob messages.
func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange string,
	queueName string,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var message T
			err := gob.NewDecoder(bytes.NewReader(data)).Decode(&message)
			return message, err
		},
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange string,
	queueName string,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	// Limit prefetch count to 10 messages
	err = ch.Qos(10, 0, false)
	if err != nil {
		ch.Close()
		return err
	}

	deliveries, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return err
	}

	go func() {
		defer ch.Close()

		for delivery := range deliveries {
			message, err := unmarshaller(delivery.Body)
			if err != nil {
				log.Println("Failed to unmarshal message:", err)

				if err := delivery.Nack(false, false); err != nil {
					log.Println("Failed to Nack:", err)
				} else {
					log.Println("NackDiscard: Message discarded")
				}
				continue
			}

			ackType := handler(message)

			switch ackType {
			case Ack:
				if err := delivery.Ack(false); err != nil {
					log.Println("Failed to Ack:", err)
				} else {
					log.Println("Ack: Message acknowledged")
				}

			case NackRequeue:
				if err := delivery.Nack(false, true); err != nil {
					log.Println("Failed to Nack:", err)
				} else {
					log.Println("NackRequeue: Message requeued")
				}

			case NackDiscard:
				if err := delivery.Nack(false, false); err != nil {
					log.Println("Failed to Nack:", err)
				} else {
					log.Println("NackDiscard: Message discarded")
				}

			default:
				log.Println("Unknown AckType, discarding message")
				if err := delivery.Nack(false, false); err != nil {
					log.Println("Failed to Nack:", err)
				}
			}
		}
	}()

	return nil
}

// PublishGob publishes a gob message to an exchange.
func PublishGob[T any](
	ch *amqp.Channel,
	exchange string,
	key string,
	val T,
) error {
	var buf bytes.Buffer

	encoder := gob.NewEncoder(&buf)

	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		},
	)

	if err != nil {
		return err
	}

	return nil
}

/*func PublishGob(
    ch *amqp.Channel,
    exchange string,
    key string,
    message any,
) error {
    var buf bytes.Buffer

    encoder := gob.NewEncoder(&buf)

    err := encoder.Encode(message)
    if err != nil {
        return err
    }

    return ch.Publish(
        exchange,
        key,
        false,
        false,
        amqp.Publishing{
            ContentType: "application/gob",
            Body:        buf.Bytes(),
        },
    )
}*/
