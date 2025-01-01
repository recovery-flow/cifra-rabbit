package cifra_rabbit

import (
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Broker struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	exchange   string
}

func NewBroker(url, exchange string) (*Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		true,  // Durable
		false, // Auto-deleted
		false, // Internal
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	return &Broker{
		connection: conn,
		channel:    ch,
		exchange:   exchange,
	}, nil
}

func (b *Broker) Close(log *logrus.Logger) {
	if err := b.channel.Close(); err != nil {
		log.Errorf("Failed to close channel: %v", err)
	}
	if err := b.connection.Close(); err != nil {
		log.Errorf("Failed to close connection: %v", err)
	}
}
