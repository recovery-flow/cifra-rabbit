package cifra_rabbit

import "github.com/streadway/amqp"

func (b *Broker) Publish(routingKey string, body []byte) error {
	return b.channel.Publish(
		b.exchange, // Exchange
		routingKey, // Routing key
		false,      // Mandatory
		false,      // Immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}
