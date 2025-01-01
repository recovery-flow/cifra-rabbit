package cifra_rabbit

import (
	"context"

	"github.com/sirupsen/logrus"
)

func (b *Broker) Listen(ctx context.Context, log *logrus.Logger, queueName string, routingKey string, handler func(context.Context, []byte) error) error {
	_, err := b.channel.QueueDeclare(
		queueName,
		true,  // Durable
		false, // Auto-deleted
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return err
	}

	err = b.channel.QueueBind(
		queueName,
		routingKey,
		b.exchange,
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return err
	}

	msgs, err := b.channel.Consume(
		queueName,
		"",    // Consumer
		false, // Auto-ack (auto delete)
		false, // Exclusive
		false, // No-local
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			if err := handler(ctx, msg.Body); err != nil {
				log.Printf("Failed to handle message: %v", err)
			} else {
				err := msg.Ack(false)
				if err != nil {
					log.Printf("Failed to acknowledge message: %v", err)
				}
			}
		}
	}()

	return nil
}
