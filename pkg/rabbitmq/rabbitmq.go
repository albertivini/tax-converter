package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

func OpenChannel() (*amqp.Channel, error) {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")

	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()

	// escolhe a quantidade de mensagens q devem ser lidas
	// e o processo concluido antes de ler mais msgs
	ch.Qos(100, 0, false)

	if err != nil {
		panic(err)
	}

	return ch, nil
}

func Consume(ch *amqp.Channel, out chan amqp.Delivery) error {
	msgs, err := ch.Consume(
		"orders",
		"go-consumer",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	for msg := range msgs {
		out <- msg
	}

	return nil
}
