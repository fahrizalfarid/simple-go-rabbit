package communication

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	Uri, Queuename string
	TimeOutContext time.Duration
}

type connection struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
	uri        string
	queueName  string
	disconnect chan error
	timeoutCtx time.Duration
}

func (c *connection) Connect() error {
	var err error
	c.conn, err = amqp.Dial(
		c.uri,
	)
	if err != nil {
		return errors.New("dial error")
	}

	go func() {
		<-c.conn.NotifyClose(make(chan *amqp.Error))
		c.disconnect <- errors.New("Disconnected")
	}()

	c.channel, err = c.conn.Channel()
	if err != nil {
		return errors.New("channel error")
	}

	log.Println("Connected")
	return nil
}

func (c *connection) reconnect() error {
	time.Sleep(time.Second)

	if err := c.Connect(); err != nil {
		return err
	}
	err := c.BindQueue()
	if err != nil {
		return err
	}

	return nil
}

func (c *connection) BindQueue() error {
	var err error

	args := make(map[string]any)
	args["x-queue-mode"] = "lazy"
	// args["x-expires"] = 60 * 1000 //60s

	c.queue, err = c.channel.QueueDeclare(
		c.queueName,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return err
	}

	// 1 message will pass to you
	// before the message ack by you
	err = c.channel.Qos(1, 0, false)
	if err != nil {
		log.Printf("Error setting qos: %s", err)
	}

	return nil
}

func (p *connection) Publish(msg []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeoutCtx)
	defer cancel()

	if err := p.channel.PublishWithContext(ctx,
		"",
		p.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         msg,
			DeliveryMode: amqp.Persistent, // save to disk not ram
			Timestamp:    time.Now(),
		},
	); err != nil {
		return err
	}

	return nil
}

func (c *connection) consume() (<-chan amqp.Delivery, error) {
	msgs, err := c.channel.Consume(
		c.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	return msgs, err
}

func (c *connection) RunConsumer() {
	runForever := make(chan struct{}, 1)
	m := make(map[string]any)

	go func() {

		for {
			msgs, err := c.consume()
			if err != nil {
				log.Println("consume", err)
			}

			for msg := range msgs {
				_ = json.Unmarshal(msg.Body, &m)
				log.Printf("Received a message: %v, %v", m, msg.Timestamp)

				// mark as ack
				msg.Ack(false)

				// or reject and requeue
				// msg.Reject(true)

			}

		}
	}()

	go func() {
		for {
			if <-c.disconnect != nil {
				c.Close()

				for {
					err := c.reconnect()
					if err != nil {
						time.Sleep(10 * time.Second)
						log.Println("reconnecting")
					} else {
						log.Println("Connected from dead")
						c.RunConsumer()
						break
					}
				}
			}
		}
	}()

	<-runForever
}

func (c *connection) Close() {
	c.channel.Close()
	c.conn.Close()
}
