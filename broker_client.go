package main

import (
	"errors"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	reconnectDelay = 5 * time.Second
	reInitDelay    = 2 * time.Second
	resendDelay    = 5 * time.Second
)

var (
	errNotReady      = errors.New("broker client is not ready")
	errAlreadyClosed = errors.New("broker client is not connected")
)

type BrokerClient struct {
	exchangeName     string
	pendingQueueName string
	logger           *log.Logger
	connection       *amqp.Connection
	channel          *amqp.Channel
	done             chan bool
	notifyConnClose  chan *amqp.Error
	notifyChanClose  chan *amqp.Error
	notifyConfirm    chan amqp.Confirmation
	isReady          bool
}

func NewBrokerClient(exchangeName, brokerURL string) *BrokerClient {
	brokerClient := &BrokerClient{
		exchangeName: exchangeName,
		done:         make(chan bool),
		logger:       log.New(log.Writer(), "broker_client: ", log.Flags()),
	}

	go brokerClient.handleReconnect(brokerURL)

	return brokerClient
}

func (c *BrokerClient) handleReconnect(brokerURL string) {
	for {
		c.isReady = false
		c.logger.Printf("connecting to %s", brokerURL)

		conn, err := c.connect(brokerURL)

		if err != nil {
			c.logger.Printf("failed to connect to %s: %s", brokerURL, err)

			select {
			case <-c.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := c.handleReInit(conn); done {
			break
		}
	}
}

func (c *BrokerClient) connect(brokerURL string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(brokerURL)

	if err != nil {
		return nil, err
	}

	c.changeConnection(conn)
	c.logger.Printf("connected to %s", brokerURL)

	return conn, nil
}

func (c *BrokerClient) handleReInit(conn *amqp.Connection) bool {
	for {
		c.isReady = false

		err := c.init(conn)

		if err != nil {
			c.logger.Printf("failed to initialize channel: %s", err)

			select {
			case <-c.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-c.done:
			return true
		case <-c.notifyConnClose:
			c.logger.Printf("connection closed, reconnecting...")
			return false
		case <-c.notifyChanClose:
			c.logger.Printf("channel closed, reinitializing...")
		}
	}
}

func (c *BrokerClient) init(conn *amqp.Connection) error {
	channel, err := conn.Channel()
	if err != nil {
		return err
	}

	err = channel.Confirm(false)
	if err != nil {
		return err
	}

	durable := true
	autoDelete := false
	internal := false
	noWait := false
	extraParams := amqp.Table{}
	err = channel.ExchangeDeclare(
		c.exchangeName,
		"topic",
		durable,
		autoDelete,
		internal,
		noWait,
		extraParams,
	)
	if err != nil {
		return err
	}

	exclusive := true
	pendingQueue, err := channel.QueueDeclare(
		"",
		durable,
		autoDelete,
		exclusive,
		noWait,
		extraParams,
	)
	if err != nil {
		return err
	}

	err = channel.QueueBind(
		pendingQueue.Name,
		"requests.status.pending",
		c.exchangeName,
		noWait,
		extraParams,
	)
	if err != nil {
		return err
	}

	c.changeChannel(channel)
	c.pendingQueueName = pendingQueue.Name
	c.isReady = true
	c.logger.Printf("channel initialized")

	return nil
}

func (c *BrokerClient) changeConnection(conn *amqp.Connection) {
	c.connection = conn
	c.notifyConnClose = make(chan *amqp.Error, 1)
	c.connection.NotifyClose(c.notifyConnClose)
}

func (c *BrokerClient) changeChannel(channel *amqp.Channel) {
	c.channel = channel

	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.channel.NotifyClose(c.notifyChanClose)

	c.notifyConfirm = make(chan amqp.Confirmation, 1)
	c.channel.NotifyPublish(c.notifyConfirm)
}

func (c *BrokerClient) Consume() (<-chan amqp.Delivery, error) {
	if !c.isReady {
		return nil, errNotReady
	}

	if err := c.channel.Qos(1, 0, false); err != nil {
		return nil, err
	}

	consumer := ""
	autoAck := false
	exclusive := false
	noLocal := false
	noWait := false
	extraParams := amqp.Table{}
	return c.channel.Consume(
		c.pendingQueueName,
		consumer,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		extraParams,
	)
}

func (c *BrokerClient) Close() error {
	if !c.isReady {
		return errAlreadyClosed
	}

	close(c.done)

	if err := c.channel.Close(); err != nil {
		return err
	}

	if err := c.connection.Close(); err != nil {
		return err
	}

	c.isReady = false

	return nil
}
