package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/robowealth-mutual-fund/broker-message/common"
)

type contextKey string

const contextKeyValue = "key"

type consumer struct {
	ready    chan bool
	handlers map[string]common.Handler
	topics   []string
}

func (c *consumer) Setup(sarama.ConsumerGroupSession) (err error) {
	c.becomeReady()
	return nil
}

func (c *consumer) becomeReady() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Setup consumer group found error: %s", err)
		}
	}()
	close(c.ready)
}

func (c *consumer) Cleanup(sarama.ConsumerGroupSession) (err error) {
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")
		c.handleMessage(msg, session)
	}

	return nil
}

func (c *consumer) handleMessage(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	ck := contextKey(contextKeyValue)
	ctx := context.WithValue(context.Background(), ck, msg.Key)
	c.handlers[msg.Topic](ctx, msg.Value, session)
}

func (kafka *broker) initGroupHandler() {
	kafka.consumer = &consumer{
		ready:    make(chan bool),
		handlers: map[string]common.Handler{},
		topics:   []string{},
	}
}
