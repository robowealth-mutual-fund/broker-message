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
	autoCommit   bool
	ready        chan bool
	handlers     map[string]common.Handler
	topics       []string
	sessionGroup sarama.ConsumerGroupSession
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
	c.sessionGroup = session
	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")
		c.handleMessage(msg)
		if c.autoCommit {
			session.Commit()
		}
	}

	return nil
}

func (c *consumer) session() (sessions sarama.ConsumerGroupSession) {
	return c.sessionGroup
}

func (c *consumer) handleMessage(msg *sarama.ConsumerMessage) {
	ck := contextKey(contextKeyValue)
	ctx := context.WithValue(context.Background(), ck, msg.Key)
	c.handlers[msg.Topic](ctx, msg.Value)
}

func (kafka *broker) initGroupHandler() {
	kafka.consumer = &consumer{
		ready:      make(chan bool),
		handlers:   map[string]common.Handler{},
		topics:     []string{},
		autoCommit: kafka.conf.AutoCommit,
	}
}
