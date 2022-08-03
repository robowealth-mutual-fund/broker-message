package message

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/robowealth-mutual-fund/broker-message/common"
	"github.com/robowealth-mutual-fund/broker-message/kafka"
)

type Broker interface {
	RegisterHandler(topic string, handler common.Handler)
	Start(errCallback common.CloseCallback)
	SendTopicMessage(topic string, msg []byte) (err error)
	Session() sarama.ConsumerGroupSession
}

func NewBroker(brokerType string, config *common.Config) (broker Broker, err error) {
	switch brokerType {
	case common.KafkaBrokerType:
		return kafka.MakeKafkaBroker(config)
	default:
		return nil, errors.New("invalid broker type")
	}
}
