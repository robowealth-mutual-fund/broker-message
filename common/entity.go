package common

import (
	"context"
	"github.com/Shopify/sarama"
)

type Config struct {
	BackOffTime  int
	MaximumRetry int
	Version      string
	Group        string
	Host         []string
	Debug        bool
	AutoCommit   bool
}

type Handler func(ctx context.Context, msg []byte, session sarama.ConsumerGroupSession)
type CloseCallback func(ctx context.Context, err error)
