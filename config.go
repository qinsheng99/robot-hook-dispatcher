package main

import (
	"errors"

	kafka "github.com/opensourceways/kafka-lib/agent"
)

type configuration struct {
	Topic          string `json:"topic"           required:"true"`
	UserAgent      string `json:"user_agent"      required:"true"`
	AccessEndpoint string `json:"access_endpoint" required:"true"`
	ConcurrentSize int    `json:"concurrent_size" required:"true"`
	kafka.Config
}

func (c *configuration) Validate() error {
	if c.Topic == "" {
		return errors.New("missing topic")
	}

	if c.UserAgent == "" {
		return errors.New("missing user_agent")
	}

	if c.AccessEndpoint == "" {
		return errors.New("missing access_endpoint")
	}

	if c.ConcurrentSize <= 0 {
		return errors.New("concurrent_size must be > 0")
	}

	return c.Config.Validate()
}

func (c *configuration) SetDefault() {}
