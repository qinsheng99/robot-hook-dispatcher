package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	kafka "github.com/opensourceways/kafka-lib/agent"
	"github.com/opensourceways/server-common-lib/config"
	"github.com/opensourceways/server-common-lib/logrusutil"
	liboptions "github.com/opensourceways/server-common-lib/options"
	"github.com/sirupsen/logrus"
)

type options struct {
	service     liboptions.ServiceOptions
	enableDebug bool
}

func (o *options) Validate() error {
	return o.service.Validate()
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	o.service.AddFlags(fs)

	fs.BoolVar(
		&o.enableDebug, "enable_debug", false,
		"whether to enable debug model.",
	)

	_ = fs.Parse(args)

	return o
}

const component = "robot-hook-dispatcher"

func main() {
	logrusutil.ComponentInit(component)

	o := gatherOptions(
		flag.NewFlagSet(os.Args[0], flag.ExitOnError),
		os.Args[1:]...,
	)
	if err := o.Validate(); err != nil {
		logrus.Errorf("Invalid options, err:%s", err.Error())

		return
	}

	if o.enableDebug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("debug enabled.")
	}

	// load config
	configAgent := config.NewConfigAgent(func() config.Config {
		return new(configuration)
	})
	if err := configAgent.Start(o.service.ConfigFile); err != nil {
		logrus.Errorf("error starting config agent, err:%s", err.Error())

		return
	}

	defer configAgent.Stop()

	getConfig := func() (*configuration, error) {
		_, cfg := configAgent.GetConfig()
		if c, ok := cfg.(*configuration); ok {
			return c, nil
		}

		return nil, errors.New("can't convert to configuration")
	}

	cfg, err := getConfig()
	if err != nil {
		logrus.Errorf("get config failed, err:%s", err.Error())

		return
	}

	// init kafka
	if err = kafka.Init(&cfg.Config, logrus.WithField("module", "kfk")); err != nil {
		logrus.Errorf("Error connecting kfk mq, err:%s", err.Error())

		return
	}

	defer kafka.Exit()

	// server
	d, err := newDispatcher(
		cfg,
		func() (int, error) {
			cfg, err := getConfig()
			if err != nil {
				return 0, err
			}

			return cfg.ConcurrentSize, nil
		},
	)
	if err != nil {
		logrus.Errorf("Error new dispatcherj, err:%s", err.Error())

		return
	}

	// run
	run(d)
}

func run(d *dispatcher) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup
	defer wg.Wait()

	called := false
	ctx, done := context.WithCancel(context.Background())

	defer func() {
		if !called {
			called = true
			done()
		}
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()

		select {
		case <-ctx.Done():
			logrus.Info("receive done. exit normally")
			return

		case <-sig:
			logrus.Info("receive exit signal")
			done()
			called = true
			return
		}
	}(ctx)

	if err := d.run(ctx); err != nil {
		logrus.Errorf("subscribe failed, err:%v", err)
	}
}
