package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/erkkah/letarette/pkg/logger"
	sslr "github.com/erkkah/sslr/internal"
)

var args struct {
	configFile string
}

func main() {
	flag.StringVar(&args.configFile, "cfg", "sslr.json", "SSLR config file")
	flag.Parse()

	config, err := sslr.LoadConfig(args.configFile)
	if err != nil {
		logger.Error.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	job, err := sslr.NewJob(ctx, config)
	if err != nil {
		logger.Error.Printf("Failed to create job: %v\n", err)
		os.Exit(2)
	}

	done := make(chan (struct{}))

	go func() {
		err = job.Run()
		if err != nil {
			logger.Error.Printf("Job failed: %v\n", err)
			os.Exit(3)
		}
		close(done)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)

	for {
		select {
		case s := <-signals:
			logger.Info.Printf("Received signal %v\n", s)
			cancel()
		case <-done:
			os.Exit(0)
		}
	}

}
