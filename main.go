package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/erkkah/letarette/pkg/logger"
	sslr "github.com/erkkah/sslr/internal"
)

var args struct {
	configFile string
	continuous bool
}

func main() {
	flag.StringVar(&args.configFile, "cfg", "sslr.json", "SSLR config file")
	flag.BoolVar(&args.continuous, "c", false, "Run continuously")
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

	done := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	var jobError error

	go func() {
	runLoop:
		for {
			jobError = job.Run()
			if jobError != nil {
				break runLoop
			}
			if !args.continuous {
				break runLoop
			}
			select {
			case <-time.After(config.WaitBetweenJobs):
				break
			case <-ctx.Done():
				break runLoop
			}
		}
		cancel()
		close(done)
	}()

	interrupted := false

	select {
	case s := <-signals:
		logger.Info.Printf("Received signal %v\n", s)
		interrupted = true
		break
	case <-ctx.Done():
		break
	}

	if jobError != nil && !interrupted {
		logger.Error.Printf("Job failed: %v\n", jobError)
		os.Exit(3)
	}

	cancel()
	<-done
}
