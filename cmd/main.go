package main

import (
	"flag"
	"fmt"
	"os"

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
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	job, err := sslr.NewJob(config)
	if err != nil {
		fmt.Printf("Failed to create job: %v\n", err)
		os.Exit(2)
	}

	err = job.Run()
	if err != nil {
		fmt.Printf("SSLR job failed: %v\n", err)
		os.Exit(3)
	}

	os.Exit(0)
}
