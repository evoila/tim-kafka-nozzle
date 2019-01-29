package main

import (
	"os"

	"github.com/evoila/tim-kafka-nozzle/cli"
)

func main() {
	cli := &cli.CLI{OutStream: os.Stdout, ErrStream: os.Stderr}
	os.Exit(cli.Run(os.Args))
}
