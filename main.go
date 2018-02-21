package main

import (
	"os"

	"github.com/evoila/osb-autoscaler-kafka-nozzle/cli"
)

func main() {
	cli := &cli.CLI{OutStream: os.Stdout, ErrStream: os.Stderr}
	os.Exit(cli.Run(os.Args))
}
