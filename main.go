package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/freshworks/load-generator/cmd"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigs
		cancel()
	}()

	cmd.Execute(ctx)
}
