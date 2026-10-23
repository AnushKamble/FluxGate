package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fluxgate/internal/clock"
	"fluxgate/internal/config"
	"fluxgate/internal/node"
)

func main() {
	cfgPath := flag.String("config", "configs/node1.yaml", "Path to node configuration YAML file")
	flag.Parse()

	cfg, err := config.LoadNodeConfig(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading node configuration: %v\n", err)
		os.Exit(1)
	}

	gateway, err := node.NewGatewayNode(cfg, clock.NewRealClock())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create gateway node: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := gateway.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start gateway node: %v\n", err)
		os.Exit(1)
	}

	// Trap shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	gateway.Stop(shutdownCtx)
}
