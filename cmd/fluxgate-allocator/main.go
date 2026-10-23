package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	fluxgatev1 "fluxgate/api/proto/fluxgate/v1"
	"fluxgate/internal/allocator"
	"fluxgate/internal/clock"
	"fluxgate/internal/config"
	"fluxgate/internal/telemetry"
	"google.golang.org/grpc"
)

func main() {
	cfgPath := flag.String("config", "configs/allocator.yaml", "Path to allocator configuration YAML file")
	flag.Parse()

	cfg, err := config.LoadAllocatorConfig(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading allocator configuration: %v\n", err)
		os.Exit(1)
	}

	logger := telemetry.NewLogger(os.Stdout, telemetry.LevelInfo, 1, map[string]interface{}{
		"component": "allocator",
	})
	realClock := clock.NewRealClock()

	policies, err := config.LoadPoliciesFile(cfg.PolicyFilePath)
	if err != nil {
		logger.Error("Failed to load policies file", "path", cfg.PolicyFilePath, "error", err)
		os.Exit(1)
	}

	coord, err := allocator.NewCoordinator(policies, realClock, 30*time.Second)
	if err != nil {
		logger.Error("Failed to initialize coordinator", "error", err)
		os.Exit(1)
	}

	grpcServer := allocator.NewGRPCServer(coord, logger)

	// 1. Start gRPC listener
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		logger.Error("Failed to bind gRPC port", "port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}

	server := grpc.NewServer()
	fluxgatev1.RegisterQuotaAllocatorServiceServer(server, grpcServer)

	go func() {
		logger.Info("Quota Allocator gRPC Server listening", "port", cfg.GRPCPort, "epoch", coord.EpochID())
		if err := server.Serve(grpcLis); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// 2. Start HTTP metrics listener
	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"healthy","epoch":"%s"}`, coord.EpochID())
	})
	metricsMux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		ok, _ := coord.CheckConservationStatus()
		fmt.Fprintf(w, `{"status":"operational","epoch":"%s","conservation_satisfied":%v}`, coord.EpochID(), ok)
	})

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPMetricsPort),
		Handler: metricsMux,
	}

	go func() {
		logger.Info("Allocator HTTP metrics listening", "port", cfg.HTTPMetricsPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP metrics error", "error", err)
		}
	}()

	// Trap signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down Quota Allocator...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	server.GracefulStop()
	logger.Info("Quota Allocator stopped cleanly")
}
