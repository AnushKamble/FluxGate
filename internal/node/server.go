package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	fluxgatev1 "fluxgate/api/proto/fluxgate/v1"
	"fluxgate/internal/clock"
	"fluxgate/internal/config"
	"fluxgate/internal/crdt"
	"fluxgate/internal/gossip"
	"fluxgate/internal/httpapi"
	"fluxgate/internal/policy"
	"fluxgate/internal/telemetry"
	"google.golang.org/grpc"
)

// GatewayNode represents an autonomous FluxGate gateway node process.
type GatewayNode struct {
	cfg           *config.NodeConfig
	incarnationID string
	clock         clock.Clock
	logger        *telemetry.Logger
	metrics       *telemetry.MetricsRegistry
	telemetry     *crdt.TelemetryStore
	httpServer    *http.Server
	apiServer     *httpapi.Server
	grpcServer    *grpc.Server
	leaseWorker   *LeaseWorker
	gossipWorker  *gossip.GossipWorker
	policyEngine  *policy.Engine
	stopOnce      sync.Once
}

func NewGatewayNode(cfg *config.NodeConfig, clk clock.Clock) (*GatewayNode, error) {
	if clk == nil {
		clk = clock.NewRealClock()
	}

	// Generate unique IncarnationID
	incBytes := make([]byte, 16)
	_, _ = rand.Read(incBytes)
	incID := hex.EncodeToString(incBytes)

	logLevel := telemetry.LevelInfo
	if cfg.LogLevel == "DEBUG" {
		logLevel = telemetry.LevelDebug
	}
	logger := telemetry.NewLogger(nil, logLevel, 1, map[string]interface{}{
		"node_id":        cfg.NodeID,
		"incarnation_id": incID,
	})

	metrics := telemetry.NewMetricsRegistry()
	telemetryStore := crdt.NewTelemetryStore(cfg.NodeID, incID)

	// Load policies
	policies, err := config.LoadPoliciesFile(cfg.PolicyFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load policies: %w", err)
	}

	engine, err := policy.NewEngine(policies, clk, false, 50000)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize policy engine: %w", err)
	}

	// Setup primary lease bucket for default policy
	var primaryPol config.PolicyDefinition
	if len(policies) > 0 {
		primaryPol = policies[0]
	}
	bucket := NewLeaseBucket(
		primaryPol.ID,
		primaryPol.Version,
		primaryPol.Capacity,
		primaryPol.RefillRate,
		cfg.LeaseRefillBatchSize,
		cfg.LeaseLowWaterRatio,
		clk,
	)

	// Register distributed lease bucket with policy engine so incoming HTTP checks
	// consume from central escrowed quota rather than an uncoordinated local bucket.
	if primaryPol.ID != "" {
		engine.RegisterPolicyLimiter(primaryPol.ID, bucket)
	}

	apiServer := httpapi.NewServer(cfg.NodeID, engine, clk, logger, metrics)
	apiServer.SetTelemetryStore(telemetryStore)

	leaseWorker := NewLeaseWorker(
		cfg.NodeID,
		incID,
		cfg.AllocatorAddr,
		bucket,
		logger,
		metrics,
		time.Duration(cfg.GossipIntervalMs)*time.Millisecond,
		func(degraded bool) {
			apiServer.SetDegraded(degraded)
		},
	)

	gossipWorker := gossip.NewGossipWorker(
		cfg.NodeID,
		incID,
		cfg.Peers,
		telemetryStore,
		time.Duration(cfg.GossipIntervalMs)*time.Millisecond,
		logger,
	)

	return &GatewayNode{
		cfg:           cfg,
		incarnationID: incID,
		clock:         clk,
		logger:        logger,
		metrics:       metrics,
		telemetry:     telemetryStore,
		apiServer:     apiServer,
		leaseWorker:   leaseWorker,
		gossipWorker:  gossipWorker,
		policyEngine:  engine,
	}, nil
}

func (n *GatewayNode) Start(ctx context.Context) error {
	// 1. Start gRPC gossip listener
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", n.cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to bind gRPC port %d: %w", n.cfg.GRPCPort, err)
	}

	n.grpcServer = grpc.NewServer()
	gossipService := gossip.NewService(n.cfg.NodeID, n.incarnationID, n.telemetry, n.logger)
	fluxgatev1.RegisterGossipServiceServer(n.grpcServer, gossipService)

	go func() {
		if err := n.grpcServer.Serve(grpcLis); err != nil {
			n.logger.Error("gRPC server terminated", "error", err)
		}
	}()

	// 2. Start Lease & Gossip workers
	if err := n.leaseWorker.Start(ctx); err != nil {
		n.logger.Warn("Lease worker startup warning", "error", err)
	}
	n.gossipWorker.Start(ctx)

	// 3. Start HTTP server
	httpAddr := fmt.Sprintf(":%d", n.cfg.HTTPPort)
	n.httpServer = &http.Server{
		Addr:    httpAddr,
		Handler: n.apiServer.Handler(),
	}

	go func() {
		n.logger.Info("HTTP Gateway listening", "addr", httpAddr)
		if err := n.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			n.logger.Error("HTTP server error", "error", err)
		}
	}()

	return nil
}

func (n *GatewayNode) Stop(ctx context.Context) {
	n.stopOnce.Do(func() {
		n.logger.Info("Shutting down FluxGate Gateway Node...")

		// Stop HTTP listener
		if n.httpServer != nil {
			_ = n.httpServer.Shutdown(ctx)
		}

		// Stop background workers
		if n.leaseWorker != nil {
			n.leaseWorker.Stop()
		}
		if n.gossipWorker != nil {
			n.gossipWorker.Stop()
		}

		// Stop gRPC server
		if n.grpcServer != nil {
			n.grpcServer.GracefulStop()
		}
		n.logger.Info("FluxGate Gateway Node stopped cleanly")
	})
}
