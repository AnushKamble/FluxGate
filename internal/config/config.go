package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strings"
)

// PolicyDefinition defines rate-limiting rules and quotas for clients/endpoints.
type PolicyDefinition struct {
	ID          string  `json:"id" yaml:"id"`
	Version     uint32  `json:"version" yaml:"version"`
	ClientID    string  `json:"client_id" yaml:"client_id"`       // Specific client ID or "*"
	Route       string  `json:"route" yaml:"route"`               // Exact or prefix route, or "*"
	Method      string  `json:"method" yaml:"method"`             // HTTP method or "*"
	Capacity    uint64  `json:"capacity" yaml:"capacity"`         // Maximum burst capacity
	RefillRate  float64 `json:"refill_rate" yaml:"refill_rate"`   // Tokens refilled per second
	DefaultCost uint64  `json:"default_cost" yaml:"default_cost"` // Default token cost per request (usually 1)
	Enabled     bool    `json:"enabled" yaml:"enabled"`
}

func (p *PolicyDefinition) Validate() error {
	if strings.TrimSpace(p.ID) == "" {
		return errors.New("policy id cannot be empty")
	}
	if p.Version == 0 {
		return errors.New("policy version must be >= 1")
	}
	if p.Capacity == 0 {
		return errors.New("policy capacity must be > 0")
	}
	if p.RefillRate < 0 {
		return errors.New("policy refill_rate cannot be negative")
	}
	if p.DefaultCost == 0 {
		p.DefaultCost = 1
	}
	if p.ClientID == "" {
		p.ClientID = "*"
	}
	if p.Route == "" {
		p.Route = "*"
	}
	if p.Method == "" {
		p.Method = "*"
	}
	return nil
}

// PoliciesFile wraps a list of policy definitions.
type PoliciesFile struct {
	Policies []PolicyDefinition `json:"policies" yaml:"policies"`
}

func LoadPoliciesFile(path string) ([]PolicyDefinition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read policies file: %w", err)
	}

	var pf PoliciesFile
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".yaml" || ext == ".yml" {
		if err := yaml.Unmarshal(data, &pf); err != nil {
			return nil, fmt.Errorf("failed to parse yaml policies: %w", err)
		}
	} else {
		if err := json.Unmarshal(data, &pf); err != nil {
			return nil, fmt.Errorf("failed to parse json policies: %w", err)
		}
	}

	seenIDs := make(map[string]bool)
	for i := range pf.Policies {
		if err := pf.Policies[i].Validate(); err != nil {
			return nil, fmt.Errorf("policy [%d] invalid: %w", i, err)
		}
		if seenIDs[pf.Policies[i].ID] {
			return nil, fmt.Errorf("duplicate policy ID: %s", pf.Policies[i].ID)
		}
		seenIDs[pf.Policies[i].ID] = true
	}

	return pf.Policies, nil
}

// NodeConfig specifies configuration for a gateway node.
type NodeConfig struct {
	NodeID               string   `json:"node_id" yaml:"node_id"`
	HTTPPort             int      `json:"http_port" yaml:"http_port"`
	GRPCPort             int      `json:"grpc_port" yaml:"grpc_port"`
	AllocatorAddr        string   `json:"allocator_addr" yaml:"allocator_addr"`
	Peers                []string `json:"peers" yaml:"peers"`
	LeaseRefillBatchSize uint64   `json:"lease_refill_batch_size" yaml:"lease_refill_batch_size"`
	LeaseLowWaterRatio   float64  `json:"lease_low_water_ratio" yaml:"lease_low_water_ratio"`
	GossipIntervalMs     int      `json:"gossip_interval_ms" yaml:"gossip_interval_ms"`
	PolicyFilePath       string   `json:"policy_file_path" yaml:"policy_file_path"`
	LogLevel             string   `json:"log_level" yaml:"log_level"`
}

func (c *NodeConfig) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return errors.New("node_id cannot be empty")
	}
	if c.HTTPPort <= 0 || c.HTTPPort > 65535 {
		return fmt.Errorf("invalid http_port: %d", c.HTTPPort)
	}
	if c.GRPCPort <= 0 || c.GRPCPort > 65535 {
		return fmt.Errorf("invalid grpc_port: %d", c.GRPCPort)
	}
	if c.LeaseRefillBatchSize == 0 {
		c.LeaseRefillBatchSize = 10
	}
	if c.LeaseLowWaterRatio <= 0 || c.LeaseLowWaterRatio >= 1.0 {
		c.LeaseLowWaterRatio = 0.3
	}
	if c.GossipIntervalMs <= 0 {
		c.GossipIntervalMs = 500
	}
	if c.LogLevel == "" {
		c.LogLevel = "INFO"
	}
	return nil
}

// AllocatorConfig specifies configuration for the Quota Allocator service.
type AllocatorConfig struct {
	GRPCPort        int    `json:"grpc_port" yaml:"grpc_port"`
	HTTPMetricsPort int    `json:"http_metrics_port" yaml:"http_metrics_port"`
	PolicyFilePath  string `json:"policy_file_path" yaml:"policy_file_path"`
	LogLevel        string `json:"log_level" yaml:"log_level"`
}

func (c *AllocatorConfig) Validate() error {
	if c.GRPCPort <= 0 || c.GRPCPort > 65535 {
		return fmt.Errorf("invalid grpc_port: %d", c.GRPCPort)
	}
	if c.HTTPMetricsPort <= 0 || c.HTTPMetricsPort > 65535 {
		return fmt.Errorf("invalid http_metrics_port: %d", c.HTTPMetricsPort)
	}
	if strings.TrimSpace(c.PolicyFilePath) == "" {
		return errors.New("policy_file_path cannot be empty")
	}
	if c.LogLevel == "" {
		c.LogLevel = "INFO"
	}
	return nil
}

func LoadNodeConfig(path string) (*NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read node config: %w", err)
	}
	var cfg NodeConfig
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".yaml" || ext == ".yml" {
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("yaml parse error: %w", err)
		}
	} else {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("json parse error: %w", err)
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	return &cfg, nil
}

func LoadAllocatorConfig(path string) (*AllocatorConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read allocator config: %w", err)
	}
	var cfg AllocatorConfig
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".yaml" || ext == ".yml" {
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("yaml parse error: %w", err)
		}
	} else {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("json parse error: %w", err)
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	return &cfg, nil
}
