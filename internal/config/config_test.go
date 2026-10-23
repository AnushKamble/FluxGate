package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPolicyValidation(t *testing.T) {
	p := PolicyDefinition{
		ID:         "test-pol",
		Version:    1,
		Capacity:   100,
		RefillRate: 10,
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("expected valid policy, got: %v", err)
	}

	invalid := PolicyDefinition{
		ID:         "",
		Version:    1,
		Capacity:   100,
		RefillRate: 10,
	}
	if err := invalid.Validate(); err == nil {
		t.Fatalf("expected error for empty id")
	}

	invalidCap := PolicyDefinition{
		ID:         "p2",
		Version:    1,
		Capacity:   0,
		RefillRate: 10,
	}
	if err := invalidCap.Validate(); err == nil {
		t.Fatalf("expected error for zero capacity")
	}
}

func TestLoadPoliciesFileYAML(t *testing.T) {
	yamlContent := `
policies:
  - id: "client-tier-1"
    version: 1
    client_id: "acme"
    route: "/api/v1/*"
    method: "*"
    capacity: 50
    refill_rate: 10
    default_cost: 1
    enabled: true
  - id: "global-default"
    version: 1
    client_id: "*"
    route: "*"
    method: "*"
    capacity: 20
    refill_rate: 2
    default_cost: 1
    enabled: true
`
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "policies.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("write temp file failed: %v", err)
	}

	policies, err := LoadPoliciesFile(path)
	if err != nil {
		t.Fatalf("load policies failed: %v", err)
	}
	if len(policies) != 2 {
		t.Fatalf("expected 2 policies, got %d", len(policies))
	}
	if policies[0].ID != "client-tier-1" || policies[0].Capacity != 50 {
		t.Errorf("unexpected policy[0] content: %+v", policies[0])
	}
}
