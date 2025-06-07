package config

import (
	"os"
	"testing"
	"time"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid single-leader leader config",
			config: Config{
				Mode:   SingleLeader,
				Role:   Leader,
				NodeID: "node1",
				Network: NetworkConfig{
					ListenAddress:    ":3000",
					DialTimeout:      5 * time.Second,
					ReadTimeout:      5 * time.Second,
					WriteTimeout:     5 * time.Second,
					HandshakeTimeout: 3 * time.Second,
				},
				Storage: StorageConfig{
					StorageRoot:  "/tmp/dfs/data",
					MetadataRoot: "/tmp/dfs/metadata",
				},
				Replication: ReplicationConfig{
					Factor: 3,
				},
			},
			wantErr: false,
		},
		{
			name: "valid single-leader follower config",
			config: Config{
				Mode:        SingleLeader,
				Role:       Follower,
				NodeID:     "node2",
				LeaderNodes: []string{"localhost:3000"},
				Network: NetworkConfig{
					ListenAddress:    ":3001",
					DialTimeout:      5 * time.Second,
					ReadTimeout:      5 * time.Second,
					WriteTimeout:     5 * time.Second,
					HandshakeTimeout: 3 * time.Second,
				},
				Storage: StorageConfig{
					StorageRoot:  "/tmp/dfs/data",
					MetadataRoot: "/tmp/dfs/metadata",
				},
				Replication: ReplicationConfig{
					Factor: 3,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid mode",
			config: Config{
				Mode:   NodeMode("invalid"),
				Role:   Leader,
				NodeID: "node1",
				Network: NetworkConfig{
					ListenAddress:    ":3000",
					DialTimeout:      5 * time.Second,
					ReadTimeout:      5 * time.Second,
					WriteTimeout:     5 * time.Second,
					HandshakeTimeout: 3 * time.Second,
				},
				Storage: StorageConfig{
					StorageRoot:  "/tmp/dfs/data",
					MetadataRoot: "/tmp/dfs/metadata",
				},
				Replication: ReplicationConfig{
					Factor: 3,
				},
			},
			wantErr: true,
		},
		{
			name: "invalid follower without leader nodes",
			config: Config{
				Mode:   SingleLeader,
				Role:   Follower,
				NodeID: "node2",
				Network: NetworkConfig{
					ListenAddress:    ":3001",
					DialTimeout:      5 * time.Second,
					ReadTimeout:      5 * time.Second,
					WriteTimeout:     5 * time.Second,
					HandshakeTimeout: 3 * time.Second,
				},
				Storage: StorageConfig{
					StorageRoot:  "/tmp/dfs/data",
					MetadataRoot: "/tmp/dfs/metadata",
				},
				Replication: ReplicationConfig{
					Factor: 3,
				},
			},
			wantErr: true,
		},
		{
			name: "invalid replication factor",
			config: Config{
				Mode:   SingleLeader,
				Role:   Leader,
				NodeID: "node1",
				Network: NetworkConfig{
					ListenAddress:    ":3000",
					DialTimeout:      5 * time.Second,
					ReadTimeout:      5 * time.Second,
					WriteTimeout:     5 * time.Second,
					HandshakeTimeout: 3 * time.Second,
				},
				Storage: StorageConfig{
					StorageRoot:  "/tmp/dfs/data",
					MetadataRoot: "/tmp/dfs/metadata",
				},
				Replication: ReplicationConfig{
					Factor: 0,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
	configContent := `
mode: "single-leader"
role: "leader"
node_id: "node1"
peer_nodes:
  - "localhost:3001"
  - "localhost:3002"
network:
  listen_address: ":3000"
  advertise_address: "localhost:3000"
  dial_timeout: "5s"
  read_timeout: "5s"
  write_timeout: "5s"
storage:
  storage_root: "/tmp/dfs/data"
  metadata_root: "/tmp/dfs/metadata"
replication:
  factor: 3
  async_replication: false
  consistency_level: "quorum"
`
	tmpfile, err := os.CreateTemp("", "config.*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(configContent)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	// Test loading the config
	config, err := LoadConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify some values
	if config.Mode != SingleLeader {
		t.Errorf("Expected mode %s, got %s", SingleLeader, config.Mode)
	}
	if config.Role != Leader {
		t.Errorf("Expected role %s, got %s", Leader, config.Role)
	}
	if config.NodeID != "node1" {
		t.Errorf("Expected node_id %s, got %s", "node1", config.NodeID)
	}
	if len(config.PeerNodes) != 2 {
		t.Errorf("Expected 2 peer nodes, got %d", len(config.PeerNodes))
	}
}

func TestEnvironmentOverrides(t *testing.T) {
	// Set environment variables
	os.Setenv("DFS_MODE", "multi-leader")
	os.Setenv("DFS_ROLE", "leader")
	os.Setenv("DFS_NODE_ID", "env-node1")
	os.Setenv("DFS_NETWORK_LISTEN_ADDRESS", ":4000")
	defer func() {
		os.Unsetenv("DFS_MODE")
		os.Unsetenv("DFS_ROLE")
		os.Unsetenv("DFS_NODE_ID")
		os.Unsetenv("DFS_NETWORK_LISTEN_ADDRESS")
	}()

	// Create a minimal config file
	configContent := `
network:
  dial_timeout: "5s"
  read_timeout: "5s"
  write_timeout: "5s"
storage:
  storage_root: "/tmp/dfs/data"
  metadata_root: "/tmp/dfs/metadata"
replication:
  factor: 3
`
	tmpfile, err := os.CreateTemp("", "config.*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(configContent)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	// Test loading the config
	config, err := LoadConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify environment variables took precedence
	if config.Mode != MultiLeader {
		t.Errorf("Expected mode %s, got %s", MultiLeader, config.Mode)
	}
	if config.Role != Leader {
		t.Errorf("Expected role %s, got %s", Leader, config.Role)
	}
	if config.NodeID != "env-node1" {
		t.Errorf("Expected node_id %s, got %s", "env-node1", config.NodeID)
	}
	if config.Network.ListenAddress != ":4000" {
		t.Errorf("Expected listen_address %s, got %s", ":4000", config.Network.ListenAddress)
	}
}