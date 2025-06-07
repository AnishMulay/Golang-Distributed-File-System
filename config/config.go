package config

import (
        "fmt"
        "strings"
        "time"

        "github.com/spf13/viper"
)

// NodeMode represents the operational mode of a node
type NodeMode string

const (
        // SingleLeader mode has one leader and multiple followers
        SingleLeader NodeMode = "single-leader"
        // MultiLeader mode allows multiple leader nodes
        MultiLeader NodeMode = "multi-leader"
        // Leaderless mode operates without designated leaders
        Leaderless NodeMode = "leaderless"
)

// NodeRole represents the role of a node in the cluster
type NodeRole string

const (
        // Leader role for leader nodes
        Leader NodeRole = "leader"
        // Follower role for follower nodes
        Follower NodeRole = "follower"
)

// NetworkConfig holds network-related configuration
type NetworkConfig struct {
        // ListenAddress is the address to listen on (e.g., ":3000")
        ListenAddress string `mapstructure:"listen_address"`
        // AdvertiseAddress is the address advertised to other nodes
        AdvertiseAddress string `mapstructure:"advertise_address"`
        // Timeouts
        DialTimeout        time.Duration `mapstructure:"dial_timeout"`
        ReadTimeout        time.Duration `mapstructure:"read_timeout"`
        WriteTimeout       time.Duration `mapstructure:"write_timeout"`
        HandshakeTimeout   time.Duration `mapstructure:"handshake_timeout"`
        // Retry settings
        MaxRetries         int           `mapstructure:"max_retries"`
        RetryBackoff      time.Duration `mapstructure:"retry_backoff"`
}

// StorageConfig holds storage-related configuration
type StorageConfig struct {
        // Root directory for file storage
        StorageRoot string `mapstructure:"storage_root"`
        // MetadataRoot directory for metadata storage
        MetadataRoot string `mapstructure:"metadata_root"`
        // File permissions for new files
        DefaultFileMode uint32 `mapstructure:"default_file_mode"`
        // Maximum file size
        MaxFileSize int64 `mapstructure:"max_file_size"`
}

// ReplicationConfig holds replication-related configuration
type ReplicationConfig struct {
        // Factor determines how many copies of each file should exist
        Factor int `mapstructure:"factor"`
        // AsyncReplication enables asynchronous replication
        AsyncReplication bool `mapstructure:"async_replication"`
        // AsyncQueueSize is the size of the async replication queue
        AsyncQueueSize int `mapstructure:"async_queue_size"`
        // ConsistencyLevel for read/write operations (e.g., "one", "quorum", "all")
        ConsistencyLevel string `mapstructure:"consistency_level"`
}

// Config holds the complete configuration for a node
type Config struct {
        // Node configuration
        Mode NodeMode `mapstructure:"mode"`
        Role NodeRole `mapstructure:"role"`
        // Node ID must be unique in the cluster
        NodeID string `mapstructure:"node_id"`
        // List of peer nodes
        PeerNodes []string `mapstructure:"peer_nodes"`
        // Leader nodes (required for follower nodes in single-leader mode)
        LeaderNodes []string `mapstructure:"leader_nodes"`
        // Component configurations
        Network     NetworkConfig     `mapstructure:"network"`
        Storage     StorageConfig     `mapstructure:"storage"`
        Replication ReplicationConfig `mapstructure:"replication"`
}

// Validate performs validation of the configuration
func (c *Config) Validate() error {
        // Validate node mode
        switch c.Mode {
        case SingleLeader, MultiLeader, Leaderless:
                // Valid mode
        default:
                return fmt.Errorf("invalid node mode: %s", c.Mode)
        }

        // Validate node role
        switch c.Role {
        case Leader, Follower:
                // Valid role
        default:
                return fmt.Errorf("invalid node role: %s", c.Role)
        }

        // Validate mode-specific requirements
        if c.Mode == SingleLeader {
                if c.Role == Follower && len(c.LeaderNodes) == 0 {
                        return fmt.Errorf("follower nodes in single-leader mode must specify leader_nodes")
                }
                if c.Role == Leader && len(c.LeaderNodes) > 0 {
                        return fmt.Errorf("leader nodes should not specify leader_nodes")
                }
        }

        // Validate replication factor
        if c.Replication.Factor < 1 {
                return fmt.Errorf("replication factor must be at least 1")
        }

        // Validate network configuration
        if c.Network.ListenAddress == "" {
                return fmt.Errorf("listen_address is required")
        }
        if c.Network.DialTimeout <= 0 {
                return fmt.Errorf("dial_timeout must be positive")
        }
        if c.Network.ReadTimeout <= 0 {
                return fmt.Errorf("read_timeout must be positive")
        }
        if c.Network.WriteTimeout <= 0 {
                return fmt.Errorf("write_timeout must be positive")
        }

        // Validate storage configuration
        if c.Storage.StorageRoot == "" {
                return fmt.Errorf("storage_root is required")
        }
        if c.Storage.MetadataRoot == "" {
                return fmt.Errorf("metadata_root is required")
        }

        return nil
}

// LoadConfig loads the configuration from file and environment variables
func LoadConfig(configPath string) (*Config, error) {
        v := viper.New()

        // Set default values
        v.SetDefault("mode", Leaderless)
        v.SetDefault("role", Leader)
        v.SetDefault("network.dial_timeout", "5s")
        v.SetDefault("network.read_timeout", "5s")
        v.SetDefault("network.write_timeout", "5s")
        v.SetDefault("network.handshake_timeout", "3s")
        v.SetDefault("network.max_retries", 3)
        v.SetDefault("network.retry_backoff", "1s")
        v.SetDefault("storage.default_file_mode", 0644)
        v.SetDefault("storage.max_file_size", int64(1<<30)) // 1GB
        v.SetDefault("replication.factor", 3)
        v.SetDefault("replication.async_replication", false)
        v.SetDefault("replication.async_queue_size", 1000)
        v.SetDefault("replication.consistency_level", "quorum")

        // Set up environment variable support
        v.SetEnvPrefix("DFS")
        v.AutomaticEnv()
        v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

        // Read config file if provided
        if configPath != "" {
                v.SetConfigFile(configPath)
                if err := v.ReadInConfig(); err != nil {
                        return nil, fmt.Errorf("error reading config file: %w", err)
                }
        }

        var config Config
        if err := v.Unmarshal(&config); err != nil {
                return nil, fmt.Errorf("error unmarshaling config: %w", err)
        }

        // Validate the configuration
        if err := config.Validate(); err != nil {
                return nil, fmt.Errorf("invalid configuration: %w", err)
        }

        return &config, nil
}