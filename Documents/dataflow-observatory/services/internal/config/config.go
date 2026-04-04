package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka     KafkaConfig     `yaml:"kafka"`
	Redis     RedisConfig     `yaml:"redis"`
	Validator ValidatorConfig `yaml:"validator"`
	Snowflake SnowflakeConfig `yaml:"snowflake"`
}

type KafkaConfig struct {
	Brokers       []string          `yaml:"brokers"`
	Topics        map[string]string `yaml:"topics"`
	ConsumerGroup string            `yaml:"consumer_group"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	DB       int    `yaml:"db"`
	AlertTTL int    `yaml:"alert_ttl_seconds"`
}

type ValidatorConfig struct {
	NullRateThreshold float64 `yaml:"null_rate_threshold"`
	AnomalyZScore     float64 `yaml:"anomaly_z_score"`
	WindowSeconds     int     `yaml:"window_seconds"`
}

type SnowflakeConfig struct {
	Account   string `yaml:"account"`
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	Warehouse string `yaml:"warehouse"`
	Database  string `yaml:"database"`
	Schema    string `yaml:"schema"`
}

// Load reads config.yaml, expanding environment variables in values.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	expanded := os.ExpandEnv(string(raw))
	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
