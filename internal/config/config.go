package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env         string
	ServiceName string
	LogLevel    string

	HTTPAddr  string
	AdminAddr string

	DatabaseURL      string
	DBMaxConns       int32
	DBMinConns       int32
	DBMaxConnLife    time.Duration
	DBMaxConnIdle    time.Duration
	DBConnectTimeout time.Duration

	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RateLimitRPM  int

	AuthPepper string

	KafkaBrokers       []string
	KafkaTopic         string
	KafkaDLQTopic      string
	KafkaConsumerGroup string
	KafkaClientID      string

	OutboxBatchSize    int
	OutboxPollInterval time.Duration
	OutboxMaxAttempts  int

	ReconciliationInterval time.Duration
	HoldSweepInterval      time.Duration
	IdempotencyTTL         time.Duration

	OTLPEndpoint string
	TraceRatio   float64
	Profiling    bool

	ShutdownTimeout   time.Duration
	ReadHeaderTimeout time.Duration
	RequestTimeout    time.Duration
}

func Load() (Config, error) {
	c := Config{
		Env:                    env("LEDGER_ENV", "dev"),
		ServiceName:            env("LEDGER_SERVICE_NAME", "ledger"),
		LogLevel:               env("LEDGER_LOG_LEVEL", "info"),
		HTTPAddr:               env("LEDGER_HTTP_ADDR", ":8080"),
		AdminAddr:              env("LEDGER_ADMIN_ADDR", ":9090"),
		DatabaseURL:            env("LEDGER_DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"),
		DBMaxConns:             int32(envInt("LEDGER_DB_MAX_CONNS", 20)),
		DBMinConns:             int32(envInt("LEDGER_DB_MIN_CONNS", 2)),
		DBMaxConnLife:          envDuration("LEDGER_DB_MAX_CONN_LIFETIME", time.Hour),
		DBMaxConnIdle:          envDuration("LEDGER_DB_MAX_CONN_IDLE", 15*time.Minute),
		DBConnectTimeout:       envDuration("LEDGER_DB_CONNECT_TIMEOUT", 5*time.Second),
		RedisAddr:              env("LEDGER_REDIS_ADDR", "localhost:6379"),
		RedisPassword:          env("LEDGER_REDIS_PASSWORD", ""),
		RedisDB:                envInt("LEDGER_REDIS_DB", 0),
		RateLimitRPM:           envInt("LEDGER_RATE_LIMIT_RPM", 6000),
		AuthPepper:             env("LEDGER_AUTH_PEPPER", "dev-pepper-change-me"),
		KafkaBrokers:           splitCSV(env("LEDGER_KAFKA_BROKERS", "localhost:19092")),
		KafkaTopic:             env("LEDGER_KAFKA_TOPIC", "ledger.events.v1"),
		KafkaDLQTopic:          env("LEDGER_KAFKA_DLQ_TOPIC", "ledger.events.v1.dlq"),
		KafkaConsumerGroup:     env("LEDGER_KAFKA_CONSUMER_GROUP", "ledger-projection"),
		KafkaClientID:          env("LEDGER_KAFKA_CLIENT_ID", "ledger"),
		OutboxBatchSize:        envInt("LEDGER_OUTBOX_BATCH_SIZE", 1000),
		OutboxPollInterval:     envDuration("LEDGER_OUTBOX_POLL_INTERVAL", 100*time.Millisecond),
		OutboxMaxAttempts:      envInt("LEDGER_OUTBOX_MAX_ATTEMPTS", 20),
		ReconciliationInterval: envDuration("LEDGER_RECONCILIATION_INTERVAL", 15*time.Minute),
		HoldSweepInterval:      envDuration("LEDGER_HOLD_SWEEP_INTERVAL", time.Minute),
		IdempotencyTTL:         envDuration("LEDGER_IDEMPOTENCY_TTL", 24*time.Hour),
		OTLPEndpoint:           env("LEDGER_OTLP_ENDPOINT", ""),
		TraceRatio:             envFloat("LEDGER_TRACE_RATIO", 0.1),
		Profiling:              envBool("LEDGER_PROFILING", false),
		ShutdownTimeout:        envDuration("LEDGER_SHUTDOWN_TIMEOUT", 15*time.Second),
		ReadHeaderTimeout:      envDuration("LEDGER_READ_HEADER_TIMEOUT", 5*time.Second),
		RequestTimeout:         envDuration("LEDGER_REQUEST_TIMEOUT", 10*time.Second),
	}

	if c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("LEDGER_DATABASE_URL is required")
	}
	if c.HTTPAddr == "" || c.AdminAddr == "" {
		return Config{}, fmt.Errorf("HTTP and admin addresses must not be empty")
	}
	if c.HTTPAddr == c.AdminAddr {
		return Config{}, fmt.Errorf("HTTP and admin addresses must differ")
	}
	if len(c.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("at least one Kafka broker is required")
	}
	if c.OutboxBatchSize <= 0 {
		return Config{}, fmt.Errorf("LEDGER_OUTBOX_BATCH_SIZE must be positive")
	}
	if c.TraceRatio < 0 || c.TraceRatio > 1 {
		return Config{}, fmt.Errorf("LEDGER_TRACE_RATIO must be between 0 and 1")
	}
	return c, nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return n
}

func envFloat(key string, fallback float64) float64 {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return fallback
	}
	return n
}

func envBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return b
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return d
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
