package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAuthPepper   = "dev-pepper-change-me"
	defaultAdminToken   = "dev-admin-token-change-me"
	minProductionSecret = 32
)

type Config struct {
	Env         string
	ServiceName string
	LogLevel    string

	HTTPAddr   string
	AdminAddr  string
	AdminToken string

	DatabaseURL      string
	DBMaxConns       int32
	DBMinConns       int32
	DBMaxConnLife    time.Duration
	DBMaxConnIdle    time.Duration
	DBConnectTimeout time.Duration

	RedisAddr           string
	RedisPassword       string
	RedisDB             int
	RateLimitRPM        int
	PreAuthRateLimitRPM int
	TrustedProxies      []*net.IPNet

	AuthPepper string

	KafkaBrokers       []string
	KafkaTopic         string
	KafkaDLQTopic      string
	KafkaConsumerGroup string
	KafkaClientID      string

	OutboxBatchSize    int
	OutboxPollInterval time.Duration
	OutboxMaxAttempts  int

	ReconciliationInterval         time.Duration
	ReconciliationStatementTimeout time.Duration
	HoldSweepInterval              time.Duration
	IdempotencyTTL                 time.Duration

	OutboxRetention          time.Duration
	ProcessedEventsRetention time.Duration
	AuditRetention           time.Duration
	ReconciliationRetention  time.Duration

	OTLPEndpoint string
	TraceRatio   float64
	Profiling    bool

	ShutdownTimeout   time.Duration
	ReadHeaderTimeout time.Duration
	RequestTimeout    time.Duration
}

func Load() (Config, error) {
	l := &loader{}
	c := Config{
		Env:                            l.str("LEDGER_ENV", "dev"),
		ServiceName:                    l.str("LEDGER_SERVICE_NAME", "ledger"),
		LogLevel:                       l.str("LEDGER_LOG_LEVEL", "info"),
		HTTPAddr:                       l.str("LEDGER_HTTP_ADDR", ":8080"),
		AdminAddr:                      l.str("LEDGER_ADMIN_ADDR", ":9090"),
		AdminToken:                     l.str("LEDGER_ADMIN_TOKEN", defaultAdminToken),
		DatabaseURL:                    l.str("LEDGER_DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"),
		DBMaxConns:                     int32(l.integer("LEDGER_DB_MAX_CONNS", 20)),
		DBMinConns:                     int32(l.integer("LEDGER_DB_MIN_CONNS", 2)),
		DBMaxConnLife:                  l.duration("LEDGER_DB_MAX_CONN_LIFETIME", time.Hour),
		DBMaxConnIdle:                  l.duration("LEDGER_DB_MAX_CONN_IDLE", 15*time.Minute),
		DBConnectTimeout:               l.duration("LEDGER_DB_CONNECT_TIMEOUT", 5*time.Second),
		RedisAddr:                      l.str("LEDGER_REDIS_ADDR", "localhost:6379"),
		RedisPassword:                  l.str("LEDGER_REDIS_PASSWORD", ""),
		RedisDB:                        l.integer("LEDGER_REDIS_DB", 0),
		RateLimitRPM:                   l.integer("LEDGER_RATE_LIMIT_RPM", 6000),
		PreAuthRateLimitRPM:            l.integer("LEDGER_PREAUTH_RATE_LIMIT_RPM", 600),
		TrustedProxies:                 l.cidrs("LEDGER_TRUSTED_PROXIES"),
		AuthPepper:                     l.str("LEDGER_AUTH_PEPPER", defaultAuthPepper),
		KafkaBrokers:                   l.csv("LEDGER_KAFKA_BROKERS", "localhost:19092"),
		KafkaTopic:                     l.str("LEDGER_KAFKA_TOPIC", "ledger.events.v1"),
		KafkaDLQTopic:                  l.str("LEDGER_KAFKA_DLQ_TOPIC", "ledger.events.v1.dlq"),
		KafkaConsumerGroup:             l.str("LEDGER_KAFKA_CONSUMER_GROUP", "ledger-projection"),
		KafkaClientID:                  l.str("LEDGER_KAFKA_CLIENT_ID", "ledger"),
		OutboxBatchSize:                l.integer("LEDGER_OUTBOX_BATCH_SIZE", 1000),
		OutboxPollInterval:             l.duration("LEDGER_OUTBOX_POLL_INTERVAL", 100*time.Millisecond),
		OutboxMaxAttempts:              l.integer("LEDGER_OUTBOX_MAX_ATTEMPTS", 20),
		ReconciliationInterval:         l.duration("LEDGER_RECONCILIATION_INTERVAL", 15*time.Minute),
		ReconciliationStatementTimeout: l.duration("LEDGER_RECONCILIATION_STATEMENT_TIMEOUT", 5*time.Minute),
		HoldSweepInterval:              l.duration("LEDGER_HOLD_SWEEP_INTERVAL", time.Minute),
		IdempotencyTTL:                 l.duration("LEDGER_IDEMPOTENCY_TTL", 24*time.Hour),
		OutboxRetention:                l.duration("LEDGER_OUTBOX_RETENTION", 7*24*time.Hour),
		ProcessedEventsRetention:       l.duration("LEDGER_PROCESSED_EVENTS_RETENTION", 30*24*time.Hour),
		AuditRetention:                 l.duration("LEDGER_AUDIT_RETENTION", 30*24*time.Hour),
		ReconciliationRetention:        l.duration("LEDGER_RECONCILIATION_RETENTION", 30*24*time.Hour),
		OTLPEndpoint:                   l.str("LEDGER_OTLP_ENDPOINT", ""),
		TraceRatio:                     l.float("LEDGER_TRACE_RATIO", 0.1),
		Profiling:                      l.boolean("LEDGER_PROFILING", false),
		ShutdownTimeout:                l.duration("LEDGER_SHUTDOWN_TIMEOUT", 15*time.Second),
		ReadHeaderTimeout:              l.duration("LEDGER_READ_HEADER_TIMEOUT", 5*time.Second),
		RequestTimeout:                 l.duration("LEDGER_REQUEST_TIMEOUT", 10*time.Second),
	}
	if err := l.err(); err != nil {
		return Config{}, err
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) IsProduction() bool {
	return strings.EqualFold(c.Env, "production") || strings.EqualFold(c.Env, "prod")
}

func (c Config) Warnings() []string {
	var out []string
	if !c.IsProduction() {
		if c.AuthPepper == defaultAuthPepper {
			out = append(out, "LEDGER_AUTH_PEPPER uses the development default")
		}
		if c.AdminToken == defaultAdminToken {
			out = append(out, "LEDGER_ADMIN_TOKEN uses the development default")
		}
	}
	return out
}

func (c Config) validate() error {
	switch {
	case c.DatabaseURL == "":
		return fmt.Errorf("LEDGER_DATABASE_URL is required")
	case c.HTTPAddr == "" || c.AdminAddr == "":
		return fmt.Errorf("HTTP and admin addresses must not be empty")
	case c.HTTPAddr == c.AdminAddr:
		return fmt.Errorf("HTTP and admin addresses must differ")
	case c.DBMaxConns < 2:
		return fmt.Errorf("LEDGER_DB_MAX_CONNS must be at least 2 (relay/reconciliation advisory locks need a connection)")
	case c.DBMinConns < 0 || c.DBMinConns > c.DBMaxConns:
		return fmt.Errorf("LEDGER_DB_MIN_CONNS must be between 0 and LEDGER_DB_MAX_CONNS")
	case c.RateLimitRPM < 0 || c.PreAuthRateLimitRPM < 0:
		return fmt.Errorf("rate limits must not be negative")
	case len(c.KafkaBrokers) == 0:
		return fmt.Errorf("at least one Kafka broker is required")
	case c.OutboxBatchSize <= 0:
		return fmt.Errorf("LEDGER_OUTBOX_BATCH_SIZE must be positive")
	case c.OutboxMaxAttempts <= 0:
		return fmt.Errorf("LEDGER_OUTBOX_MAX_ATTEMPTS must be positive")
	case c.IdempotencyTTL <= 0:
		return fmt.Errorf("LEDGER_IDEMPOTENCY_TTL must be positive")
	case c.ReconciliationStatementTimeout <= 0:
		return fmt.Errorf("LEDGER_RECONCILIATION_STATEMENT_TIMEOUT must be positive")
	case c.OutboxPollInterval <= 0:
		return fmt.Errorf("LEDGER_OUTBOX_POLL_INTERVAL must be positive")
	case c.ReconciliationInterval <= 0:
		return fmt.Errorf("LEDGER_RECONCILIATION_INTERVAL must be positive")
	case c.HoldSweepInterval <= 0:
		return fmt.Errorf("LEDGER_HOLD_SWEEP_INTERVAL must be positive")
	case c.ShutdownTimeout <= 0:
		return fmt.Errorf("LEDGER_SHUTDOWN_TIMEOUT must be positive")
	case c.RequestTimeout <= 0:
		return fmt.Errorf("LEDGER_REQUEST_TIMEOUT must be positive")
	case c.ReadHeaderTimeout <= 0:
		return fmt.Errorf("LEDGER_READ_HEADER_TIMEOUT must be positive")
	case c.DBConnectTimeout <= 0:
		return fmt.Errorf("LEDGER_DB_CONNECT_TIMEOUT must be positive")
	case c.RedisDB < 0:
		return fmt.Errorf("LEDGER_REDIS_DB must not be negative")
	case c.OutboxRetention <= 0 || c.ProcessedEventsRetention <= 0 || c.AuditRetention <= 0 || c.ReconciliationRetention <= 0:
		return fmt.Errorf("retention durations must be positive")
	case math.IsNaN(c.TraceRatio) || c.TraceRatio < 0 || c.TraceRatio > 1:
		return fmt.Errorf("LEDGER_TRACE_RATIO must be between 0 and 1")
	}
	if c.IsProduction() {
		if len(c.AuthPepper) < minProductionSecret || c.AuthPepper == defaultAuthPepper {
			return fmt.Errorf("LEDGER_AUTH_PEPPER must be a non-default secret of at least %d characters in production", minProductionSecret)
		}
		if len(c.AdminToken) < minProductionSecret || c.AdminToken == defaultAdminToken {
			return fmt.Errorf("LEDGER_ADMIN_TOKEN must be a non-default secret of at least %d characters in production", minProductionSecret)
		}
	}
	return nil
}

type loader struct {
	errs []error
}

func (l *loader) err() error {
	if len(l.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %w", errors.Join(l.errs...))
}

func (l *loader) str(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return fallback
}

func (l *loader) integer(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
		return fallback
	}
	return n
}

func (l *loader) float(key string, fallback float64) float64 {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
		return fallback
	}
	return n
}

func (l *loader) boolean(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
		return fallback
	}
	return b
}

func (l *loader) duration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
		return fallback
	}
	return d
}

func (l *loader) csv(key, fallback string) []string {
	return splitCSV(l.str(key, fallback))
}

func (l *loader) cidrs(key string) []*net.IPNet {
	raw := l.str(key, "")
	if raw == "" {
		return nil
	}
	var out []*net.IPNet
	for _, item := range splitCSV(raw) {
		_, network, err := net.ParseCIDR(item)
		if err != nil {
			l.errs = append(l.errs, fmt.Errorf("%s: invalid CIDR %q: %w", key, item, err))
			continue
		}
		out = append(out, network)
	}
	return out
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
