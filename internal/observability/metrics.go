package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const Namespace = "ledger"

type Metrics struct {
	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
	HTTPInFlight prometheus.Gauge

	Transfers        *prometheus.CounterVec
	TransferDuration prometheus.Histogram
	TxRetries        prometheus.Counter

	IdempotencyReplays *prometheus.CounterVec

	OutboxPublished prometheus.Counter
	OutboxFailures  prometheus.Counter
	OutboxPending   prometheus.Gauge
	OutboxOldestAge prometheus.Gauge

	ConsumerProcessed *prometheus.CounterVec

	ReconciliationDiscrepancies *prometheus.GaugeVec
	HoldsExpired                prometheus.Counter

	RetentionDeleted   *prometheus.CounterVec
	TableEstimatedRows *prometheus.GaugeVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total number of HTTP requests.",
		}, []string{"method", "route", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request latency.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"method", "route"}),
		HTTPInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "http",
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight HTTP requests.",
		}),
		Transfers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "transfers_total",
			Help:      "Transfers by result.",
		}, []string{"status"}),
		TransferDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "transfer_duration_seconds",
			Help:      "End-to-end transfer use case latency.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}),
		TxRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "tx_retries_total",
			Help:      "Transactions retried after serialization failures or deadlocks.",
		}),
		IdempotencyReplays: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "idempotency_replays_total",
			Help:      "Requests answered from a stored idempotent response.",
		}, []string{"endpoint"}),
		OutboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "outbox_published_total",
			Help:      "Outbox events published to the broker.",
		}),
		OutboxFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "outbox_publish_failures_total",
			Help:      "Outbox publish attempts that failed.",
		}),
		OutboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "outbox_pending_events",
			Help:      "Number of unpublished outbox events.",
		}),
		OutboxOldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "outbox_oldest_event_age_seconds",
			Help:      "Age of the oldest unpublished outbox event.",
		}),
		ConsumerProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "consumer_events_total",
			Help:      "Events processed by the consumer.",
		}, []string{"topic", "result"}),
		ReconciliationDiscrepancies: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "reconciliation_discrepancies",
			Help:      "Discrepancies found by the latest reconciliation run.",
		}, []string{"kind"}),
		HoldsExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "holds_expired_total",
			Help:      "Holds released by the expiry worker.",
		}),
		RetentionDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "retention_deleted_rows_total",
			Help:      "Rows deleted by retention jobs.",
		}, []string{"table"}),
		TableEstimatedRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "table_estimated_rows",
			Help:      "Estimated table row counts from pg_class.reltuples.",
		}, []string{"table"}),
	}

	reg.MustRegister(
		m.HTTPRequests, m.HTTPDuration, m.HTTPInFlight,
		m.Transfers, m.TransferDuration, m.TxRetries,
		m.IdempotencyReplays,
		m.OutboxPublished, m.OutboxFailures, m.OutboxPending, m.OutboxOldestAge,
		m.ConsumerProcessed,
		m.ReconciliationDiscrepancies, m.HoldsExpired,
		m.RetentionDeleted, m.TableEstimatedRows,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}
