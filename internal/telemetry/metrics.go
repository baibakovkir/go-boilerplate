// Package telemetry собирает ограниченные по кардинальности метрики процессов.
package telemetry

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"go-boilerplate/internal/artifacts"
	"go-boilerplate/internal/database"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics принадлежит одному процессу и не использует глобальный registry.
type Metrics struct {
	Registry      *prometheus.Registry
	Events        *prometheus.CounterVec
	Duration      *prometheus.HistogramVec
	Active        prometheus.Gauge
	StorageErrors *prometheus.CounterVec
}

// New создаёт независимый набор метрик API или worker.
func New() *Metrics {
	metrics := &Metrics{
		Registry: prometheus.NewRegistry(),
		Events: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "boilerplate_job_events_total", Help: "Job lifecycle and delivery events."},
			[]string{"event", "job_type"},
		),
		Duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "boilerplate_job_attempt_duration_seconds",
				Help:    "Duration of worker attempts.",
				Buckets: []float64{.1, 1, 5, 15, 60, 300, 900, 3600},
			},
			[]string{"job_type", "outcome"},
		),
		Active: prometheus.NewGauge(
			prometheus.GaugeOpts{Name: "boilerplate_worker_active_jobs", Help: "Currently executing worker jobs."},
		),
		StorageErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "boilerplate_artifact_errors_total",
				Help: "Object storage operation failures.",
			},
			[]string{"operation"},
		),
	}
	metrics.Registry.MustRegister(
		metrics.Events,
		metrics.Duration,
		metrics.Active,
		metrics.StorageErrors,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return metrics
}

// Event учитывает событие без идентификаторов задач и пользователей в labels.
func (metrics *Metrics) Event(event, kind string) { metrics.Events.WithLabelValues(event, kind).Inc() }

// Serve запускает отдельный HTTP listener. Возвращаемая функция останавливает его.
func (metrics *Metrics) Serve(address string) (func() error, error) {
	if address == "" {
		return func() error { return nil }, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() { _ = server.Serve(listener) }()
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := server.Shutdown(ctx)
		if err != nil {
			server.Close()
		}
		return err
	}, nil
}

// Database регистрирует агрегаты очереди только на стороне API.
func (metrics *Metrics) Database(db *database.Store) {
	metrics.Registry.MustRegister(&databaseCollector{db: db})
}

type databaseCollector struct{ db *database.Store }

func (collector *databaseCollector) Describe(channel chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(collector, channel)
}

func (collector *databaseCollector) Collect(channel chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	up := 1.0
	rows, err := collector.db.Pool.Query(ctx, "SELECT status,count(*) FROM jobs GROUP BY status")
	if err != nil {
		up = 0
	} else {
		for rows.Next() {
			var status string
			var count float64
			if rows.Scan(&status, &count) != nil {
				up = 0
				break
			}
			channel <- prometheus.MustNewConstMetric(
				prometheus.NewDesc("boilerplate_jobs", "Number of jobs by status.", []string{"status"}, nil),
				prometheus.GaugeValue, count, status)
		}
		if rows.Err() != nil {
			up = 0
		}
		rows.Close()
	}
	for _, query := range []struct{ name, sql, help string }{
		{
			"boilerplate_queued_oldest_age_seconds",
			"SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(created_at)),0) FROM jobs WHERE status='queued'",
			"Age of oldest queued job.",
		},
		{"boilerplate_outbox_entries", "SELECT count(*) FROM outbox", "Number of pending outbox entries."},
		{
			"boilerplate_outbox_oldest_age_seconds",
			"SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(available_at)),0) FROM outbox",
			"Age of oldest outbox entry.",
		},
	} {
		var value float64
		if err := collector.db.Pool.QueryRow(ctx, query.sql).Scan(&value); err != nil {
			up = 0
			continue
		}
		channel <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(query.name, query.help, nil, nil), prometheus.GaugeValue, value)
	}
	channel <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("boilerplate_queue_collector_up",
			"Whether queue state was collected successfully.",
			nil,
			nil),

		prometheus.GaugeValue, up)
}

// Storage учитывает ошибки S3 без ключей объектов в labels.
func (metrics *Metrics) Storage(store artifacts.Store) artifacts.Store {
	return &storage{Store: store, metrics: metrics}
}

type storage struct {
	artifacts.Store
	metrics *Metrics
}

func (store *storage) record(op string, err error) {
	if err != nil {
		store.metrics.StorageErrors.WithLabelValues(op).Inc()
	}
}

func (store *storage) Put(ctx context.Context, key string, body io.ReadSeeker, size int64) error {
	err := store.Store.Put(ctx, key, body, size)
	store.record("put", err)
	return err
}

func (store *storage) Open(ctx context.Context, key string) (artifacts.Reader, error) {
	reader, err := store.Store.Open(ctx, key)
	store.record("open", err)
	if err != nil {
		return nil, err
	}
	return &observedReader{Reader: reader, store: store}, nil
}

type observedReader struct {
	artifacts.Reader
	store *storage
}

func (reader *observedReader) Read(buffer []byte) (int, error) {
	count, err := reader.Reader.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		reader.store.record("read", err)
	}
	return count, err
}

func (store *storage) Delete(ctx context.Context, key string) error {
	err := store.Store.Delete(ctx, key)
	store.record("delete", err)
	return err
}

func (store *storage) List(ctx context.Context, cursor string) ([]artifacts.Object, string, error) {
	objects, next, err := store.Store.List(ctx, cursor)
	store.record("list", err)
	return objects, next, err
}

func (store *storage) Check(ctx context.Context) error {
	err := store.Store.Check(ctx)
	store.record("check", err)
	return err
}
