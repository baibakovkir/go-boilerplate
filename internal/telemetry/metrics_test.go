package telemetry

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestIndependentRegistries(t *testing.T) {
	first, second := New(), New()
	first.Event("retry", "users.import")
	for index, metrics := range []*Metrics{first, second} {
		response := httptest.NewRecorder()
		promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}).
			ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		if response.Code != 200 {
			t.Fatal(response.Code)
		}
		found := strings.Contains(
			response.Body.String(),
			`boilerplate_job_events_total{event="retry",job_type="users.import"} 1`,
		)
		if found != (index == 0) {
			t.Fatalf("registry isolation: %d", index)
		}
	}
}
