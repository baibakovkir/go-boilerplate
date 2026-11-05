package httpapi

import (
	"net/http/httptest"
	"testing"

	"go-boilerplate/internal/config"
)

func TestProxyAndOrigin(t *testing.T) {
	s := &Server{
		Config: &config.Config{
			HTTPServer: config.HTTPServerConfig{
				Address:        "localhost:8181",
				TrustedProxies: []string{"10.0.0.0/8"},
				AllowedOrigins: []string{"http://localhost:5173"},
			},
		},
	}
	r := httptest.NewRequest("POST", "http://localhost:8181/test", nil)
	r.RemoteAddr = "192.0.2.1:1234"
	r.Header.Set("X-Forwarded-For", "203.0.113.1")
	if got := s.clientIP(r); got != "192.0.2.1" {
		t.Fatal("trusted spoofed forwarding", got)
	}
	r.RemoteAddr = "10.0.0.2:1234"
	r.Header.Set("X-Forwarded-For", "198.51.100.2, 203.0.113.5, 10.0.0.3")
	if got := s.clientIP(r); got != "203.0.113.5" {
		t.Fatal("incorrect proxy chain", got)
	}
	for _, origin := range []string{"http://localhost:8181", "http://localhost:5173", ""} {
		r.Header.Set("Origin", origin)
		if !s.originAllowed(r) {
			t.Fatal("allowed origin rejected")
		}
	}
	r.Host = "evil.test:8181"
	r.Header.Set("Origin", "http://evil.test:8181")
	if s.originAllowed(r) {
		t.Fatal("client-controlled Host trusted as same origin")
	}
	r.Host = "localhost:8181"
	for _, origin := range []string{
		"null",
		"http://evil.test",
		"https://localhost:5173",
		"http://localhost:5173.evil.test",
	} {

		r.Header.Set("Origin", origin)
		if s.originAllowed(r) {
			t.Fatal("untrusted origin accepted", origin)
		}
	}
}
