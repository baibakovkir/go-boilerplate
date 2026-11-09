package httpapi

import (
	"net/http/httptest"
	"testing"

	appconfig "go-boilerplate/internal/config"
)

func TestProxyAndOrigin(t *testing.T) {
	server := &Server{
		Config: &appconfig.Config{
			HTTPServer: appconfig.HTTPServerConfig{
				Address:        "localhost:8181",
				TrustedProxies: []string{"10.0.0.0/8"},
				AllowedOrigins: []string{"http://localhost:5173"},
			},
		},
	}
	request := httptest.NewRequest("POST", "http://localhost:8181/test", nil)
	request.RemoteAddr = "192.0.2.1:1234"
	request.Header.Set("X-Forwarded-For", "203.0.113.1")
	if got := server.clientIP(request); got != "192.0.2.1" {
		t.Fatal("trusted spoofed forwarding", got)
	}
	request.RemoteAddr = "10.0.0.2:1234"
	request.Header.Set("X-Forwarded-For", "198.51.100.2, 203.0.113.5, 10.0.0.3")
	if got := server.clientIP(request); got != "203.0.113.5" {
		t.Fatal("incorrect proxy chain", got)
	}
	for _, origin := range []string{"http://localhost:8181", "http://localhost:5173", ""} {
		request.Header.Set("Origin", origin)
		if !server.originAllowed(request) {
			t.Fatal("allowed origin rejected")
		}
	}
	request.Host = "evil.test:8181"
	request.Header.Set("Origin", "http://evil.test:8181")
	if server.originAllowed(request) {
		t.Fatal("client-controlled Host trusted as same origin")
	}
	request.Host = "localhost:8181"
	for _, origin := range []string{
		"null",
		"http://evil.test",
		"https://localhost:5173",
		"http://localhost:5173.evil.test",
	} {

		request.Header.Set("Origin", origin)
		if server.originAllowed(request) {
			t.Fatal("untrusted origin accepted", origin)
		}
	}
}
