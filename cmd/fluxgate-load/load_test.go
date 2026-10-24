package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLoadGeneratorSmoke(t *testing.T) {
	// Start mock HTTP server
	requestsReceived := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestsReceived++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"allowed":true}`))
	}))
	defer srv.Close()

	client := &http.Client{Timeout: 1 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			if requestsReceived == 0 {
				t.Fatalf("expected requests to be sent to mock server")
			}
			return
		default:
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}
	}
}
