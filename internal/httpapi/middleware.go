package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"fluxgate/internal/telemetry"
)

const MaxBodyBytes = 64 * 1024 // 64 KB maximum request body

type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriterInterceptor) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func LoggingAndMetricsMiddleware(logger *telemetry.Logger, metrics *telemetry.MetricsRegistry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		rw := &responseWriterInterceptor{ResponseWriter: w, statusCode: http.StatusOK}

		// Enforce maximum body size
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)

		defer func() {
			if rec := recover(); rec != nil {
				rw.statusCode = http.StatusInternalServerError
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(ErrorResponse{
					Error:   "internal_error",
					Message: "Internal server error",
				})
				logger.Error("HTTP Panic Recovered", "panic", rec, "path", r.URL.Path)
			}

			duration := time.Since(t0)
			routeClass := telemetry.SanitizeRouteClass(r.URL.Path)
			metrics.IncHTTPRequest(r.Method, rw.statusCode, routeClass)

			if rw.statusCode >= 500 {
				logger.Error("HTTP Request Error", "method", r.Method, "path", r.URL.Path, "status", rw.statusCode, "duration_us", duration.Microseconds())
			}
		}()

		next.ServeHTTP(rw, r)
	})
}
