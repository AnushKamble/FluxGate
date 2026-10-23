package httpapi

import (
	"fmt"
	"net/http"
	"time"

	"fluxgate/internal/limiter"
)

const (
	HeaderRateLimitLimit     = "X-RateLimit-Limit"
	HeaderRateLimitRemaining = "X-RateLimit-Remaining"
	HeaderRateLimitReset     = "X-RateLimit-Reset"
	HeaderRetryAfter         = "Retry-After"
	HeaderContentType        = "Content-Type"
)

// ApplyRateLimitHeaders attaches standard rate-limiting headers to the HTTP response.
func ApplyRateLimitHeaders(w http.ResponseWriter, d limiter.Decision) {
	w.Header().Set(HeaderRateLimitLimit, fmt.Sprintf("%d", d.Limit))
	w.Header().Set(HeaderRateLimitRemaining, fmt.Sprintf("%d", d.Remaining))
	w.Header().Set(HeaderRateLimitReset, fmt.Sprintf("%d", d.ResetTime.Unix()))

	if !d.Allowed && d.RetryAfter > 0 {
		retrySec := int64(d.RetryAfter.Round(time.Second).Seconds())
		if retrySec < 1 {
			retrySec = 1
		}
		w.Header().Set(HeaderRetryAfter, fmt.Sprintf("%d", retrySec))
	}
}
