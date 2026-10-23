package httpapi

import (
	"errors"
	"strings"
)

// CheckRequest is the payload sent to POST /api/v1/check.
type CheckRequest struct {
	ClientID string `json:"client_id"`
	Route    string `json:"route,omitempty"`
	Method   string `json:"method,omitempty"`
	Cost     uint64 `json:"cost,omitempty"`
}

func (r *CheckRequest) Validate() error {
	client := strings.TrimSpace(r.ClientID)
	if client == "" {
		return errors.New("client_id cannot be empty")
	}
	if len(client) > 128 {
		return errors.New("client_id exceeds maximum length of 128 characters")
	}
	if len(r.Route) > 256 {
		return errors.New("route exceeds maximum length of 256 characters")
	}
	if r.Cost > 10000 {
		return errors.New("cost exceeds maximum allowed single-request cost (10000)")
	}
	return nil
}

// CheckResponse is the response returned by POST /api/v1/check.
type CheckResponse struct {
	Allowed           bool   `json:"allowed"`
	ClientID          string `json:"client_id"`
	Remaining         uint64 `json:"remaining"`
	Limit             uint64 `json:"limit"`
	NodeID            string `json:"node_id"`
	Reason            string `json:"reason,omitempty"`
	RetryAfterSeconds int64  `json:"retry_after_seconds,omitempty"`
	Mode              string `json:"mode,omitempty"`
}

// ErrorResponse is a standard error payload.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}
