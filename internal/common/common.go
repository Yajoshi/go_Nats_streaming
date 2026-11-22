// Package common holds the subjects, JetStream names and message types
// shared by the EO service and the FLL client.
package common

import (
	"encoding/json"
	"os"
)

const (
	DefaultNATSURL = "nats://localhost:4222"

	// Subjects.
	RequestSubject  = "fll.eo.requests"  // FLL -> EO (persisted in JetStream)
	UpdateSubject   = "eo.fll.updates"   // EO -> FLL (business ACK + progress)
	ResponseSubject = "eo.fll.responses" // EO -> FLL (final result)

	// EventsSubject matches both UpdateSubject and ResponseSubject. It is
	// captured by a second JetStream stream so FLL never misses an event.
	EventsSubject = "eo.fll.>"

	// JetStream objects.
	StreamName       = "EO_REQUESTS" // stores requests (FLL -> EO)
	ConsumerName     = "EO_WORKER"
	EventsStreamName = "EO_EVENTS" // stores updates and responses (EO -> FLL)
)

// Status values used in updates and responses.
const (
	StatusAccepted   = "ACCEPTED"    // business ACK
	StatusInProgress = "IN_PROGRESS" // progress update
	StatusCompleted  = "COMPLETED"   // final, success
	StatusFailed     = "FAILED"      // final, failure
)

// Request is sent by FLL to EO.
type Request struct {
	RequestID string          `json:"requestId"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

// InstallPayload is the payload for type INSTALL_APPLICATION.
type InstallPayload struct {
	Application string `json:"application"`
	Version     string `json:"version"`
}

// Update is published by EO on UpdateSubject (business ACK and progress).
type Update struct {
	RequestID string `json:"requestId"`
	Status    string `json:"status"`
	Stage     string `json:"stage,omitempty"`
	Progress  int    `json:"progress"`
	Message   string `json:"message"`
}

// Response is the final message published by EO on ResponseSubject.
type Response struct {
	RequestID string `json:"requestId"`
	Status    string `json:"status"`
	Message   string `json:"message"`
}

// Env returns the environment variable value or a default.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
