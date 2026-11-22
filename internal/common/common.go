// Package common holds the subjects, JetStream names and message types
// shared by the Consumer service and the REQ client.
package common

import (
	"encoding/json"
	"os"
)

const (
	DefaultNATSURL = "nats://localhost:4222"

	// Subjects.
	RequestSubject  = "fll.eo.requests"  // REQ -> CON  (persisted in JetStream)
	UpdateSubject   = "con.req.updates"   // CON  -> REQ (business ACK + progress)
	ResponseSubject = "con.req.responses" // CON  -> REQ (final result)

	// EventsSubject matches both UpdateSubject and ResponseSubject. It is
	// captured by a second JetStream stream so REQ never misses an event.
	EventsSubject = "con.req.>"

	// JetStream objects.
	StreamName       = "CON_REQUESTS" // stores requests (REQ -> EO)
	ConsumerName     = "EO_WORKER"
	EventsStreamName = "CON_EVENTS" // stores updates and responses (CON  -> FLL)
)

// Status values used in updates and responses.
const (
	StatusAccepted   = "ACCEPTED"    // business ACK
	StatusInProgress = "IN_PROGRESS" // progress update
	StatusCompleted  = "COMPLETED"   // final, success
	StatusFailed     = "FAILED"      // final, failure
)

// Request is sent by REQ to EO.
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

// Update is published by CON  on UpdateSubject (business ACK and progress).
type Update struct {
	RequestID string `json:"requestId"`
	Status    string `json:"status"`
	Stage     string `json:"stage,omitempty"`
	Progress  int    `json:"progress"`
	Message   string `json:"message"`
}

// Response is the final message published by CON  on ResponseSubject.
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
