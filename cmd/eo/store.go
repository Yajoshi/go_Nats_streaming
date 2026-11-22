package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"example.com/nats-fll-eo/internal/common"
)

// Job states kept in the local job store.
const (
	statePending   = "PENDING"
	stateRunning   = "RUNNING"
	stateCompleted = "COMPLETED"
	stateFailed    = "FAILED"
)

type jobRecord struct {
	Request   common.Request `json:"request"`
	State     string         `json:"state"`
	Message   string         `json:"message"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

func (r jobRecord) terminal() bool {
	return r.State == stateCompleted || r.State == stateFailed
}

// jobStore is a tiny durable job store: one JSON file per requestId.
// EO persists a request here BEFORE acknowledging the JetStream message,
// so a crash after the ACK cannot lose the job, and a redelivered request
// is detected by requestId (idempotency).
type jobStore struct {
	dir string
	mu  sync.Mutex
}

func newJobStore(dir string) (*jobStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &jobStore{dir: dir}, nil
}

func (s *jobStore) path(id string) string {
	// requestId is used as a file name; keep it safe.
	return filepath.Join(s.dir, filepath.Base(id)+".json")
}

// Create stores a new job. created is false if the job already exists.
func (s *jobStore) Create(req common.Request) (rec jobRecord, created bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, err := s.readLocked(req.RequestID); err == nil {
		return existing, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return jobRecord{}, false, err
	}

	rec = jobRecord{Request: req, State: statePending, UpdatedAt: time.Now().UTC()}
	if err := s.writeLocked(rec); err != nil {
		return jobRecord{}, false, err
	}
	return rec, true, nil
}

func (s *jobStore) Update(id, state, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, err := s.readLocked(id)
	if err != nil {
		return err
	}
	rec.State = state
	rec.Message = message
	rec.UpdatedAt = time.Now().UTC()
	return s.writeLocked(rec)
}

// Unfinished returns jobs that were accepted but not completed (crash recovery).
func (s *jobStore) Unfinished() ([]jobRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []jobRecord
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.dir, e.Name()))
		if err != nil {
			continue
		}
		var rec jobRecord
		if json.Unmarshal(b, &rec) != nil {
			continue
		}
		if !rec.terminal() {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (s *jobStore) readLocked(id string) (jobRecord, error) {
	var rec jobRecord
	b, err := os.ReadFile(s.path(id))
	if err != nil {
		return rec, err
	}
	err = json.Unmarshal(b, &rec)
	return rec, err
}

func (s *jobStore) writeLocked(rec jobRecord) error {
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(rec.Request.RequestID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(rec.Request.RequestID))
}
