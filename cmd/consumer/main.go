// Consumer service: pulls requests from NATS JetStream, persists them, acknowledges
// them, processes them, publishes progress updates and a final response.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"example.com/nats-fll-eo/internal/common"
)

type service struct {
	nc    *nats.Conn
	js    nats.JetStreamContext
	store *jobStore

	mu      sync.Mutex
	running map[string]bool // requestIds currently being processed
	sem     chan struct{}   // limits concurrent jobs
	wg      sync.WaitGroup
}

func main() {
	natsURL := common.Env("NATS_URL", common.DefaultNATSURL)
	dataDir := common.Env("CON_DATA_DIR", "./data/jobs")

	store, err := newJobStore(dataDir)
	if err != nil {
		log.Fatalf("job store: %v", err)
	}

	nc, err := nats.Connect(natsURL,
		nats.Name("eo-service"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { log.Printf("NATS disconnected: %v", err) }),
		nats.ReconnectHandler(func(c *nats.Conn) { log.Printf("NATS reconnected to %s", c.ConnectedUrl()) }),
	)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}
	if err := ensureStreamAndConsumer(js); err != nil {
		log.Fatalf("setup: %v", err)
	}

	sub, err := js.PullSubscribe(common.RequestSubject, common.ConsumerName,
		nats.Bind(common.StreamName, common.ConsumerName))
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	svc := &service{
		nc:      nc,
		js:      js,
		store:   store,
		running: map[string]bool{},
		sem:     make(chan struct{}, 4), // up to 4 concurrent installations
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("CON  connected to %s, waiting for REQ requests on %q", natsURL, common.RequestSubject)

	// Resume anything accepted before a previous crash or restart.
	svc.resume()

	for ctx.Err() == nil {
		msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Printf("fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, msg := range msgs {
			svc.handle(msg)
		}
	}

	log.Println("shutting down, waiting for running jobs...")
	svc.wg.Wait()
}

// ensureStreamAndConsumer creates the streams and the durable pull consumer if
// they do not exist yet, so the demo works with a fresh NATS server.
func ensureStreamAndConsumer(js nats.JetStreamContext) error {
	// Stream for events going CON  -> REQ (progress updates and final responses).
	if _, err := js.StreamInfo(common.EventsStreamName); err != nil {
		if !errors.Is(err, nats.ErrStreamNotFound) {
			return err
		}
		_, err = js.AddStream(&nats.StreamConfig{
			Name:      common.EventsStreamName,
			Subjects:  []string{common.EventsSubject},
			Storage:   nats.FileStorage,
			Retention: nats.LimitsPolicy,
			MaxAge:    24 * time.Hour,
		})
		if err != nil {
			return fmt.Errorf("add events stream: %w", err)
		}
		log.Printf("created stream %s", common.EventsStreamName)
	}

	// Stream for requests going REQ -> EO.
	if _, err := js.StreamInfo(common.StreamName); err != nil {
		if !errors.Is(err, nats.ErrStreamNotFound) {
			return err
		}
		_, err = js.AddStream(&nats.StreamConfig{
			Name:      common.StreamName,
			Subjects:  []string{common.RequestSubject},
			Storage:   nats.FileStorage,
			Retention: nats.LimitsPolicy,
			MaxAge:    24 * time.Hour,
		})
		if err != nil {
			return fmt.Errorf("add stream: %w", err)
		}
		log.Printf("created stream %s", common.StreamName)
	}

	if _, err := js.ConsumerInfo(common.StreamName, common.ConsumerName); err != nil {
		if !errors.Is(err, nats.ErrConsumerNotFound) {
			return err
		}
		_, err = js.AddConsumer(common.StreamName, &nats.ConsumerConfig{
			Durable:       common.ConsumerName,
			AckPolicy:     nats.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    10,
			FilterSubject: common.RequestSubject,
		})
		if err != nil {
			return fmt.Errorf("add consumer: %w", err)
		}
		log.Printf("created consumer %s", common.ConsumerName)
	}
	return nil
}

// handle implements: parse -> persist job -> ACK (NATS) -> business ACK -> process.
func (s *service) handle(msg *nats.Msg) {
	var req common.Request
	if err := json.Unmarshal(msg.Data, &req); err != nil || req.RequestID == "" || req.Type == "" {
		log.Printf("invalid request, terminating message: %v", err)
		_ = msg.Term() // never redeliver malformed messages
		if req.RequestID != "" {
			s.publishResponse(req.RequestID, common.StatusFailed, "invalid request")
		}
		return
	}

	log.Printf("received request %s (%s)", req.RequestID, req.Type)

	rec, created, err := s.store.Create(req)
	if err != nil {
		// Could not persist: do not ACK, JetStream will redeliver after AckWait.
		log.Printf("persist failed for %s: %v (will be redelivered)", req.RequestID, err)
		_ = msg.Nak()
		return
	}

	// The job is durably stored, so it is now safe to ACK the JetStream message.
	if err := msg.Ack(); err != nil {
		log.Printf("NATS ACK failed for %s: %v", req.RequestID, err)
		return
	}

	if !created {
		// Redelivery or duplicate: do not install twice.
		if rec.terminal() {
			log.Printf("duplicate of finished request %s, re-sending final response", req.RequestID)
			s.publishResponse(req.RequestID, rec.State, rec.Message)
		} else {
			log.Printf("duplicate of in-flight request %s, ignoring", req.RequestID)
		}
		return
	}

	// Business ACK for the REQ UI.
	s.publishUpdate(common.Update{
		RequestID: req.RequestID,
		Status:    common.StatusAccepted,
		Message:   "Request received and accepted",
	})

	s.start(req)
}

// resume restarts jobs that were accepted but never finished.
func (s *service) resume() {
	jobs, err := s.store.Unfinished()
	if err != nil {
		log.Printf("resume scan failed: %v", err)
		return
	}
	for _, j := range jobs {
		log.Printf("resuming unfinished request %s", j.Request.RequestID)
		s.start(j.Request)
	}
}

func (s *service) start(req common.Request) {
	s.mu.Lock()
	if s.running[req.RequestID] {
		s.mu.Unlock()
		return
	}
	s.running[req.RequestID] = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.sem <- struct{}{}
		defer func() { <-s.sem }()
		defer func() {
			s.mu.Lock()
			delete(s.running, req.RequestID)
			s.mu.Unlock()
		}()
		s.process(req)
	}()
}

type stage struct {
	name     string
	progress int
}

func (s *service) process(req common.Request) {
	log.Printf("processing request %s", req.RequestID)
	_ = s.store.Update(req.RequestID, stateRunning, "")

	if req.Type != "INSTALL_APPLICATION" {
		s.finish(req.RequestID, stateFailed, "unsupported request type: "+req.Type)
		return
	}

	var p common.InstallPayload
	if err := json.Unmarshal(req.Payload, &p); err != nil || p.Application == "" {
		s.finish(req.RequestID, stateFailed, "invalid payload")
		return
	}

	stages := []stage{
		{"VALIDATING", 10},
		{"DOWNLOADING", 40},
		{"INSTALLING", 70},
		{"VERIFYING", 90},
	}

	for _, st := range stages {
		s.publishUpdate(common.Update{
			RequestID: req.RequestID,
			Status:    common.StatusInProgress,
			Stage:     st.name,
			Progress:  st.progress,
			Message:   fmt.Sprintf("%s %s %s", st.name, p.Application, p.Version),
		})

		// Replace this sleep with the real work for the stage.
		time.Sleep(2 * time.Second)

		// Demo hook: lets you test the failure path from the REQ client.
		if p.Application == "fail-me" && st.name == "INSTALLING" {
			s.finish(req.RequestID, stateFailed, "simulated installation failure")
			return
		}
	}

	s.finish(req.RequestID, stateCompleted,
		fmt.Sprintf("Application %s %s installed successfully", p.Application, p.Version))
}

func (s *service) finish(id, state, message string) {
	if err := s.store.Update(id, state, message); err != nil {
		log.Printf("store update failed for %s: %v", id, err)
	}
	status := common.StatusCompleted
	if state == stateFailed {
		status = common.StatusFailed
	}
	s.publishResponse(id, status, message)
	log.Printf("request %s finished: %s", id, status)
}

func (s *service) publishUpdate(u common.Update) {
	s.publishJSON(common.UpdateSubject, u)
}

func (s *service) publishResponse(id, status, message string) {
	s.publishJSON(common.ResponseSubject, common.Response{
		RequestID: id, Status: status, Message: message,
	})
}

func (s *service) publishJSON(subject string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		log.Printf("marshal for %s: %v", subject, err)
		return
	}
	// js.Publish waits for the CON_EVENTS stream to confirm it stored the
	// message, so an event is never silently lost.
	if _, err := s.js.Publish(subject, b); err != nil {
		log.Printf("publish to %s failed: %v", subject, err)
	}
}
