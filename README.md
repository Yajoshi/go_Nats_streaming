NATS request/response demo: FLL <-> EO
======================================

Two NATS subjects plus a persisted request subject, using JetStream:

  fll.eo.requests    FLL -> EO   request (stored in JetStream stream EO_REQUESTS)
  eo.fll.updates     EO  -> FLL  business ACK ("ACCEPTED") and progress updates
  eo.fll.responses   EO  -> FLL  final response (COMPLETED or FAILED)

Every message carries a requestId so FLL can correlate updates and the final
response with the original request.

Prerequisites
-------------
  - Go 1.22 or newer         https://go.dev/dl/
  - Docker (for the NATS server), or a local nats-server binary with JetStream

Steps
-----
1. Unzip and enter the project:

     unzip nats-fll-eo.zip
     cd nats-fll-eo

2. Download dependencies (this adds the NATS client to go.mod and creates go.sum):

     go mod tidy

3. Start NATS with JetStream enabled:

     docker compose up -d

   Without Docker:  nats-server -js

4. Terminal 1 - start EO (it creates the stream and durable consumer if missing):

     go run ./cmd/eo

5. Terminal 2 - send a request as FLL:

     go run ./cmd/fll

   Expected output:

     [SENT]        req-... stored in stream EO_REQUESTS (seq 1)
     [ACCEPTED]    Request received and accepted
     [IN_PROGRESS]  10%  VALIDATING   VALIDATING companionapp 1.2.0
     [IN_PROGRESS]  40%  DOWNLOADING  DOWNLOADING companionapp 1.2.0
     [IN_PROGRESS]  70%  INSTALLING   INSTALLING companionapp 1.2.0
     [IN_PROGRESS]  90%  VERIFYING    VERIFYING companionapp 1.2.0
     [COMPLETED] Application companionapp 1.2.0 installed successfully

FLL options:

     go run ./cmd/fll -app companionapp -version 2.0.0 -id req-10001 -timeout 90s

Optional: build binaries

     go build -o bin/eo ./cmd/eo
     go build -o bin/fll ./cmd/fll

Things worth trying
-------------------
  - Failure path:      go run ./cmd/fll -app fail-me
  - EO offline:        stop EO, run the FLL client (the request is stored in the
                       stream), then start EO. It picks the request up.
                       (Start EO once first so the stream exists.)
  - Duplicate request: run FLL twice with the same -id. EO does not run the job
                       twice; it re-sends the final response for a finished job.
  - Crash recovery:    kill EO during a job, restart it. Jobs that were accepted
                       but not finished are resumed from ./data/jobs.

Durable events (FLL never misses an update)
-------------------------------------------
EO publishes updates and responses into a second JetStream stream, EO_EVENTS
(subject eo.fll.>). FLL reads it through a JetStream consumer.

  - Default test client: live view of new events only.
  - With -durable NAME: a durable consumer that remembers its position. Events
    published while FLL was down are delivered when it reconnects.
  - With -replay: replay everything stored in the stream (filtered by -id).

Try the catch-up behaviour:

  1. Terminal 2:  go run ./cmd/fll -durable fll-ui -id req-20001
  2. Press Ctrl+C in that window while it is at DOWNLOADING or INSTALLING.
  3. Wait about 10 seconds (EO keeps working and finishes the job).
  4. Terminal 2:  go run ./cmd/fll -durable fll-ui -id req-20001 -no-send
     The events published while FLL was away are delivered, ending with COMPLETED.

Use -no-send so the request is not sent a second time (a re-sent id is safely
ignored by EO anyway, but -no-send is the clean way to only listen).

Note: if you run EO from an earlier version, just restart it once so it creates
the EO_EVENTS stream. FLL needs that stream to exist before it subscribes.

Configuration (environment variables)
-------------------------------------
  NATS_URL       default nats://localhost:4222
  EO_DATA_DIR    default ./data/jobs   (EO job store, one JSON file per requestId)

How acknowledgement works
-------------------------
EO implements both kinds of ACK:

  1. NATS ACK (JetStream). EO first writes the request to its own job store,
     then calls msg.Ack(). If the write fails, EO calls Nak() and JetStream
     redelivers. Malformed messages are Term()'d so they are never redelivered.
  2. Business ACK. Right after the NATS ACK, EO publishes a message with
     status ACCEPTED on eo.fll.updates, so the UI can show "request accepted".

Idempotency: the job store is keyed by requestId, so a redelivered or
duplicated request cannot trigger the installation twice.

Project layout
--------------
  docker-compose.yml          NATS server with JetStream
  internal/common/common.go   subjects, stream names, message types
  cmd/eo/main.go              EO service (pull consumer, ACKs, processing)
  cmd/eo/store.go             file-based job store (durability + idempotency)
  cmd/fll/main.go             FLL test client

Where to plug in real work
--------------------------
In cmd/eo/main.go, function process(): replace the time.Sleep(2 * time.Second)
inside the stage loop with the real work for each stage (validate, download,
install, verify). Return failures through s.finish(id, stateFailed, "reason").

Production notes
----------------
  - Updates and final responses are stored in the EO_EVENTS stream (subject
    eo.fll.>), kept for 24 hours. A real FLL service should use a durable
    consumer (see "Durable events" above) with its own unique name per instance.
  - Replace the file job store with a database if EO runs as several replicas.
  - Add NATS authentication and TLS (nats.UserCredentials, nats.Secure).
