// REQ test client: publishes one request to CON  and prints the business ACK,
// progress updates and final response for that requestId.
//
// Events from CON  are read from the CON_EVENTS JetStream stream. With -durable,
// the client uses a durable consumer, so events published while the client was
// down or disconnected are delivered when it comes back.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"example.com/nats-fll-eo/internal/common"
)

func main() {
	app := flag.String("app", "companionapp", "application to install")
	version := flag.String("version", "1.2.0", "application version")
	id := flag.String("id", "", "requestId (default: generated)")
	timeout := flag.Duration("timeout", 60*time.Second, "how long to wait for the final response")
	durable := flag.String("durable", "", "durable consumer name; survives restarts and catches up on missed events")
	replay := flag.Bool("replay", false, "without -durable: also replay stored events from before this run")
	noSend := flag.Bool("no-send", false, "do not send a request; only listen for events of -id")
	flag.Parse()

	reqID := *id
	if reqID == "" {
		reqID = fmt.Sprintf("req-%d", time.Now().UnixNano()/int64(time.Millisecond))
	}

	nc, err := nats.Connect(common.Env("NATS_URL", common.DefaultNATSURL), nats.Name("fll-client"))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	done := make(chan common.Response, 1)
	isDurable := *durable != ""

	// One subscription on con.req.> keeps updates and the final response in order.
	// Events for other requestIds are filtered out.
	handler := func(m *nats.Msg) {
		if isDurable {
			// Acknowledge every event, including ones for other requests,
			// so the durable consumer moves forward.
			defer m.Ack()
		}

		switch m.Subject {
		case common.UpdateSubject:
			var u common.Update
			if json.Unmarshal(m.Data, &u) != nil || u.RequestID != reqID {
				return
			}
			if u.Status == common.StatusAccepted {
				fmt.Printf("[ACCEPTED]    %s\n", u.Message)
			} else {
				fmt.Printf("[%-11s] %3d%%  %-12s %s\n", u.Status, u.Progress, u.Stage, u.Message)
			}
		case common.ResponseSubject:
			var r common.Response
			if json.Unmarshal(m.Data, &r) != nil || r.RequestID != reqID {
				return
			}
			select {
			case done <- r:
			default:
			}
		}
	}

	opts := []nats.SubOpt{nats.BindStream(common.EventsStreamName)}
	switch {
	case isDurable:
		// Created on first use; afterwards it resumes where it left off.
		opts = append(opts, nats.Durable(*durable), nats.DeliverAll(), nats.AckExplicit(), nats.ManualAck())
	case *replay:
		opts = append(opts, nats.DeliverAll(), nats.AckNone())
	default:
		opts = append(opts, nats.DeliverNew(), nats.AckNone())
	}

	// Subscribe BEFORE publishing the request.
	sub, err := js.Subscribe(common.EventsSubject, handler, opts...)
	if err != nil {
		log.Fatalf("subscribe events (is CON_EVENTS stream created? start CON  once first): %v", err)
	}
	defer sub.Unsubscribe()

	if isDurable {
		fmt.Printf("[LISTENING]   durable consumer %q on stream %s\n", *durable, common.EventsStreamName)
	}

	if !*noSend {
		payload, _ := json.Marshal(common.InstallPayload{Application: *app, Version: *version})
		req := common.Request{RequestID: reqID, Type: "INSTALL_APPLICATION", Payload: payload}
		data, _ := json.Marshal(req)

		// js.Publish waits for the stream's persistence ACK, so REQ knows the
		// request is stored even if CON  is down right now.
		pubAck, err := js.Publish(common.RequestSubject, data)
		if err != nil {
			log.Fatalf("publish request (is CON_REQUESTS stream created? start CON  once first): %v", err)
		}
		fmt.Printf("[SENT]        %s stored in stream %s (seq %d)\n", reqID, pubAck.Stream, pubAck.Sequence)
	}

	select {
	case r := <-done:
		fmt.Printf("[%s] %s\n", r.Status, r.Message)
		if r.Status != common.StatusCompleted {
			os.Exit(1)
		}
	case <-time.After(*timeout):
		fmt.Println("timed out waiting for the final response")
		os.Exit(2)
	}
}
