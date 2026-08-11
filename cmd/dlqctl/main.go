// Command dlqctl inspects and replays dead-lettered payment messages.
//
//	dlqctl list
//	dlqctl replay <event-id> --actor you@example.com --reason "provider outage resolved"
//	dlqctl replay-all --actor you@example.com --reason "backlog cleared"
//
// A dead letter is a payment that could not be completed. It is never dropped,
// because the alternative is a customer's money moving with no record of why
// nothing happened afterwards. Replaying is a deliberate, attributed act.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
)

const readTimeout = 5 * time.Second

type dlqEntry struct {
	EventID     string
	OriginTopic string
	Reason      string
	Attempt     string
	Key         string
	Payload     []byte
	Timestamp   time.Time
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "dlqctl: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("expected a command: list, replay, replay-all")
	}

	flags := flag.NewFlagSet("dlqctl", flag.ContinueOnError)
	actor := flags.String("actor", "", "who is performing this replay")
	reason := flags.String("reason", "", "why this replay is being performed")

	command := args[0]
	rest := args[1:]

	var target string
	if command == "replay" {
		if len(rest) == 0 {
			return errors.New("replay requires an event id")
		}
		target, rest = rest[0], rest[1:]
	}
	if err := flags.Parse(rest); err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	topics := messaging.DefaultTopics()

	switch command {
	case "list":
		entries, err := readDLQ(cfg.Kafka.Brokers, topics.DLQ)
		if err != nil {
			return err
		}
		printEntries(entries)
		return nil

	case "replay", "replay-all":
		// Replay moves money. Requiring attribution means the audit trail can
		// answer who decided to retry a failed payment and on what grounds —
		// which is the first question asked when a replay goes wrong.
		if *actor == "" || *reason == "" {
			return errors.New("replay requires --actor and --reason")
		}

		entries, err := readDLQ(cfg.Kafka.Brokers, topics.DLQ)
		if err != nil {
			return err
		}
		return replay(cfg.Kafka.Brokers, topics, entries, target, command == "replay-all", *actor, *reason)

	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

// readDLQ reads the topic from the beginning without joining a consumer group.
//
// Deliberately group-less: joining would commit offsets and make the messages
// disappear for the real consumers, and inspecting a dead-letter queue must not
// change it.
func readDLQ(brokers []string, topic string) ([]dlqEntry, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to kafka: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	var entries []dlqEntry
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			// A timeout simply means the end of the topic was reached.
			break
		}

		empty := true
		fetches.EachRecord(func(r *kgo.Record) {
			empty = false
			entries = append(entries, toEntry(r))
		})
		if empty {
			break
		}
	}

	return entries, nil
}

func toEntry(r *kgo.Record) dlqEntry {
	entry := dlqEntry{Key: string(r.Key), Payload: r.Value, Timestamp: r.Timestamp}
	for _, h := range r.Headers {
		switch h.Key {
		case messaging.HeaderEventID:
			entry.EventID = string(h.Value)
		case messaging.HeaderAttempt:
			entry.Attempt = string(h.Value)
		case "dlq-reason":
			entry.Reason = string(h.Value)
		case "dlq-origin-topic":
			entry.OriginTopic = string(h.Value)
		}
	}
	return entry
}

func printEntries(entries []dlqEntry) {
	if len(entries) == 0 {
		fmt.Println("dead letter queue is empty")
		return
	}

	fmt.Printf("%d dead-lettered message(s)\n\n", len(entries))
	for _, e := range entries {
		fmt.Printf("event    %s\n", e.EventID)
		fmt.Printf("  origin   %s\n", e.OriginTopic)
		fmt.Printf("  merchant %s\n", e.Key)
		fmt.Printf("  attempts %s\n", e.Attempt)
		fmt.Printf("  at       %s\n", e.Timestamp.Format(time.RFC3339))
		fmt.Printf("  reason   %s\n", e.Reason)

		var pretty map[string]any
		if err := json.Unmarshal(e.Payload, &pretty); err == nil {
			if encoded, err := json.Marshal(pretty); err == nil {
				fmt.Printf("  payload  %s\n", encoded)
			}
		}
		fmt.Println()
	}
}

// replay republishes dead letters to the topic they came from.
//
// The attempt counter is reset, so a replayed message gets the full retry ladder
// again. That is the intent: a human has judged the original cause resolved, and
// resuming at the last rung would send it straight back to the DLQ.
func replay(brokers []string, topics messaging.Topics, entries []dlqEntry, target string, all bool, actor, reason string) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("connect to kafka: %w", err)
	}
	defer client.Close()

	producer := messaging.NewProducer(client)
	ctx := context.Background()

	replayed := 0
	for _, e := range entries {
		if !all && e.EventID != target {
			continue
		}

		destination := e.OriginTopic
		if destination == "" || destination == topics.DLQ {
			destination = topics.Authorize
		}

		if err := producer.PublishWithHeaders(ctx, destination, e.Key, e.EventID, e.Payload,
			map[string]string{
				messaging.HeaderAttempt: "0",
				"replayed-by":           actor,
				"replay-reason":         reason,
			}); err != nil {
			return fmt.Errorf("replay %s: %w", e.EventID, err)
		}

		fmt.Printf("replayed %s -> %s (by %s: %s)\n", e.EventID, destination, actor, reason)
		replayed++
	}

	if replayed == 0 {
		if all {
			fmt.Println("nothing to replay")
			return nil
		}
		return fmt.Errorf("no dead letter found with event id %s", target)
	}

	fmt.Printf("\n%d message(s) replayed\n", replayed)
	return nil
}
