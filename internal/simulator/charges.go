package simulator

import (
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Charge is the simulator's record of an operation it performed.
type Charge struct {
	Reference      string    `json:"reference"`
	IdempotencyKey string    `json:"idempotency_key"`
	TransactionID  string    `json:"transaction_id"`
	Operation      string    `json:"operation"`
	AmountMinor    int64     `json:"amount_minor"`
	Currency       string    `json:"currency"`
	Status         string    `json:"status"`
	Mode           string    `json:"mode"`
	CreatedAt      time.Time `json:"created_at"`
	ResolvedAt     time.Time `json:"resolved_at,omitempty"`
}

// Store holds charges in memory, keyed by idempotency key.
//
// The simulator is itself idempotent, and that is not a convenience — it is
// what makes the interesting faults survivable. When the provider times out
// after succeeding, the charge must still exist and still be findable under the
// same key, or the caller has no way to discover what happened and the only
// remaining option is to guess.
//
// State is deliberately in memory and not persisted: killing the process is a
// scenario the demo needs, and a provider that forgets is a different failure
// from a provider that lies.
type Store struct {
	mu       sync.RWMutex
	byKey    map[string]*Charge
	byRefKey map[string]string // provider reference -> idempotency key

	// attempts counts how many times each idempotency key has been presented.
	// Fault verdicts are derived per attempt so that retries can eventually get
	// through; see faultKey.
	attempts map[string]int
}

func NewStore() *Store {
	return &Store{
		byKey:    make(map[string]*Charge),
		byRefKey: make(map[string]string),
		attempts: make(map[string]int),
	}
}

// NextAttempt records and returns the attempt number for this key, starting at 1.
func (s *Store) NextAttempt(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[key]++
	return s.attempts[key]
}

// CurrentAttempt returns the attempt number without incrementing, so that the
// pre- and post-work fault checks within a single request agree on which
// attempt they are deciding for.
func (s *Store) CurrentAttempt(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.attempts[key]
}

// GetOrCreate returns the existing charge for key, or creates one from the
// template. The boolean reports whether a new charge was created.
//
// This is the whole idempotency guarantee, and it is done under one lock so
// that two concurrent requests carrying the same key cannot both create.
func (s *Store) GetOrCreate(key string, template Charge) (*Charge, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.byKey[key]; ok {
		clone := *existing
		return &clone, false
	}

	template.IdempotencyKey = key
	template.Reference = "ch_" + uuid.NewString()
	template.CreatedAt = time.Now().UTC()

	stored := template
	s.byKey[key] = &stored
	s.byRefKey[stored.Reference] = key

	clone := stored
	return &clone, true
}

// Get returns the charge for key.
func (s *Store) Get(key string) (*Charge, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c, ok := s.byKey[key]
	if !ok {
		return nil, false
	}
	clone := *c
	return &clone, true
}

// GetByReference returns the charge with the given provider reference.
func (s *Store) GetByReference(reference string) (*Charge, bool) {
	s.mu.RLock()
	key, ok := s.byRefKey[reference]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return s.Get(key)
}

// SetStatus transitions a charge identified by provider reference, used by the
// endpoint that completes a pending or redirect-based authorization.
func (s *Store) SetStatus(reference, status string) (*Charge, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key, ok := s.byRefKey[reference]
	if !ok {
		return nil, false
	}
	c := s.byKey[key]
	c.Status = status
	c.ResolvedAt = time.Now().UTC()

	clone := *c
	return &clone, true
}

// Count reports how many charges exist, which the tests use to assert that a
// retried operation produced exactly one.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byKey)
}

// List returns every charge, oldest first.
//
// Exposed so the count is observable from outside the process. "The provider
// was charged exactly once" is the central claim this simulator exists to
// support, and a claim that can only be checked by an in-process test is one
// nobody else can check at all.
func (s *Store) List() []Charge {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Charge, 0, len(s.byKey))
	for _, c := range s.byKey {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

// Reset clears all state, used between test cases.
func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKey = make(map[string]*Charge)
	s.byRefKey = make(map[string]string)
	s.attempts = make(map[string]int)
}
