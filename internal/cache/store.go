// Package cache provides a concurrency-safe in-memory key/value store with
// optional per-entry expiration.
package cache

import (
	"bytes"
	"sync"
	"time"
)

// State describes the result of a lookup.
type State uint8

const (
	// StateNotFound means the key is not present in the store.
	StateNotFound State = iota
	// StateFound means the key is present and has not expired.
	StateFound
	// StateExpired means the key was present but its TTL elapsed. Expired
	// entries are removed during the lookup.
	StateExpired
)

// LookupResult is the result of Store.Get.
type LookupResult struct {
	// State reports whether the key was found, missing, or expired.
	State State
	// Value is the stored value when State is StateFound.
	Value []byte
	// ExpiresAt is the expiration time of a found entry. The zero value means
	// the entry never expires.
	ExpiresAt time.Time
}

type entry struct {
	value     []byte
	expiresAt time.Time
}

// expired reports whether the entry has a TTL that has elapsed at now.
func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// Store is a concurrency-safe in-memory key/value store.
//
// Expiration is lazy: entries whose TTL has elapsed are removed when they are
// next read or deleted. There is no background reaper, so expired entries that
// are never accessed stay in memory until the store is discarded.
type Store struct {
	mu      sync.RWMutex
	entries map[string]entry
	now     func() time.Time
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return NewStoreWithClock(time.Now)
}

// NewStoreWithClock returns an empty Store that uses now as its time source.
// It is intended for tests that need deterministic expiration.
func NewStoreWithClock(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{
		entries: make(map[string]entry),
		now:     now,
	}
}

// Set stores value under key, replacing any previous entry.
//
// A positive ttl makes the entry expire after ttl. A zero or negative ttl
// stores the entry without expiration. The value is copied, so later changes
// to value do not affect the stored data.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	e := entry{value: bytes.Clone(value)}
	if ttl > 0 {
		e.expiresAt = s.now().Add(ttl)
	}

	s.mu.Lock()
	s.entries[key] = e
	s.mu.Unlock()
}

// Get looks up key. When the entry's TTL has elapsed, Get removes the entry
// and reports StateExpired.
func (s *Store) Get(key string) LookupResult {
	for {
		s.mu.RLock()
		e, ok := s.entries[key]
		if !ok {
			s.mu.RUnlock()
			return LookupResult{State: StateNotFound}
		}
		if !e.expired(s.now()) {
			result := LookupResult{
				State:     StateFound,
				Value:     bytes.Clone(e.value),
				ExpiresAt: e.expiresAt,
			}
			s.mu.RUnlock()
			return result
		}
		s.mu.RUnlock()

		// The entry has expired. Take the write lock to reclaim it, but first
		// re-check: another goroutine may have replaced it with a live entry
		// or deleted it while this goroutine was between locks.
		s.mu.Lock()
		current, ok := s.entries[key]
		if !ok {
			s.mu.Unlock()
			return LookupResult{State: StateNotFound}
		}
		if !current.expired(s.now()) {
			s.mu.Unlock()
			// The key now holds a live value; retry the read path.
			continue
		}
		delete(s.entries, key)
		s.mu.Unlock()
		return LookupResult{State: StateExpired}
	}
}

// Delete removes key and reports whether a live entry was removed. It returns
// false when the key is missing or already expired; an expired entry is
// reclaimed either way.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok {
		return false
	}
	delete(s.entries, key)
	return !e.expired(s.now())
}

// Keys returns the keys of all live entries. Expired entries are reclaimed and
// omitted. The order of the keys is unspecified.
func (s *Store) Keys() []string {
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.entries))
	for key, e := range s.entries {
		if e.expired(now) {
			delete(s.entries, key)
			continue
		}
		keys = append(keys, key)
	}
	return keys
}
