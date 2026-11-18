package cache

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// sortedKeys returns the store's live keys in a stable order.
func sortedKeys(store *Store) []string {
	keys := store.Keys()
	slices.Sort(keys)
	return keys
}

func TestKeysExpiryBoundaries(t *testing.T) {
	clock := newFakeClock()
	store := NewStoreWithClock(clock.Now)

	store.Set("always", []byte("value"), 0)
	store.Set("short", []byte("value"), time.Second)
	store.Set("boundary", []byte("value"), 5*time.Second)
	store.Set("long", []byte("value"), 10*time.Second)

	clock.Advance(5 * time.Second)
	if got, want := sortedKeys(store), []string{"always", "long"}; !slices.Equal(got, want) {
		t.Fatalf("Keys() after 5s = %v, want %v", got, want)
	}

	clock.Advance(5 * time.Second)
	if got, want := sortedKeys(store), []string{"always"}; !slices.Equal(got, want) {
		t.Fatalf("Keys() after 10s = %v, want %v", got, want)
	}
}

func TestKeysReclaimsExpiredEntries(t *testing.T) {
	clock := newFakeClock()
	store := NewStoreWithClock(clock.Now)

	store.Set("gone-1", []byte("value"), time.Second)
	store.Set("gone-2", []byte("value"), time.Second)

	clock.Advance(2 * time.Second)

	if got := store.Keys(); len(got) != 0 {
		t.Fatalf("Keys() with only expired entries = %v, want no keys", got)
	}

	// Keys removed the expired entries, so Get reports a plain miss instead of
	// StateExpired.
	for _, key := range []string{"gone-1", "gone-2"} {
		if got := store.Get(key); got.State != StateNotFound {
			t.Fatalf("Get(%q) after Keys = %v, want StateNotFound", key, got.State)
		}
	}
}

func TestKeysOverwriteAndEmptyValues(t *testing.T) {
	clock := newFakeClock()
	store := NewStoreWithClock(clock.Now)

	// Overwriting with a zero TTL must clear the old TTL.
	store.Set("key", []byte("value"), time.Second)
	store.Set("key", []byte("value"), 0)
	store.Set("empty-value", []byte{}, 0)
	store.Set("unicode-键", []byte("值"), 0)

	clock.Advance(2 * time.Second)

	want := []string{"empty-value", "key", "unicode-键"}
	if got := sortedKeys(store); !slices.Equal(got, want) {
		t.Fatalf("Keys() = %v, want %v", got, want)
	}
}

func TestKeysReturnsAllEntries(t *testing.T) {
	store := NewStore()

	const n = 1000
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		store.Set(key, nil, 0)
		want = append(want, key)
	}

	if got := sortedKeys(store); !slices.Equal(got, want) {
		t.Fatalf("Keys() returned %d keys, want %d", len(got), len(want))
	}
}
