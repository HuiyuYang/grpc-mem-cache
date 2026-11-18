package cache

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced time source for deterministic TTL tests.
type fakeClock struct {
	mu sync.Mutex
	at time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{at: time.Date(2026, time.November, 17, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

func TestSetGet(t *testing.T) {
	store := NewStoreWithClock(newFakeClock().Now)

	store.Set("greeting", []byte("hello"), 0)

	got := store.Get("greeting")
	if got.State != StateFound {
		t.Fatalf("Get state = %v, want StateFound", got.State)
	}
	if string(got.Value) != "hello" {
		t.Fatalf("Get value = %q, want %q", got.Value, "hello")
	}
	if !got.ExpiresAt.IsZero() {
		t.Fatalf("Get expiresAt = %v, want zero time", got.ExpiresAt)
	}
}

func TestSetOverwrites(t *testing.T) {
	store := NewStore()

	store.Set("key", []byte("first"), 0)
	store.Set("key", []byte("second"), 0)

	if got := store.Get("key"); string(got.Value) != "second" {
		t.Fatalf("Get value = %q, want %q", got.Value, "second")
	}
}

func TestGetMissing(t *testing.T) {
	store := NewStore()

	if got := store.Get("missing"); got.State != StateNotFound {
		t.Fatalf("Get state = %v, want StateNotFound", got.State)
	}
}

func TestExpiration(t *testing.T) {
	clock := newFakeClock()
	store := NewStoreWithClock(clock.Now)

	store.Set("session", []byte("token"), 10*time.Second)
	wantExpiry := clock.Now().Add(10 * time.Second)

	got := store.Get("session")
	if got.State != StateFound {
		t.Fatalf("Get before expiry state = %v, want StateFound", got.State)
	}
	if !got.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("Get expiresAt = %v, want %v", got.ExpiresAt, wantExpiry)
	}

	clock.Advance(9 * time.Second)
	if got := store.Get("session"); got.State != StateFound {
		t.Fatalf("Get just before expiry state = %v, want StateFound", got.State)
	}

	// Expiry is inclusive: an entry expires at its exact expiration time.
	clock.Advance(time.Second)
	got = store.Get("session")
	if got.State != StateExpired {
		t.Fatalf("Get at expiry state = %v, want StateExpired", got.State)
	}
	if got.Value != nil {
		t.Fatalf("Get expired value = %q, want nil", got.Value)
	}

	// The expired entry is reclaimed, so the next lookup is a plain miss.
	if got := store.Get("session"); got.State != StateNotFound {
		t.Fatalf("Get after expiry state = %v, want StateNotFound", got.State)
	}
}

func TestOverwriteClearsTTL(t *testing.T) {
	clock := newFakeClock()
	store := NewStoreWithClock(clock.Now)

	store.Set("key", []byte("with-ttl"), time.Second)
	store.Set("key", []byte("no-ttl"), 0)

	clock.Advance(2 * time.Second)

	got := store.Get("key")
	if got.State != StateFound {
		t.Fatalf("Get state = %v, want StateFound", got.State)
	}
	if string(got.Value) != "no-ttl" {
		t.Fatalf("Get value = %q, want %q", got.Value, "no-ttl")
	}
}

func TestZeroAndNegativeTTLNeverExpire(t *testing.T) {
	clock := newFakeClock()
	store := NewStoreWithClock(clock.Now)

	store.Set("zero", []byte("value"), 0)
	store.Set("negative", []byte("value"), -time.Second)

	clock.Advance(24 * time.Hour)

	for _, key := range []string{"zero", "negative"} {
		if got := store.Get(key); got.State != StateFound {
			t.Fatalf("Get(%q) state = %v, want StateFound", key, got.State)
		}
	}
}

func TestDelete(t *testing.T) {
	clock := newFakeClock()
	store := NewStoreWithClock(clock.Now)

	if store.Delete("missing") {
		t.Fatal("Delete(missing) = true, want false")
	}

	store.Set("key", []byte("value"), 0)
	if !store.Delete("key") {
		t.Fatal("Delete(live key) = false, want true")
	}
	if got := store.Get("key"); got.State != StateNotFound {
		t.Fatalf("Get after Delete state = %v, want StateNotFound", got.State)
	}

	store.Set("expired", []byte("value"), time.Second)
	clock.Advance(2 * time.Second)
	if store.Delete("expired") {
		t.Fatal("Delete(expired key) = true, want false")
	}
	if got := store.Get("expired"); got.State != StateNotFound {
		t.Fatalf("Get after deleting expired key state = %v, want StateNotFound", got.State)
	}
}

func TestValuesAreCopied(t *testing.T) {
	store := NewStore()

	input := []byte("hello")
	store.Set("key", input, 0)
	input[0] = 'H'

	first := store.Get("key")
	if string(first.Value) != "hello" {
		t.Fatalf("Get value = %q, want %q", first.Value, "hello")
	}

	first.Value[0] = 'H'
	second := store.Get("key")
	if string(second.Value) != "hello" {
		t.Fatalf("Get value = %q after mutating a previous result, want %q", second.Value, "hello")
	}
}

func TestKeys(t *testing.T) {
	clock := newFakeClock()
	store := NewStoreWithClock(clock.Now)

	if got := store.Keys(); len(got) != 0 {
		t.Fatalf("Keys() on empty store = %v, want no keys", got)
	}

	store.Set("alpha", []byte("value"), 0)
	store.Set("beta", []byte("value"), 10*time.Second)
	store.Set("expired", []byte("value"), time.Second)

	clock.Advance(2 * time.Second)

	got := store.Keys()
	slices.Sort(got)
	want := []string{"alpha", "beta"}
	if !slices.Equal(got, want) {
		t.Fatalf("Keys() = %v, want %v", got, want)
	}

	// Keys reclaims expired entries, so the next lookup is a plain miss.
	if got := store.Get("expired"); got.State != StateNotFound {
		t.Fatalf("Get(expired) state = %v, want StateNotFound", got.State)
	}
}

func TestConcurrentAccess(t *testing.T) {
	store := NewStore()

	const (
		goroutines = 32
		iterations = 200
		keys       = 16
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("key-%d", (g+i)%keys)
				switch i % 5 {
				case 0:
					store.Set(key, []byte(key), 0)
				case 1:
					// Short TTLs make goroutines race on the expired-entry
					// reclamation path as well.
					store.Set(key, []byte(key), time.Nanosecond)
				case 2:
					store.Get(key)
				case 3:
					store.Delete(key)
				case 4:
					store.Keys()
				}
			}
		}(g)
	}
	wg.Wait()

	// The store must remain usable after the concurrent workload.
	store.Set("final", []byte("value"), 0)
	got := store.Get("final")
	if got.State != StateFound || !bytes.Equal(got.Value, []byte("value")) {
		t.Fatalf("Get(final) = %+v, want found value", got)
	}
}
