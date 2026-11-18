package cachesvc

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	cachev1 "github.com/HuiyuYang/grpc-mem-cache/proto/cache/v1"
)

// assertListKeys checks the full live key set returned by ListKeys.
func assertListKeys(t *testing.T, ctx context.Context, client cachev1.CacheServiceClient, want []string) {
	t.Helper()

	resp, err := client.ListKeys(ctx, &cachev1.ListKeysRequest{})
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	got := slices.Clone(resp.GetKeys())
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("ListKeys = %v, want %v", got, want)
	}
}

func TestListKeysExpiryBoundaries(t *testing.T) {
	clock := newFakeClock()
	client := newTestClient(t, clock)
	ctx := t.Context()

	for _, item := range []struct {
		key string
		ttl time.Duration
	}{
		{key: "always"},
		{key: "short", ttl: time.Second},
		{key: "boundary", ttl: 5 * time.Second},
		{key: "long", ttl: 10 * time.Second},
	} {
		if _, err := client.Set(ctx, &cachev1.SetRequest{
			Key:   item.key,
			Value: []byte("value"),
			Ttl:   durationpb.New(item.ttl),
		}); err != nil {
			t.Fatalf("Set(%q): %v", item.key, err)
		}
	}

	// Expiry is inclusive: entries expiring exactly now are no longer live.
	clock.Advance(5 * time.Second)
	assertListKeys(t, ctx, client, []string{"always", "long"})

	// ListKeys reclaimed the expired entries, so Get reports a plain miss.
	for _, key := range []string{"short", "boundary"} {
		resp, err := client.Get(ctx, &cachev1.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if resp.GetStatus() != cachev1.GetResponse_STATUS_NOT_FOUND {
			t.Fatalf("Get(%q) status = %v, want STATUS_NOT_FOUND", key, resp.GetStatus())
		}
	}

	clock.Advance(5 * time.Second)
	assertListKeys(t, ctx, client, []string{"always"})
}

func TestListKeysEmptyValuesAndDeletes(t *testing.T) {
	client := newTestClient(t, newFakeClock())
	ctx := t.Context()

	// An entry with an empty value is still live and must be listed.
	if _, err := client.Set(ctx, &cachev1.SetRequest{Key: "empty", Value: []byte{}}); err != nil {
		t.Fatalf("Set(empty): %v", err)
	}
	if _, err := client.Set(ctx, &cachev1.SetRequest{Key: "unicode-键", Value: []byte("值")}); err != nil {
		t.Fatalf("Set(unicode): %v", err)
	}
	assertListKeys(t, ctx, client, []string{"empty", "unicode-键"})

	for _, key := range []string{"empty", "unicode-键"} {
		if _, err := client.Delete(ctx, &cachev1.DeleteRequest{Key: key}); err != nil {
			t.Fatalf("Delete(%q): %v", key, err)
		}
	}
	assertListKeys(t, ctx, client, nil)
}

func TestListKeysLargeResult(t *testing.T) {
	client := newTestClient(t, newFakeClock())
	ctx := t.Context()

	const n = 500
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if _, err := client.Set(ctx, &cachev1.SetRequest{Key: key, Value: []byte("value")}); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
		want = append(want, key)
	}

	assertListKeys(t, ctx, client, want)
}

func TestListKeysConcurrentWithWrites(t *testing.T) {
	client := newTestClient(t, newFakeClock())
	ctx := t.Context()

	const (
		writers    = 8
		readers    = 4
		iterations = 40
	)

	var wg sync.WaitGroup
	errCh := make(chan error, writers+readers)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("writer-%d-key-%d", w, i)
				if _, err := client.Set(ctx, &cachev1.SetRequest{Key: key, Value: []byte("value")}); err != nil {
					errCh <- fmt.Errorf("Set(%q): %w", key, err)
					return
				}
				if i%2 == 0 {
					if _, err := client.Delete(ctx, &cachev1.DeleteRequest{Key: key}); err != nil {
						errCh <- fmt.Errorf("Delete(%q): %w", key, err)
						return
					}
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				resp, err := client.ListKeys(ctx, &cachev1.ListKeysRequest{})
				if err != nil {
					errCh <- fmt.Errorf("ListKeys: %w", err)
					return
				}
				seen := make(map[string]struct{}, len(resp.GetKeys()))
				for _, key := range resp.GetKeys() {
					if !strings.HasPrefix(key, "writer-") {
						errCh <- fmt.Errorf("ListKeys returned unexpected key %q", key)
						return
					}
					if _, ok := seen[key]; ok {
						errCh <- fmt.Errorf("ListKeys returned duplicate key %q", key)
						return
					}
					seen[key] = struct{}{}
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
