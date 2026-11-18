package cachesvc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/HuiyuYang/grpc-mem-cache/internal/cache"
	cachev1 "github.com/HuiyuYang/grpc-mem-cache/proto/cache/v1"
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

// newTestClient starts the service on an in-memory listener and returns a
// client connected to it.
func newTestClient(t *testing.T, clock *fakeClock) cachev1.CacheServiceClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	cachev1.RegisterCacheServiceServer(server, New(cache.NewStoreWithClock(clock.Now)))

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve: %v", err)
		}
	})

	return cachev1.NewCacheServiceClient(conn)
}

func TestSetGetDelete(t *testing.T) {
	client := newTestClient(t, newFakeClock())
	ctx := t.Context()

	if _, err := client.Set(ctx, &cachev1.SetRequest{Key: "greeting", Value: []byte("hello")}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	resp, err := client.Get(ctx, &cachev1.GetRequest{Key: "greeting"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_FOUND {
		t.Fatalf("Get status = %v, want STATUS_FOUND", resp.GetStatus())
	}
	if !bytes.Equal(resp.GetValue(), []byte("hello")) {
		t.Fatalf("Get value = %q, want %q", resp.GetValue(), "hello")
	}
	if resp.GetExpiresAt() != nil {
		t.Fatalf("Get expires_at = %v, want unset for a key without TTL", resp.GetExpiresAt())
	}

	del, err := client.Delete(ctx, &cachev1.DeleteRequest{Key: "greeting"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !del.GetDeleted() {
		t.Fatal("Delete(deleted key) = false, want true")
	}

	resp, err = client.Get(ctx, &cachev1.GetRequest{Key: "greeting"})
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_NOT_FOUND {
		t.Fatalf("Get after delete status = %v, want STATUS_NOT_FOUND", resp.GetStatus())
	}

	del, err = client.Delete(ctx, &cachev1.DeleteRequest{Key: "greeting"})
	if err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
	if del.GetDeleted() {
		t.Fatal("Delete(missing key) = true, want false")
	}
}

func TestGetMissing(t *testing.T) {
	client := newTestClient(t, newFakeClock())

	resp, err := client.Get(t.Context(), &cachev1.GetRequest{Key: "missing"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_NOT_FOUND {
		t.Fatalf("Get status = %v, want STATUS_NOT_FOUND", resp.GetStatus())
	}
	if resp.GetValue() != nil {
		t.Fatalf("Get value = %q, want nil", resp.GetValue())
	}
}

func TestExpiration(t *testing.T) {
	clock := newFakeClock()
	client := newTestClient(t, clock)
	ctx := t.Context()

	if _, err := client.Set(ctx, &cachev1.SetRequest{
		Key:   "session",
		Value: []byte("token"),
		Ttl:   durationpb.New(30 * time.Second),
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	resp, err := client.Get(ctx, &cachev1.GetRequest{Key: "session"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_FOUND {
		t.Fatalf("Get status = %v, want STATUS_FOUND", resp.GetStatus())
	}
	wantExpiry := clock.Now().Add(30 * time.Second)
	if got := resp.GetExpiresAt().AsTime(); !got.Equal(wantExpiry) {
		t.Fatalf("Get expires_at = %v, want %v", got, wantExpiry)
	}

	clock.Advance(30 * time.Second)

	resp, err = client.Get(ctx, &cachev1.GetRequest{Key: "session"})
	if err != nil {
		t.Fatalf("Get at expiry: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_EXPIRED {
		t.Fatalf("Get at expiry status = %v, want STATUS_EXPIRED", resp.GetStatus())
	}
	if resp.GetValue() != nil {
		t.Fatalf("Get at expiry value = %q, want nil", resp.GetValue())
	}

	// The expired entry is reclaimed, so the next lookup is a plain miss.
	resp, err = client.Get(ctx, &cachev1.GetRequest{Key: "session"})
	if err != nil {
		t.Fatalf("Get after expiry: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_NOT_FOUND {
		t.Fatalf("Get after expiry status = %v, want STATUS_NOT_FOUND", resp.GetStatus())
	}
}

func TestZeroTTLDoesNotExpire(t *testing.T) {
	clock := newFakeClock()
	client := newTestClient(t, clock)
	ctx := t.Context()

	if _, err := client.Set(ctx, &cachev1.SetRequest{
		Key:   "unset",
		Value: []byte("value"),
	}); err != nil {
		t.Fatalf("Set with unset TTL: %v", err)
	}
	if _, err := client.Set(ctx, &cachev1.SetRequest{
		Key:   "zero",
		Value: []byte("value"),
		Ttl:   &durationpb.Duration{},
	}); err != nil {
		t.Fatalf("Set with zero TTL: %v", err)
	}

	clock.Advance(24 * time.Hour)

	for _, key := range []string{"unset", "zero"} {
		resp, err := client.Get(ctx, &cachev1.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if resp.GetStatus() != cachev1.GetResponse_STATUS_FOUND {
			t.Fatalf("Get(%q) status = %v, want STATUS_FOUND", key, resp.GetStatus())
		}
	}
}

func TestListKeys(t *testing.T) {
	clock := newFakeClock()
	client := newTestClient(t, clock)
	ctx := t.Context()

	resp, err := client.ListKeys(ctx, &cachev1.ListKeysRequest{})
	if err != nil {
		t.Fatalf("ListKeys on empty cache: %v", err)
	}
	if len(resp.GetKeys()) != 0 {
		t.Fatalf("ListKeys on empty cache = %v, want no keys", resp.GetKeys())
	}

	for _, item := range []struct {
		key string
		ttl time.Duration
	}{
		{key: "alpha"},
		{key: "beta", ttl: 30 * time.Second},
		{key: "expired", ttl: 10 * time.Second},
	} {
		if _, err := client.Set(ctx, &cachev1.SetRequest{
			Key:   item.key,
			Value: []byte("value"),
			Ttl:   durationpb.New(item.ttl),
		}); err != nil {
			t.Fatalf("Set(%q): %v", item.key, err)
		}
	}

	clock.Advance(10 * time.Second)

	resp, err = client.ListKeys(ctx, &cachev1.ListKeysRequest{})
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	got := resp.GetKeys()
	slices.Sort(got)
	want := []string{"alpha", "beta"}
	if !slices.Equal(got, want) {
		t.Fatalf("ListKeys = %v, want %v", got, want)
	}

	// ListKeys reclaims expired entries, so the next lookup is a plain miss.
	gotResp, err := client.Get(ctx, &cachev1.GetRequest{Key: "expired"})
	if err != nil {
		t.Fatalf("Get(expired): %v", err)
	}
	if gotResp.GetStatus() != cachev1.GetResponse_STATUS_NOT_FOUND {
		t.Fatalf("Get(expired) status = %v, want STATUS_NOT_FOUND", gotResp.GetStatus())
	}

	if _, err := client.Delete(ctx, &cachev1.DeleteRequest{Key: "alpha"}); err != nil {
		t.Fatalf("Delete(alpha): %v", err)
	}
	resp, err = client.ListKeys(ctx, &cachev1.ListKeysRequest{})
	if err != nil {
		t.Fatalf("ListKeys after delete: %v", err)
	}
	if got := resp.GetKeys(); !slices.Equal(got, []string{"beta"}) {
		t.Fatalf("ListKeys after delete = %v, want [beta]", got)
	}
}

func TestInvalidArguments(t *testing.T) {
	client := newTestClient(t, newFakeClock())
	ctx := t.Context()

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "Set empty key",
			call: func() error {
				_, err := client.Set(ctx, &cachev1.SetRequest{Key: ""})
				return err
			},
		},
		{
			name: "Get empty key",
			call: func() error {
				_, err := client.Get(ctx, &cachev1.GetRequest{Key: ""})
				return err
			},
		},
		{
			name: "Delete empty key",
			call: func() error {
				_, err := client.Delete(ctx, &cachev1.DeleteRequest{Key: ""})
				return err
			},
		},
		{
			name: "Set negative TTL",
			call: func() error {
				_, err := client.Set(ctx, &cachev1.SetRequest{
					Key: "key",
					Ttl: durationpb.New(-time.Second),
				})
				return err
			},
		},
		{
			name: "Set very negative TTL",
			call: func() error {
				_, err := client.Set(ctx, &cachev1.SetRequest{
					Key: "key",
					Ttl: &durationpb.Duration{Seconds: -10_000_000_000},
				})
				return err
			},
		},
		{
			name: "Set invalid TTL",
			call: func() error {
				_, err := client.Set(ctx, &cachev1.SetRequest{
					Key: "key",
					Ttl: &durationpb.Duration{Seconds: 1, Nanos: -1},
				})
				return err
			},
		},
		{
			name: "Set TTL too large",
			call: func() error {
				_, err := client.Set(ctx, &cachev1.SetRequest{
					Key: "key",
					Ttl: &durationpb.Duration{Seconds: 10_000_000_000},
				})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error = %v (code %v), want InvalidArgument", err, status.Code(err))
			}
		})
	}
}

func TestConcurrentRPCs(t *testing.T) {
	client := newTestClient(t, newFakeClock())
	ctx := t.Context()

	const (
		goroutines = 16
		iterations = 25
	)

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("goroutine-%d-key-%d", g, i)
				value := []byte(key)

				if _, err := client.Set(ctx, &cachev1.SetRequest{Key: key, Value: value}); err != nil {
					errCh <- fmt.Errorf("Set(%q): %w", key, err)
					return
				}
				resp, err := client.Get(ctx, &cachev1.GetRequest{Key: key})
				if err != nil {
					errCh <- fmt.Errorf("Get(%q): %w", key, err)
					return
				}
				if resp.GetStatus() != cachev1.GetResponse_STATUS_FOUND || !bytes.Equal(resp.GetValue(), value) {
					errCh <- fmt.Errorf("Get(%q) = status %v value %q, want FOUND %q", key, resp.GetStatus(), resp.GetValue(), value)
					return
				}
				if _, err := client.Delete(ctx, &cachev1.DeleteRequest{Key: key}); err != nil {
					errCh <- fmt.Errorf("Delete(%q): %w", key, err)
					return
				}
				if _, err := client.ListKeys(ctx, &cachev1.ListKeysRequest{}); err != nil {
					errCh <- fmt.Errorf("ListKeys: %w", err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
