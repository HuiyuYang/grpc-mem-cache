package cachesvc

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	cachev1 "github.com/HuiyuYang/grpc-mem-cache/proto/cache/v1"
)

// TestSetOverwriteClearsTTL covers Set replacing an existing entry: the new
// value must win and the previous TTL must be dropped.
func TestSetOverwriteClearsTTL(t *testing.T) {
	clock := newFakeClock()
	client := newTestClient(t, clock)
	ctx := t.Context()

	if _, err := client.Set(ctx, &cachev1.SetRequest{
		Key:   "key",
		Value: []byte("first"),
		Ttl:   durationpb.New(10 * time.Second),
	}); err != nil {
		t.Fatalf("Set(first): %v", err)
	}
	if _, err := client.Set(ctx, &cachev1.SetRequest{
		Key:   "key",
		Value: []byte("second"),
	}); err != nil {
		t.Fatalf("Set(second): %v", err)
	}

	// The first write's TTL would have elapsed by now.
	clock.Advance(10 * time.Second)

	resp, err := client.Get(ctx, &cachev1.GetRequest{Key: "key"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_FOUND {
		t.Fatalf("Get status = %v, want STATUS_FOUND", resp.GetStatus())
	}
	if !bytes.Equal(resp.GetValue(), []byte("second")) {
		t.Fatalf("Get value = %q, want %q", resp.GetValue(), "second")
	}
	if resp.GetExpiresAt() != nil {
		t.Fatalf("Get expires_at = %v, want unset", resp.GetExpiresAt())
	}
}

// TestSetEmptyValue covers storing an empty value: it is a valid live entry.
func TestSetEmptyValue(t *testing.T) {
	client := newTestClient(t, newFakeClock())
	ctx := t.Context()

	if _, err := client.Set(ctx, &cachev1.SetRequest{Key: "empty", Value: []byte{}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	resp, err := client.Get(ctx, &cachev1.GetRequest{Key: "empty"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_FOUND {
		t.Fatalf("Get status = %v, want STATUS_FOUND", resp.GetStatus())
	}
	if len(resp.GetValue()) != 0 {
		t.Fatalf("Get value = %q, want empty", resp.GetValue())
	}
}

// TestGetBeforeExpiry covers Get while an entry with a TTL is still live and
// at the exact expiration boundary.
func TestGetBeforeExpiry(t *testing.T) {
	clock := newFakeClock()
	client := newTestClient(t, clock)
	ctx := t.Context()

	wantExpiry := clock.Now().Add(10 * time.Second)
	if _, err := client.Set(ctx, &cachev1.SetRequest{
		Key:   "session",
		Value: []byte("token"),
		Ttl:   durationpb.New(10 * time.Second),
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	clock.Advance(9 * time.Second)

	resp, err := client.Get(ctx, &cachev1.GetRequest{Key: "session"})
	if err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_FOUND {
		t.Fatalf("Get before expiry status = %v, want STATUS_FOUND", resp.GetStatus())
	}
	if !bytes.Equal(resp.GetValue(), []byte("token")) {
		t.Fatalf("Get before expiry value = %q, want %q", resp.GetValue(), "token")
	}
	if got := resp.GetExpiresAt().AsTime(); !got.Equal(wantExpiry) {
		t.Fatalf("Get before expiry expires_at = %v, want %v", got, wantExpiry)
	}

	clock.Advance(time.Second)

	resp, err = client.Get(ctx, &cachev1.GetRequest{Key: "session"})
	if err != nil {
		t.Fatalf("Get at expiry: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_EXPIRED {
		t.Fatalf("Get at expiry status = %v, want STATUS_EXPIRED", resp.GetStatus())
	}
}

// TestDeleteExpired covers Delete on an entry whose TTL elapsed: it reports
// false, removes the stale entry, and later Gets are plain misses.
func TestDeleteExpired(t *testing.T) {
	clock := newFakeClock()
	client := newTestClient(t, clock)
	ctx := t.Context()

	if _, err := client.Set(ctx, &cachev1.SetRequest{
		Key:   "session",
		Value: []byte("token"),
		Ttl:   durationpb.New(time.Second),
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	clock.Advance(2 * time.Second)

	del, err := client.Delete(ctx, &cachev1.DeleteRequest{Key: "session"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if del.GetDeleted() {
		t.Fatal("Delete(expired key) = true, want false")
	}

	resp, err := client.Get(ctx, &cachev1.GetRequest{Key: "session"})
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if resp.GetStatus() != cachev1.GetResponse_STATUS_NOT_FOUND {
		t.Fatalf("Get after Delete status = %v, want STATUS_NOT_FOUND", resp.GetStatus())
	}
}
