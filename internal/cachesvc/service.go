// Package cachesvc implements the cache.v1 gRPC service on top of an
// in-memory cache.Store.
package cachesvc

import (
	"context"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/HuiyuYang/grpc-mem-cache/internal/cache"
	cachev1 "github.com/HuiyuYang/grpc-mem-cache/proto/cache/v1"
)

// Service implements cachev1.CacheServiceServer.
type Service struct {
	cachev1.UnimplementedCacheServiceServer
	store *cache.Store
}

var _ cachev1.CacheServiceServer = (*Service)(nil)

// New returns a Service backed by store.
func New(store *cache.Store) *Service {
	return &Service{store: store}
}

// Set stores a value with an optional TTL.
func (s *Service) Set(ctx context.Context, req *cachev1.SetRequest) (*cachev1.SetResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}
	ttl, err := ttlFromProto(req.GetTtl())
	if err != nil {
		return nil, err
	}

	s.store.Set(req.GetKey(), req.GetValue(), ttl)
	return &cachev1.SetResponse{}, nil
}

// Get returns a value and reports whether the key was found, missing, or
// expired.
func (s *Service) Get(ctx context.Context, req *cachev1.GetRequest) (*cachev1.GetResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}

	result := s.store.Get(req.GetKey())
	switch result.State {
	case cache.StateFound:
		resp := &cachev1.GetResponse{
			Status: cachev1.GetResponse_STATUS_FOUND,
			Value:  result.Value,
		}
		if !result.ExpiresAt.IsZero() {
			resp.ExpiresAt = timestamppb.New(result.ExpiresAt)
		}
		return resp, nil
	case cache.StateExpired:
		return &cachev1.GetResponse{Status: cachev1.GetResponse_STATUS_EXPIRED}, nil
	default:
		return &cachev1.GetResponse{Status: cachev1.GetResponse_STATUS_NOT_FOUND}, nil
	}
}

// Delete removes a key.
func (s *Service) Delete(ctx context.Context, req *cachev1.DeleteRequest) (*cachev1.DeleteResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}

	return &cachev1.DeleteResponse{Deleted: s.store.Delete(req.GetKey())}, nil
}

// ListKeys returns the keys of all live entries.
func (s *Service) ListKeys(ctx context.Context, req *cachev1.ListKeysRequest) (*cachev1.ListKeysResponse, error) {
	return &cachev1.ListKeysResponse{Keys: s.store.Keys()}, nil
}

func validateKey(key string) error {
	if key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	return nil
}

// maxTTLSeconds is the largest whole-second TTL that time.Duration can hold.
// TTLs with at least this many seconds are rejected so that converting the
// protobuf value to time.Duration cannot overflow.
const maxTTLSeconds = int64(math.MaxInt64) / int64(time.Second)

func ttlFromProto(ttl *durationpb.Duration) (time.Duration, error) {
	if ttl == nil {
		return 0, nil
	}
	if err := ttl.CheckValid(); err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "invalid ttl: %v", err)
	}
	// Check the sign on the protobuf fields rather than on AsDuration, whose
	// conversion overflows for out-of-range values.
	if ttl.GetSeconds() < 0 || ttl.GetNanos() < 0 {
		return 0, status.Error(codes.InvalidArgument, "ttl must not be negative")
	}
	if ttl.GetSeconds() >= maxTTLSeconds {
		return 0, status.Error(codes.InvalidArgument, "ttl is too large")
	}
	return ttl.AsDuration(), nil
}
