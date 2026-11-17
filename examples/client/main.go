// Command client is a small example that exercises the cache service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	cachev1 "github.com/HuiyuYang/grpc-mem-cache/proto/cache/v1"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC server address")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect to %s: %v", *addr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := cachev1.NewCacheServiceClient(conn)

	// Set a value that expires after one minute.
	if _, err := client.Set(ctx, &cachev1.SetRequest{
		Key:   "greeting",
		Value: []byte("hello"),
		Ttl:   durationpb.New(time.Minute),
	}); err != nil {
		log.Fatalf("set: %v", err)
	}

	// Read it back.
	resp, err := client.Get(ctx, &cachev1.GetRequest{Key: "greeting"})
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Printf("get: status=%s value=%q expires_at=%v\n",
		resp.GetStatus(), resp.GetValue(), resp.GetExpiresAt().AsTime())

	// Delete it.
	del, err := client.Delete(ctx, &cachev1.DeleteRequest{Key: "greeting"})
	if err != nil {
		log.Fatalf("delete: %v", err)
	}
	fmt.Printf("delete: deleted=%v\n", del.GetDeleted())

	// A second Get reports that the key is gone.
	resp, err = client.Get(ctx, &cachev1.GetRequest{Key: "greeting"})
	if err != nil {
		log.Fatalf("get after delete: %v", err)
	}
	fmt.Printf("get after delete: status=%s\n", resp.GetStatus())
}
