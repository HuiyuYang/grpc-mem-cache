// Command server runs the single-node gRPC in-memory cache service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/HuiyuYang/grpc-mem-cache/internal/cache"
	"github.com/HuiyuYang/grpc-mem-cache/internal/cachesvc"
	cachev1 "github.com/HuiyuYang/grpc-mem-cache/proto/cache/v1"
)

func main() {
	addr := flag.String("addr", ":50051", "TCP address for the gRPC server to listen on")
	flag.Parse()

	if err := run(*addr); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", addr, err)
	}

	server := grpc.NewServer()
	cachev1.RegisterCacheServiceServer(server, cachesvc.New(cache.NewStore()))
	// Reflection lets tools such as grpcurl discover the service without the
	// proto files.
	reflection.Register(server)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		slog.Info("shutting down")
		server.GracefulStop()
	}()

	slog.Info("gRPC cache server listening", "addr", lis.Addr().String())
	err = server.Serve(lis)

	// If Serve returns without a signal, release the shutdown goroutine.
	stop()
	<-stopped

	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
