# grpc-mem-cache

用 Go、Protocol Buffers 和 gRPC 实现的单机内存缓存服务（第一版）。

## 功能

- `Set`：写入 key/value，支持可选 TTL；不设置或 TTL 为 0 时永不过期。
- `Get`：读取数据，通过响应状态区分「不存在」和「已过期」，并在命中带 TTL 的数据时返回到期时间。
- `Delete`：删除数据；删除不存在或已过期的 key 不报错。
- 并发安全：存储层使用 `sync.RWMutex`，gRPC 服务可同时处理多个请求。
- 服务入口支持通过 `-addr` 配置监听地址，并在收到 SIGINT/SIGTERM 时优雅退出。
- 本次不包含持久化、集群和认证。

## 项目结构

```text
.
├── cmd/server/           # 服务入口
├── examples/client/      # 调用示例
├── internal/cache/       # 并发安全的内存存储与惰性过期
├── internal/cachesvc/    # gRPC 服务实现
├── proto/cache/v1/       # proto 定义及生成代码
└── README.md
```

## 环境要求

- Go 1.26.1 或更高版本
- 仅在重新生成 proto 代码时需要 `protoc`、`protoc-gen-go` 和 `protoc-gen-go-grpc`
- 调用示例可选使用 [grpcurl](https://github.com/fullstorydev/grpcurl)

## 构建

```bash
go build ./...
```

或生成服务端二进制：

```bash
go build -o bin/cache-server ./cmd/server
```

## 启动

```bash
go run ./cmd/server
# 等价于
go run ./cmd/server -addr :50051
```

通过 `-addr` 指定监听地址：

```bash
go run ./cmd/server -addr 127.0.0.1:9090
```

服务启用了 gRPC reflection，因此可以直接用 grpcurl 查看服务定义并调用，无需传入 proto 文件。

## API

proto 定义见 [`proto/cache/v1/cache.proto`](proto/cache/v1/cache.proto)。

| RPC | 请求 | 响应 | 说明 |
| --- | --- | --- | --- |
| `Set` | `key`, `value`, 可选 `ttl` | 空 | 写入或覆盖 key；`ttl` 为 `google.protobuf.Duration` |
| `Get` | `key` | `status`, `value`, `expires_at` | 命中、缺失、过期分别返回 `STATUS_FOUND`、`STATUS_NOT_FOUND`、`STATUS_EXPIRED` |
| `Delete` | `key` | `deleted` | 仅当删除了未过期的数据时 `deleted` 为 `true` |

行为约定：

- `key` 不能为空，否则返回 gRPC `InvalidArgument`。
- `ttl` 未设置或为 0 表示永不过期；负数或非法 `Duration` 返回 `InvalidArgument`。
- 过期是惰性的：条目在 TTL 到期后不再被读取，并在下一次 `Get`/`Delete` 时清理。没有后台清理协程，因此无人访问的过期条目会一直占用内存，直到进程退出。
- `Get` 通过响应中的 `status` 字段表达业务状态，而不是返回 gRPC 错误，便于调用方区分「不存在」和「已过期」。
- 同一个 key 并发写入时最后一次生效；单次请求内 key/value 是原子读写。

## 生成代码

安装代码生成插件（版本与当前生成代码一致）：

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
```

在仓库根目录执行：

```bash
protoc -I proto \
  --go_out=proto --go_opt=paths=source_relative \
  --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
  proto/cache/v1/cache.proto
```

生成文件为 `proto/cache/v1/cache.pb.go` 和 `proto/cache/v1/cache_grpc.pb.go`。如果 `protoc` 提示找不到 `google/protobuf/duration.proto`，追加 protoc 自带的 include 目录，例如 macOS Homebrew：

```bash
protoc -I proto -I "$(brew --prefix protobuf)/include" \
  --go_out=proto --go_opt=paths=source_relative \
  --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
  proto/cache/v1/cache.proto
```

## 调用示例

先启动服务：

```bash
go run ./cmd/server -addr 127.0.0.1:50051
```

### grpcurl

```bash
# Set：value 为 bytes，JSON 中使用 base64；ttl 为 60 秒
grpcurl -plaintext -d '{"key":"foo","value":"YmFy","ttl":"60s"}' \
  127.0.0.1:50051 cache.v1.CacheService/Set

# Get
grpcurl -plaintext -d '{"key":"foo"}' \
  127.0.0.1:50051 cache.v1.CacheService/Get

# Delete
grpcurl -plaintext -d '{"key":"foo"}' \
  127.0.0.1:50051 cache.v1.CacheService/Delete
```

### Go 客户端

完整示例见 [`examples/client/main.go`](examples/client/main.go)，可直接运行：

```bash
go run ./examples/client -addr 127.0.0.1:50051
```

核心调用片段：

```go
conn, err := grpc.NewClient("127.0.0.1:50051",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

client := cachev1.NewCacheServiceClient(conn)
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

_, err = client.Set(ctx, &cachev1.SetRequest{
    Key:   "foo",
    Value: []byte("bar"),
    Ttl:   durationpb.New(time.Minute),
})
if err != nil {
    log.Fatal(err)
}

resp, err := client.Get(ctx, &cachev1.GetRequest{Key: "foo"})
if err != nil {
    log.Fatal(err)
}
if resp.GetStatus() == cachev1.GetResponse_STATUS_FOUND {
    fmt.Println(string(resp.GetValue()))
}
```

import 路径：

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/protobuf/types/known/durationpb"

    cachev1 "github.com/HuiyuYang/grpc-mem-cache/proto/cache/v1"
)
```

## 测试

```bash
go test -race ./...
go vet ./...
```

测试覆盖读写、覆盖写、删除、TTL 过期（含精确到期边界）、零/负 TTL、不存在与过期状态区分、value 拷贝隔离以及并发读写/删除/过期清理。
