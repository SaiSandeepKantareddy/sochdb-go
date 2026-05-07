# SochDB Go gRPC V1 Plan

## Goal

Make the Go SDK a real remote/server SDK, not a partially complete wrapper
with placeholder gRPC methods.

## Previous gap

The original V1 problem was that `grpc_client.go` exposed a remote client
surface while several methods still returned placeholder errors.

That implementation gap is now closed for the current V1 slice:

- vector index create / insert / search
- collection create / add / search
- graph add / traverse / edge query helpers
- semantic cache get / put
- trace start / span start / span end
- KV get / put / delete

The remaining work is no longer generated-stub wiring. The next work is parity,
integration validation against a live server, and documentation cleanup.

## Priority implementation slice

We should not try to implement every service at once.
The best V1 slice is:

### Vector index service

- `CreateIndex`
- `InsertVectors`
- `GrpcSearch`

### Collection service

- `CreateCollection`
- `AddDocuments`
- `SearchCollection`

### Basic connectivity

- `HealthCheck`
- version / serving status exposure if available

This is enough to make Go useful for real remote vector workloads.

## Proto source of truth

Use:

- `sochdb/proto/sochdb.proto`

Relevant services already exist there:

- `VectorIndexService`
- `CollectionService`
- `GraphService`
- `SemanticCacheService`
- `TraceService`
- `KvService`

## Required tooling

To implement the Go gRPC client cleanly we need:

- `go`
- `protoc`
- `protoc-gen-go`
- `protoc-gen-go-grpc`

The current local machine has `protoc`, but not:

- `go`
- `protoc-gen-go`
- `protoc-gen-go-grpc`

So the next implementation pass should begin with explicit toolchain setup,
not hidden local environment mutation.

## Suggested generated package layout

Generate Go protobuf files into a stable package path under this repo, for
example:

- `internal/gen/sochdb/v1`

Then `grpc_client.go` can wrap those generated clients instead of trying to be
the transport implementation itself.

## API mapping notes

### `CreateIndex`

Map:

- `name`
- `dimension`
- `metric`
- default HNSW config when the caller does not provide one

### `InsertVectors`

Need to flatten `[][]float32` into the proto's flat `repeated float` payload.

### `GrpcSearch`

Map results into:

- `[]GrpcSearchResult`

### `CreateCollection`

Should map the current higher-level Go API to the proto collection creation
shape.

### `AddDocuments`

Requires careful mapping between Go document structs and the proto document
messages.

### `SearchCollection`

Should return typed Go documents, not raw proto structs.

## Enterprise expectations for Go

The Go SDK should feel like a real production client:

- `context.Context` for request control
- explicit timeout configuration
- idiomatic error handling
- minimal hidden magic

## Recommended next execution order

1. add live-server integration coverage for the remote client paths
2. tighten Python / Node / Go parity expectations per feature area
3. add or refresh one end-to-end remote example that exercises more than vector search
4. clean stale docs that still describe the old placeholder state

## Definition of done for Go V1

Go remote V1 is good enough when:

- gRPC client stubs are generated and checked in or reproducibly generated
- vector index create/insert/search works
- collection create/add/search works
- basic graph / semantic cache / trace / KV remote paths work
- one remote example runs end to end
- basic tests exist
