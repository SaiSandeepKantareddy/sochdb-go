# SochDB Go gRPC V1 Plan

## Goal

Make the Go SDK a real remote/server SDK, not a partially complete wrapper
with placeholder gRPC methods.

## Current gap

`grpc_client.go` already defines the user-facing client surface, but many
methods still return:

- `gRPC proto files not yet generated`

That means the main issue is not API design anymore.
The main issue is implementation and generated client wiring.

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
- other services later

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

## Recommended execution order

1. install Go + protobuf generators
2. generate Go stubs from `sochdb.proto`
3. wire `VectorIndexService`
4. wire `CollectionService`
5. add one remote example
6. add tests against a running gRPC server

## Definition of done for Go V1

Go remote V1 is good enough when:

- gRPC client stubs are generated and checked in or reproducibly generated
- vector index create/insert/search works
- collection create/add/search works
- one remote example runs end to end
- basic tests exist
