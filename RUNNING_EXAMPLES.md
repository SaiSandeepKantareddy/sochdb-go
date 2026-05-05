# Running SochDB Go Examples

## Prerequisites

- Go 1.24.0 or higher
- SochDB native library built (either debug or release)

## Setting Up Library Path

The Go SDK requires the SochDB native library (`libsochdb_storage.dylib` on macOS, `.so` on Linux).

### Option 1: Using Debug Build

```bash
export DYLD_LIBRARY_PATH=/Users/sushanth/sochdb/sochdb/target/debug
export CGO_LDFLAGS="-L/Users/sushanth/sochdb/sochdb/target/debug -lsochdb_storage -Wl,-rpath,/Users/sushanth/sochdb/sochdb/target/debug"
```

### Option 2: Build Release Version

```bash
cd /Users/sushanth/sochdb/sochdb
cargo build --release
export DYLD_LIBRARY_PATH=/Users/sushanth/sochdb/sochdb/target/release
```

## Running Examples

### 1. Embedded Mode Example

The embedded example demonstrates direct FFI usage without requiring a server:

```bash
cd examples/embedded
DYLD_LIBRARY_PATH=/Users/sushanth/sochdb/sochdb/target/debug \
CGO_LDFLAGS="-L/Users/sushanth/sochdb/sochdb/target/debug -lsochdb_storage -Wl,-rpath,/Users/sushanth/sochdb/sochdb/target/debug" \
go run main.go
```

**Expected Output:**
```
=== SochDB Go SDK - Embedded Mode (FFI) ===

Opening database...

1. Basic KV Operations:
  user:1 = Alice

2. Path Operations:
  alice email = alice@example.com

3. Transactions:
  Transaction committed

4. Scan Operations:
  scan:1 = val1
  scan:2 = val2
  scan:3 = val3
  Found 3 keys

5. Database Stats:
  Active transactions: 1
  Memtable size: 123 bytes

6. Checkpoint:
  Checkpoint LSN: 0

✅ All operations completed successfully!
```

### 2. Server Mode Example

The main example demonstrates gRPC and IPC modes (requires server):

```bash
cd example
DYLD_LIBRARY_PATH=/Users/sushanth/sochdb/sochdb/target/debug \
CGO_LDFLAGS="-L/Users/sushanth/sochdb/sochdb/target/debug -lsochdb_storage -Wl,-rpath,/Users/sushanth/sochdb/sochdb/target/debug" \
go run main.go
```

**Note:** This example requires:
- gRPC server running (for gRPC mode)
- IPC server running at `/tmp/sochdb.sock` (for IPC mode)

The format utilities will work without a server.

### 3. Hosted Remote Smoke Example

This example runs a minimal hosted gRPC path against the shared demo endpoint:

```bash
SOCHDB_GRPC_ADDRESS=studio.agentslab.host:50053 \
go run ./examples/hosted_remote
```

**Expected behavior:**
- creates a fresh collection name per run
- inserts demo documents
- runs a search and prints a short result summary

A matching manual GitHub Actions workflow is available at
`.github/workflows/hosted-smoke.yml` for on-demand hosted validation.

Latest hosted validation:

- GitHub-hosted workflow passed on May 5, 2026:
  `https://github.com/SaiSandeepKantareddy/sochdb-go/actions/runs/25357488825`

## Running Tests

### Embedded Tests

```bash
cd embedded
DYLD_LIBRARY_PATH=/Users/sushanth/sochdb/sochdb/target/debug \
CGO_LDFLAGS="-L/Users/sushanth/sochdb/sochdb/target/debug -lsochdb_storage -Wl,-rpath,/Users/sushanth/sochdb/sochdb/target/debug" \
go test -v
```

**Test Results:**
- ✅ TestEmbeddedDatabase - All subtests pass
- ✅ TestConcurrentTransactions - Concurrent access works
- ✅ TestBasicKV - Basic operations verified
- ✅ TestPathOperations - Path-based keys work
- ✅ TestTransactions - ACID with SSI confirmed
- ✅ TestScanOperations - Prefix scanning works
- ✅ TestSSIConflictHandling - Conflict resolution works
- ✅ TestStatistics - Stats retrieval works
- ✅ TestCheckpoint - Checkpointing works

All tests pass in ~0.45 seconds.

## Status Summary

| Example | Status | Notes |
|---------|--------|-------|
| `examples/embedded/main.go` | ✅ Working | Embedded FFI mode, no server needed |
| `examples/hosted_remote/main.go` | ✅ Working | Hosted gRPC smoke path |
| `example/main.go` (gRPC) | ⚠️ Requires Server | Need gRPC server running |
| `example/main.go` (IPC) | ⚠️ Requires Server | Need IPC server at `/tmp/sochdb.sock` |
| `example/main.go` (Format utils) | ✅ Working | Format utilities work standalone |
| `embedded/database_test.go` | ✅ All Pass | 9 tests pass |
| `embedded/validation_test.go` | ✅ All Pass | Validation tests pass |

## Common Issues

### Issue: "library 'sochdb_storage' not found"

**Solution:** Set the library path environment variables as shown above.

### Issue: "search path not found"

This is just a warning - the code will still work if CGO_LDFLAGS is set correctly.

### Issue: gRPC operation returns server-side unimplemented or runtime errors

The Go SDK now includes generated gRPC stubs and a working remote client path.
If a remote operation still fails, the most likely causes are:

- the target SochDB server does not expose that RPC yet
- the server is unreachable or misconfigured
- the example is pointed at the wrong address or namespace

## Recommended Workflow

For local development, **embedded mode** is still the fastest path when you do
not need a running server:

```bash
# One-time setup
export DYLD_LIBRARY_PATH=/Users/sushanth/sochdb/sochdb/target/debug
export CGO_LDFLAGS="-L/Users/sushanth/sochdb/sochdb/target/debug -lsochdb_storage -Wl,-rpath,/Users/sushanth/sochdb/sochdb/target/debug"

# Run examples
cd examples/embedded && go run main.go

# Run tests
cd embedded && go test -v
```
