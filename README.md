# SochDB Go SDK

**LLM-Optimized Embedded Database with Native Vector Search**

---

## Installation

### Step 1: Install the Native Library

```bash
# Automatic installation (installs to /usr/local/lib)
SOCHDB_ROOT=/path/to/sochdb ./install-lib.sh
```

**If automatic installation doesn't work**, see manual installation steps in [Troubleshooting](#library-not-found-error).

### Step 2: Install the Go SDK

```bash
go get github.com/sochdb/sochdb-go
```

### Step 3: Install pkg-config (if not already installed)

**macOS:**
```bash
brew install pkg-config
```

**Linux:**
```bash
# Ubuntu/Debian
sudo apt-get install pkg-config

# Fedora/RHEL
sudo yum install pkgconfig
```

### Verify Installation

```bash
# Check if library is found
pkg-config --libs libsochdb_storage

# Expected output: -L/usr/local/lib -lsochdb_storage
```

---

## Architecture: Flexible Deployment

**Dual-mode architecture: Embedded (FFI) + Concurrent + Server (gRPC/IPC)**  
Choose the deployment mode that fits your needs.

---

# SochDB Go SDK Documentation

> **Version 0.4.5** — LLM-Optimized Embedded Database with Native Vector Search

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Features](#features)
   - [Semantic Cache](#semantic-cache---llm-response-caching)
   - [Context Query Builder](#context-query-builder---token-aware-llm-context)
   - [Namespace API](#namespace-api---multi-tenant-isolation)
   - [Priority Queue API](#priority-queue-api---task-processing)
3. [Architecture](#architecture-flexible-deployment)
4. [System Requirements](#system-requirements)
5. [Troubleshooting](#troubleshooting)
6. [API Reference](#api-reference)
   - [Core Key-Value Operations](#core-key-value-operations)
   - [Transactions (ACID with SSI)](#transactions-acid-with-ssi)
   - [Prefix Scanning](#prefix-scanning)
   - [Namespaces & Collections](#namespaces--collections)
   - [Priority Queues](#priority-queues)
   - [Semantic Cache](#semantic-cache)
   - [Graph Operations](#graph-operations-ipc--grpc)
   - [Context Query Builder](#context-query-builder)
   - [Memory System (LLM-Native)](#memory-system-llm-native)
   - [Data Formats](#data-formats-toonjsoncolumnar)
   - [Server Mode (IPC / gRPC)](#server-mode-ipc--grpc)
   - [Checkpoints & Statistics](#checkpoints--statistics)
   - [Index Policies](#index-policies)
   - [Error Handling](#error-handling)
   - [Performance](#performance)

---

## 1. Quick Start

### Concurrent Embedded Mode

For web applications with multiple workers/processes:

```go
import "github.com/sochdb/sochdb-go/embedded"

// Open in concurrent mode - multiple processes can access simultaneously
db, err := embedded.OpenConcurrent("./web_db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Reads are lock-free and parallel (~100ns)
value, err := db.Get([]byte("user:123"))

// Writes are automatically coordinated
err = db.Put([]byte("user:123"), []byte(`{"name": "Alice"}`))

// Check if concurrent mode is active
fmt.Printf("Concurrent mode: %v\n", db.IsConcurrent())  // true
```

### Gin/Echo Example (Multiple Workers)

```go
package main

import (
    "github.com/gin-gonic/gin"
    "github.com/sochdb/sochdb-go/embedded"
    "log"
)

func main() {
    // Open database in concurrent mode
    db, err := embedded.OpenConcurrent("./gin_db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    r := gin.Default()
    
    r.GET("/user/:id", func(c *gin.Context) {
        // Multiple concurrent requests can read simultaneously
        key := []byte("user:" + c.Param("id"))
        data, err := db.Get(key)
        if err != nil || data == nil {
            c.JSON(404, gin.H{"error": "not found"})
            return
        }
        c.Data(200, "application/json", data)
    })
    
    r.POST("/user/:id", func(c *gin.Context) {
        // Writes are serialized automatically
        key := []byte("user:" + c.Param("id"))
        body, _ := c.GetRawData()
        if err := db.Put(key, body); err != nil {
            c.JSON(500, gin.H{"error": err.Error()})
            return
        }
        c.JSON(200, gin.H{"status": "ok"})
    })
    
    // Start with multiple workers (e.g., via systemd or Docker)
    // Each worker process can access the database concurrently
    r.Run(":8080")
}
```

### Performance

| Operation | Standard Mode | Concurrent Mode |
|-----------|---------------|-----------------|
| Read (single process) | ~100ns | ~100ns |
| Read (multi-process) | **Blocked** ❌ | ~100ns ✅ |
| Write | ~5ms (fsync) | ~60µs (amortized) |
| Max concurrent readers | 1 | 1024 |

### Deployment with Systemd (Multiple Workers)

```bash
# /etc/systemd/system/myapp@.service
[Unit]
Description=MyApp Worker %i
After=network.target

[Service]
Type=simple
User=appuser
WorkingDirectory=/opt/myapp
ExecStart=/opt/myapp/server
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
# Start 4 worker instances (all can access same DB concurrently)
sudo systemctl start myapp@1
sudo systemctl start myapp@2
sudo systemctl start myapp@3
sudo systemctl start myapp@4

# Enable on boot
sudo systemctl enable myapp@{1,2,3,4}
```

### Docker Compose Example

```yaml
version: '3.8'
services:
  app:
    build: .
    deploy:
      replicas: 4  # 4 workers share the same database
    volumes:
      - ./data:/app/data  # Shared database volume
    ports:
      - "8080-8083:8080"
```

---

## Engine Status

| Component | Status |
|-----------|--------|
| **Cost-based optimizer** | ✅ Production-ready — full cost model, cardinality estimation, plan caching |
| **Adaptive group commit** | ✅ Implemented — Little's Law-based batch sizing |
| **WAL compaction** | ⚠️ Partial — manual `Checkpoint()` + `TruncateWAL()` available |
| **HNSW vector index** | ✅ Production-ready — CGo FFI bindings |

---

## Features

### Semantic Cache - LLM Response Caching
Vector similarity-based caching for LLM responses to reduce costs and latency:

```go
import (
    sochdb "github.com/sochdb/sochdb-go"
    "github.com/sochdb/sochdb-go/embedded"
)

db, _ := embedded.Open("./mydb")
defer db.Close()

cache := sochdb.NewSemanticCache(db, "llm_responses")

// Store LLM response with embedding
cache.Put(
    "What is machine learning?",
    "Machine learning is a subset of AI...",
    []float32{0.1, 0.2, ...},  // 384-dim vector
    3600,  // TTL in seconds
    map[string]interface{}{"model": "gpt-4", "tokens": 150},
)

// Check cache before calling LLM
hit, _ := cache.Get(queryEmbedding, 0.85)
if hit != nil {
    fmt.Printf("Cache HIT! Similarity: %.4f\n", hit.Score)
    fmt.Printf("Response: %s\n", hit.Value)
}

// Get statistics
stats, _ := cache.Stats()
fmt.Printf("Hit rate: %.1f%%\n", stats.HitRate*100)
```

**Key Benefits:**
- ✅ Cosine similarity matching (0-1 threshold)
- ✅ TTL-based expiration
- ✅ Hit/miss statistics tracking
- ✅ Memory usage monitoring
- ✅ Automatic expired entry purging

### Context Query Builder - Token-Aware LLM Context
Assemble LLM context with priority-based truncation and token budgeting:

```go
import sochdb "github.com/sochdb/sochdb-go"

builder := sochdb.NewContextQueryBuilder().
    WithBudget(4096).  // Token limit
    SetFormat(sochdb.FormatTOON).
    SetTruncation(sochdb.TailDrop)

builder.
    Literal("SYSTEM", 0, "You are a helpful AI assistant.").
    Literal("USER_PROFILE", 1, "User: Alice, Role: Engineer").
    Literal("HISTORY", 2, "Recent conversation context...").
    Literal("KNOWLEDGE", 3, "Retrieved documents...")

result, _ := builder.Execute()
fmt.Printf("Tokens: %d/%d\n", result.TokenCount, 4096)
fmt.Printf("Context:\n%s\n", result.Text)
```

**Key Benefits:**
- ✅ Priority-based section ordering (lower = higher priority)
- ✅ Token budget enforcement
- ✅ Multiple truncation strategies (tail drop, head drop, proportional)
- ✅ Multiple output formats (TOON, JSON, Markdown)
- ✅ Token count estimation

### Namespace API - Multi-Tenant Isolation
First-class namespace handles for secure multi-tenancy and data isolation:

```go
import (
    sochdb "github.com/sochdb/sochdb-go"
    "github.com/sochdb/sochdb-go/embedded"
)

db, _ := embedded.Open("./mydb")
defer db.Close()

// Create isolated namespace for each tenant
namespace := &sochdb.Namespace{}

// Create vector collection
collection, _ := namespace.CreateCollection(sochdb.CollectionConfig{
    Name:      "documents",
    Dimension: 384,
    Metric:    sochdb.DistanceMetricCosine,
    Indexed:   true,
})

// Insert and search vectors
collection.Insert([]float32{1.0, 2.0, ...}, map[string]interface{}{"title": "Doc 1"}, "")
results, _ := collection.Search(sochdb.SearchRequest{
    QueryVector: []float32{...},
    K:          10,
})
```

**[→ See Full Example](./examples/namespace/main.go)**

### Priority Queue API - Task Processing
Efficient priority queue with ordered-key storage (no O(N) blob rewrites):

```go
import (
    sochdb "github.com/sochdb/sochdb-go"
    "github.com/sochdb/sochdb-go/embedded"
)

db, _ := embedded.Open("./queue_db")
defer db.Close()

queue := sochdb.NewPriorityQueue(db, "tasks", nil)

// Enqueue with priority (lower = higher urgency)
taskID, _ := queue.Enqueue(1, []byte("urgent task"), map[string]interface{}{"type": "payment"})

// Worker processes tasks
task, _ := queue.Dequeue("worker-1")
if task != nil {
    // Process task...
    queue.Ack(task.TaskID)
}

// Get statistics
stats, _ := queue.Stats()
fmt.Printf("Pending: %d, Completed: %d\n", stats.Pending, stats.Completed)
```

**[→ See Full Example](./examples/queue/main.go)**

**Key Benefits:**
- ✅ O(log N) enqueue/dequeue with ordered scans
- ✅ Atomic claim protocol for concurrent workers
- ✅ Visibility timeout for crash recovery
- ✅ Dead letter queue for failed tasks
- ✅ Multiple queues per database

---

## Architecture: Flexible Deployment

```
┌─────────────────────────────────────────────────────────────┐
│                    DEPLOYMENT OPTIONS                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. EMBEDDED MODE (FFI)          2. SERVER MODE (gRPC)      │
│  ┌─────────────────────┐         ┌─────────────────────┐   │
│  │   Go App        │         │   Go App        │   │
│  │   ├─ Database.open()│         │   ├─ SochDBClient() │   │
│  │   └─ Direct FFI     │         │   └─ gRPC calls     │   │
│  │         │           │         │         │           │   │
│  │         ▼           │         │         ▼           │   │
│  │   libsochdb_storage │         │   sochdb-grpc       │   │
│  │   (Rust native)     │         │   (Rust server)     │   │
│  └─────────────────────┘         └─────────────────────┘   │
│                                                               │
│  ✅ No server needed               ✅ Multi-language          │
│  ✅ Local files                    ✅ Centralized logic      │
│  ✅ Simple deployment              ✅ Production scale       │
└─────────────────────────────────────────────────────────────┘
```

### When to Use Each Mode

**Embedded Mode (FFI):**
- ✅ Local development and testing
- ✅ Jupyter notebooks and data science
- ✅ Single-process applications
- ✅ Edge deployments without network
- ✅ No server setup required

**Server Mode (gRPC):**
- ✅ Production deployments
- ✅ Multi-language teams (Python, Node.js, Go)
- ✅ Distributed systems
- ✅ Centralized business logic
- ✅ Horizontal scaling

---

---

## System Requirements

### For Concurrent Mode

- **SochDB Core**: Latest version
- **Go Version**: 1.18+
- **CGO**: Required (uses C bindings)
- **Native Library**: `libsochdb_storage.{dylib,so}`

**Operating Systems:**
- ✅ Linux (Ubuntu 20.04+, RHEL 8+)
- ✅ macOS (10.15+, both Intel and Apple Silicon)
- ⚠️  Windows (requires WSL2 or native builds with MinGW)

**File Descriptors:**
- Default limit: 1024 (sufficient for most workloads)
- For high concurrency: Increase with `ulimit -n 4096`

**Memory:**
- Standard mode: ~50MB base + data
- Concurrent mode: +4KB per concurrent reader slot (1024 slots = ~4MB overhead)

---

## Troubleshooting

### "Database is locked" Error (Standard Mode)

```
Error: database is locked by another process
```

**Solution**: Use concurrent mode for multi-process access:

```go
// ❌ Standard mode - only one process allowed
db, _ := embedded.Open("./data.db")

// ✅ Concurrent mode - unlimited processes
db, _ := embedded.OpenConcurrent("./data.db")
```

### Library Not Found Error

```
ld: library not found for -lsochdb_storage
```

**Solution 1** - Manual installation:
```bash
# Build the native library
cd /path/to/sochdb
cargo build --release

# Copy to system location (macOS/Linux)
sudo cp target/release/libsochdb_storage.{dylib,so} /usr/local/lib/
sudo ldconfig  # Linux only

# Create pkg-config file
cat > /tmp/libsochdb_storage.pc <<EOF
prefix=/usr/local
exec_prefix=\${prefix}
libdir=\${exec_prefix}/lib
includedir=\${prefix}/include

Name: libsochdb_storage
Description: SochDB Native Storage Library
Libs: -L\${libdir} -lsochdb_storage
Cflags: -I\${includedir}
EOF

sudo mv /tmp/libsochdb_storage.pc /usr/local/lib/pkgconfig/
```

**Solution 2** - Development mode (no installation):
```bash
export DYLD_LIBRARY_PATH=/path/to/sochdb/target/release  # macOS
export LD_LIBRARY_PATH=/path/to/sochdb/target/release    # Linux
export CGO_LDFLAGS="-L/path/to/sochdb/target/release -lsochdb_storage"
```

### CGO Not Found

```
go: C compiler "gcc" not found
```

**macOS**:
```bash
xcode-select --install
```

**Linux**:
```bash
# Ubuntu/Debian
sudo apt-get install build-essential

# RHEL/Fedora
sudo yum groupinstall "Development Tools"
```

### Performance Issues

**Symptom**: Concurrent reads slower than expected

**Check 1** - Verify concurrent mode is active:
```go
if !db.IsConcurrent() {
    log.Fatal("Database opened in standard mode!")
}
```

**Check 2** - Monitor reader slot usage:
```bash
# Enable debug logging in SochDB core
export RUST_LOG=sochdb_storage=debug
```

**Check 3** - Tune write batching:
```go
// Batch writes for better throughput
tx, _ := db.BeginTxn()
for i := 0; i < 1000; i++ {
    tx.Put([]byte(fmt.Sprintf("key%d", i)), value)
}
tx.Commit()  // Single fsync for entire batch
```

---

---

## API Reference

> **Version 0.4.5** — Complete API documentation with Go examples.

All core logic runs in the Rust engine via CGo FFI. The SDK is a thin client.

---

### Core Key-Value Operations

```go
package main

import (
    "fmt"
    "log"
    "github.com/sochdb/sochdb-go/embedded"
)

func main() {
    db, err := embedded.Open("./mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Put / Get / Delete
    db.Put([]byte("user:1"), []byte(`{"name":"Alice"}`))
    value, _ := db.Get([]byte("user:1"))
    fmt.Println(string(value)) // {"name":"Alice"}
    db.Delete([]byte("user:1"))

    // Path-based keys (hierarchical)
    db.PutPath("/users/alice/profile", []byte(`{"age":30}`))
    profile, _ := db.GetPath("/users/alice/profile")
    fmt.Println(string(profile))
}
```

---

### Transactions (ACID with SSI)

SochDB uses Serializable Snapshot Isolation for full ACID transactions:

```go
db, _ := embedded.Open("./mydb")
defer db.Close()

// Auto-managed transaction
err := db.WithTransaction(func(txn *embedded.Transaction) error {
    txn.Put([]byte("key1"), []byte("val1"))
    txn.Put([]byte("key2"), []byte("val2"))
    val, _ := txn.Get([]byte("key1"))
    fmt.Println(string(val))
    return nil // auto-commits; return error to abort
})

// Manual transaction control
txn := db.Begin()
txn.Put([]byte("balance:alice"), []byte("100"))
txn.Put([]byte("balance:bob"), []byte("200"))
if err := txn.Commit(); err != nil {
    // SSI conflict — retry
    fmt.Println("Conflict:", err)
}
```

---

### Prefix Scanning

```go
db, _ := embedded.Open("./mydb")
defer db.Close()

// Insert test data
db.Put([]byte("user:1"), []byte("Alice"))
db.Put([]byte("user:2"), []byte("Bob"))
db.Put([]byte("user:3"), []byte("Charlie"))

// Scan all keys with prefix (auto-transaction)
iter := db.ScanPrefix([]byte("user:"))
defer iter.Close()

for {
    key, value, ok := iter.Next()
    if !ok {
        break
    }
    fmt.Printf("%s = %s\n", key, value)
}

// Transaction-scoped scan
txn := db.Begin()
defer txn.Abort()
it := txn.ScanPrefix([]byte("order:"))
defer it.Close()
for {
    k, v, ok := it.Next()
    if !ok { break }
    fmt.Printf("%s = %s\n", k, v)
}
txn.Commit()
```

---

### Namespaces & Collections

Multi-tenant isolation with vector-enabled collections:

```go
import (
    sochdb "github.com/sochdb/sochdb-go"
    "github.com/sochdb/sochdb-go/embedded"
)

db, _ := embedded.Open("./mydb")
defer db.Close()

// Create a namespace
ns := sochdb.NewNamespace(db, "tenant_1", sochdb.NamespaceConfig{
    Name:        "tenant_1",
    DisplayName: "Tenant One",
    Labels:      map[string]string{"tier": "premium"},
})

// Create a collection with vector search
docs, _ := ns.CreateCollection(sochdb.CollectionConfig{
    Name:      "documents",
    Dimension: 384,
    Metric:    sochdb.DistanceMetricCosine,
    Indexed:   true,
})

// Insert vectors with metadata
id, _ := docs.Insert(
    []float32{0.1, 0.2, 0.3 /* ...384 dims */},
    map[string]interface{}{"title": "Introduction to AI", "author": "Alice"},
    "", // auto-generate ID
)

// Batch insert
ids, _ := docs.InsertMany(
    [][]float32{{0.1, 0.2}, {0.3, 0.4}},
    []map[string]interface{}{{"title": "Doc 1"}, {"title": "Doc 2"}},
    nil, // auto-generate IDs
)

// Search with filter
results, _ := docs.Search(sochdb.SearchRequest{
    QueryVector:     []float32{0.1, 0.2, 0.3 /* ... */},
    K:               5,
    Filter:          map[string]interface{}{"author": "Alice"},
    IncludeMetadata: true,
})
for _, r := range results {
    fmt.Printf("%s: score=%.4f\n", r.ID, r.Score)
}

// Collection management
count, _ := docs.Count()
collections, _ := ns.ListCollections()
ns.DeleteCollection("documents")
```

---

### Priority Queues

Ordered task queues with priority-based dequeue, ack/nack, and dead-letter support:

```go
import sochdb "github.com/sochdb/sochdb-go"

db, _ := embedded.Open("./mydb")
defer db.Close()

// Create a queue
queue := sochdb.CreateQueue(db, "background-jobs", &sochdb.QueueConfig{
    VisibilityTimeout: 30,
    MaxRetries:        3,
    DeadLetterQueue:   "failed-jobs",
})

// Enqueue tasks (lower priority = higher urgency)
taskID, _ := queue.Enqueue(
    1,
    []byte(`{"action":"send_email","to":"alice@example.com"}`),
    map[string]interface{}{"source": "api"},
)

queue.Enqueue(10, []byte("low priority"), nil)
queue.Enqueue(1, []byte("high priority"), nil)

// Dequeue highest priority task
task, _ := queue.Dequeue("worker-1")
if task != nil {
    fmt.Printf("Processing: %s (priority: %d)\n", task.TaskID, task.Priority)
    fmt.Printf("Payload: %s\n", task.Payload)

    // Process task...
    if err := processTask(task); err != nil {
        queue.Nack(task.TaskID) // Re-queue for retry
    } else {
        queue.Ack(task.TaskID) // Mark completed
    }
}

// Queue statistics
stats, _ := queue.Stats()
fmt.Printf("Pending: %d, Claimed: %d, Completed: %d\n",
    stats.Pending, stats.Claimed, stats.Completed)

// Purge completed/dead-lettered tasks
purged, _ := queue.Purge()
fmt.Printf("Purged %d tasks\n", purged)
```

---

### Semantic Cache

Cache LLM responses with vector-similarity retrieval:

```go
import sochdb "github.com/sochdb/sochdb-go"

db, _ := embedded.Open("./cache_db")
defer db.Close()

cache := sochdb.NewSemanticCache(db, "llm_responses")

// Store response with embedding
cache.Put(
    "What is machine learning?",
    "Machine learning is a subset of AI...",
    []float32{0.1, 0.2, 0.3 /* ... */}, // embedding
    3600,  // TTL seconds
    map[string]interface{}{"model": "gpt-4", "tokens": 42},
)

// Check cache before calling LLM
hit, _ := cache.Get(queryEmbedding, 0.85)
if hit != nil {
    fmt.Printf("Cache HIT (score: %.4f)\n", hit.Score)
    fmt.Printf("Response: %s\n", hit.Value)
}

// Cache management
stats, _ := cache.Stats()
fmt.Printf("Hit rate: %.1f%%\n", stats.HitRate*100)
cache.PurgeExpired()
cache.Clear()
```

---

### Graph Operations (IPC / gRPC)

Graph overlay via IPC or gRPC client:

```go
// IPC Client
client, _ := sochdb.Connect("/tmp/sochdb.sock")
defer client.Close()

// Add nodes
client.AddNode("social", "alice", "person", map[string]string{"name": "Alice"})
client.AddNode("social", "bob", "person", map[string]string{"name": "Bob"})

// Add edges
client.AddEdge("social", "alice", "works_at", "acme", nil)
client.AddEdge("social", "alice", "knows", "bob", nil)

// Graph traversal (BFS/DFS)
result, _ := client.Traverse("social", "alice", 2, "bfs")
fmt.Printf("Nodes: %d, Edges: %d\n", len(result.Nodes), len(result.Edges))

// gRPC Client
grpc, _ := sochdb.GrpcConnect("localhost:50051")
defer grpc.Close()

grpc.AddGraphNode("alice", "person", map[string]string{"name": "Alice"}, "social")
grpc.AddGraphEdge("alice", "knows", "bob", nil, "social")
nodes, edges, _ := grpc.TraverseGraph("alice", 2, "bfs", "social")
```

---

### Context Query Builder

Token-budget-aware context assembly for LLM prompts:

```go
builder := sochdb.NewContextQueryBuilder()

result, _ := builder.
    ForSession("session_123").
    WithBudget(4096).
    SetFormat(sochdb.FormatMarkdown).
    SetTruncation(sochdb.Proportional).
    Literal("system", 100, "You are a helpful assistant.").
    Literal("user_query", 90, "Tell me about SochDB").
    Execute()

fmt.Printf("Tokens used: %d\n", result.TokenCount)
fmt.Println(result.Text)
```

---

### Memory System (LLM-Native)

Complete memory for AI agents — extraction, consolidation, hybrid retrieval:

```go
import sochdb "github.com/sochdb/sochdb-go"

db, _ := embedded.Open("./memory_db")
defer db.Close()

// 1. Extract entities and relations
pipeline := sochdb.NewExtractionPipeline(db, "user_123", &sochdb.ExtractionSchema{
    EntityTypes:   []string{"person", "organization"},
    MinConfidence: 0.7,
})

extracted, _ := pipeline.ExtractAndCommit(
    "Alice joined Acme Corp",
    func(text string) (map[string]interface{}, error) {
        return map[string]interface{}{
            "entities": []map[string]interface{}{
                {"name": "Alice", "entity_type": "person", "confidence": 0.95},
                {"name": "Acme Corp", "entity_type": "organization", "confidence": 0.9},
            },
        }, nil
    },
)
fmt.Printf("Extracted %d entities\n", len(extracted.Entities))

// 2. Consolidate facts
consolidator := sochdb.NewConsolidator(db, "user_123", nil)
consolidator.Add(&sochdb.RawAssertion{
    Fact:       map[string]interface{}{"subject": "Alice", "predicate": "lives_in", "object": "SF"},
    Source:     "conversation_1",
    Confidence: 0.9,
})
consolidator.Consolidate()
facts, _ := consolidator.GetCanonicalFacts()

// 3. Hybrid retrieval (vector + BM25 with RRF fusion)
retriever := sochdb.NewHybridRetriever(db, "user_123", nil)
retriever.IndexDocuments(map[string]map[string]interface{}{
    "doc1": {"content": "Alice is a software engineer", "embedding": []float32{0.1, 0.2}},
})
results, _ := retriever.Retrieve("Who is Alice?", sochdb.NewAllAllowedSet())
```

---

### Data Formats (TOON/JSON/Columnar)

```go
// Wire format types
wire, _ := sochdb.ParseWireFormat("toon")
ctx, _ := sochdb.ParseContextFormat("json")

caps := sochdb.NewFormatCapabilities()
roundTrip := caps.SupportsRoundTrip(wire)
fmt.Printf("TOON round-trip: %v\n", roundTrip) // true
```

---

### Server Mode (IPC / gRPC)

```go
// IPC Client (Unix socket)
client, _ := sochdb.Connect("/tmp/sochdb.sock")
defer client.Close()

client.Put([]byte("key"), []byte("value"))
value, _ := client.Get([]byte("key"))
client.Delete([]byte("key"))

// Scan with prefix
kvs, _ := client.Scan("user:")
for _, kv := range kvs {
    fmt.Printf("%s = %s\n", kv.Key, kv.Value)
}

// Transactions
txnID, _ := client.BeginTransaction()
// ... operations ...
client.CommitTransaction(txnID)

// Statistics
stats, _ := client.Stats()
fmt.Printf("Memtable: %d bytes, WAL: %d bytes\n",
    stats.MemtableSizeBytes, stats.WALSizeBytes)

// gRPC Client
grpc, _ := sochdb.GrpcConnect("localhost:50051")
defer grpc.Close()

grpc.CreateIndex("embeddings", 384, "cosine")
grpc.InsertVectors("embeddings", []uint64{1, 2}, vectors)
results, _ := grpc.GrpcSearch("embeddings", queryVector, 5)
```

---

### Checkpoints & Statistics

```go
db, _ := embedded.Open("./mydb")
defer db.Close()

// Force checkpoint (flush memtable → disk)
lsn, _ := db.Checkpoint()
fmt.Printf("Checkpoint LSN: %d\n", lsn)

// Database statistics
stats, _ := db.Stats()
fmt.Printf("Memtable: %d bytes\n", stats.MemtableSizeBytes)
fmt.Printf("WAL: %d bytes\n", stats.WalSizeBytes)
fmt.Printf("Active txns: %d\n", stats.ActiveTransactions)
```

---

### Index Policies

```go
db, _ := embedded.Open("./mydb")
defer db.Close()

// Set index policy for a table
db.SetTableIndexPolicy("events", embedded.IndexAppendOnly)

// Get current policy
policy, _ := db.GetTableIndexPolicy("events")
// IndexWriteOptimized(0), IndexBalanced(1), IndexScanOptimized(2), IndexAppendOnly(3)
```

---

### Error Handling

```go
import sochdb "github.com/sochdb/sochdb-go"

db, err := embedded.Open("./mydb")
if err != nil {
    var lockErr *sochdb.DatabaseLockedError
    if errors.As(err, &lockErr) {
        fmt.Printf("Database locked by PID %d\n", lockErr.HolderPID)
    }
    log.Fatal(err)
}

err = db.WithTransaction(func(txn *embedded.Transaction) error {
    return txn.Put([]byte("key"), []byte("value"))
})
if err != nil {
    // SSI conflict error: retry
    fmt.Println("Transaction error:", err)
}
```

**Error types:**
- `ConnectionError` — socket/network failures
- `ProtocolError` — wire protocol issues
- `TransactionError` — SSI conflicts
- `DatabaseLockedError` — database locked by another process
- `LockTimeoutError` — lock acquisition timeout
- `EpochMismatchError` — stale epoch
- `SplitBrainError` — concurrent mode conflict
- `FormatConversionError` — format conversion failure

---

### Performance

| Operation | Latency |
|-----------|---------|
| KV Read | ~100ns |
| KV Write (fsync) | ~5ms |
| KV Write (concurrent, amortized) | ~60µs |
| Vector Search (HNSW, 1M vectors) | <5ms |
| Prefix Scan (per item) | ~200ns |
| Max Concurrent Readers | 1024 |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, building from source, and pull request guidelines.

---

## License

Apache License 2.0
