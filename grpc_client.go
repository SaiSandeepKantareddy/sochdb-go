// Package sochdb provides a thin gRPC client wrapper for the SochDB server.
// All business logic runs on the server (Thick Server / Thin Client architecture).
//
// The client is approximately ~300 lines of code, delegating all operations to the server.
package sochdb

import (
	"context"
	"fmt"
	"time"

	sochdbv1 "github.com/sochdb/sochdb-go/internal/gen/sochdbv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GrpcClient provides thin gRPC client for SochDB.
// All operations are delegated to the SochDB gRPC server.
type GrpcClient struct {
	conn                *grpc.ClientConn
	address             string
	timeout             time.Duration
	vectorIndexClient   sochdbv1.VectorIndexServiceClient
	collectionClient    sochdbv1.CollectionServiceClient
	graphClient         sochdbv1.GraphServiceClient
	semanticCacheClient sochdbv1.SemanticCacheServiceClient
	traceClient         sochdbv1.TraceServiceClient
	kvClient            sochdbv1.KvServiceClient
}

// GrpcClientOptions configures the gRPC client.
type GrpcClientOptions struct {
	Address string
	Timeout time.Duration
	Secure  bool
}

// GrpcSearchResult represents a vector search result.
type GrpcSearchResult struct {
	ID       uint64
	Distance float32
}

// GrpcDocument represents a document with embedding.
type GrpcDocument struct {
	ID        string
	Content   string
	Embedding []float32
	Metadata  map[string]string
}

// GrpcGraphNode represents a node in the graph.
type GrpcGraphNode struct {
	ID         string
	NodeType   string
	Properties map[string]string
}

// GrpcGraphEdge represents an edge in the graph.
type GrpcGraphEdge struct {
	FromID     string
	EdgeType   string
	ToID       string
	Properties map[string]string
}

// NewGrpcClient creates a new gRPC client connected to SochDB server.
func NewGrpcClient(opts GrpcClientOptions) (*GrpcClient, error) {
	if opts.Address == "" {
		opts.Address = "localhost:50051"
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}

	// Create connection options
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100 * 1024 * 1024)),
	}

	if opts.Secure {
		return nil, fmt.Errorf("secure connections not yet implemented")
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(opts.Address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", opts.Address, err)
	}

	return &GrpcClient{
		conn:                conn,
		address:             opts.Address,
		timeout:             opts.Timeout,
		vectorIndexClient:   sochdbv1.NewVectorIndexServiceClient(conn),
		collectionClient:    sochdbv1.NewCollectionServiceClient(conn),
		graphClient:         sochdbv1.NewGraphServiceClient(conn),
		semanticCacheClient: sochdbv1.NewSemanticCacheServiceClient(conn),
		traceClient:         sochdbv1.NewTraceServiceClient(conn),
		kvClient:            sochdbv1.NewKvServiceClient(conn),
	}, nil
}

// GrpcConnect is a convenience function to connect to a SochDB gRPC server.
func GrpcConnect(address string) (*GrpcClient, error) {
	return NewGrpcClient(GrpcClientOptions{Address: address})
}

// Close closes the gRPC connection.
func (c *GrpcClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ctx returns a context with the configured timeout.
func (c *GrpcClient) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), c.timeout)
}

// ===========================================================================
// Vector Index Operations
// ===========================================================================

// CreateIndex creates a new vector index.
func (c *GrpcClient) CreateIndex(name string, dimension int, metric string) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.vectorIndexClient.CreateIndex(ctx, &sochdbv1.CreateIndexRequest{
		Name:      name,
		Dimension: uint32(dimension),
		Metric:    parseDistanceMetric(metric),
		// Match the engine's HnswConfig::default() (m=32, m0=64, efC=256, F32)
		// so an untuned index reaches 95+ recall@10 (Cohere-1M 768d cosine =
		// 0.972). The old m=16/m0=32/efC=100 built a cheap graph that capped
		// recall near 0.90 on hard data.
		Config: &sochdbv1.HnswConfig{
			MaxConnections:       32,
			MaxConnectionsLayer0: 64,
			EfConstruction:       256,
			EfSearch:             64,
		},
	})
	if err != nil {
		return fmt.Errorf("create index %q: %w", name, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("create index %q failed: %s", name, resp.GetError())
	}
	return nil
}

// InsertVectors inserts vectors into an index.
func (c *GrpcClient) InsertVectors(indexName string, ids []uint64, vectors [][]float32) (int, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	flat, err := flattenVectors(vectors)
	if err != nil {
		return 0, err
	}
	resp, err := c.vectorIndexClient.InsertBatch(ctx, &sochdbv1.InsertBatchRequest{
		IndexName: indexName,
		Ids:       ids,
		Vectors:   flat,
	})
	if err != nil {
		return 0, fmt.Errorf("insert vectors into %q: %w", indexName, err)
	}
	if resp.GetError() != "" {
		return int(resp.GetInsertedCount()), fmt.Errorf("insert vectors into %q failed: %s", indexName, resp.GetError())
	}
	return int(resp.GetInsertedCount()), nil
}

// GrpcSearch performs k-nearest neighbor search.
func (c *GrpcClient) GrpcSearch(indexName string, query []float32, k int) ([]GrpcSearchResult, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.vectorIndexClient.Search(ctx, &sochdbv1.SearchRequest{
		IndexName: indexName,
		Query:     query,
		K:         uint32(k),
	})
	if err != nil {
		return nil, fmt.Errorf("search index %q: %w", indexName, err)
	}
	if resp.GetError() != "" {
		return nil, fmt.Errorf("search index %q failed: %s", indexName, resp.GetError())
	}

	results := make([]GrpcSearchResult, 0, len(resp.GetResults()))
	for _, result := range resp.GetResults() {
		results = append(results, GrpcSearchResult{
			ID:       result.GetId(),
			Distance: result.GetDistance(),
		})
	}
	return results, nil
}

// ===========================================================================
// Collection Operations
// ===========================================================================

// CreateCollection creates a new collection.
func (c *GrpcClient) CreateCollection(name string, dimension int, namespace string) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.collectionClient.CreateCollection(ctx, &sochdbv1.CreateCollectionRequest{
		Name:      name,
		Namespace: namespace,
		Dimension: uint32(dimension),
		Metric:    sochdbv1.DistanceMetric_DISTANCE_METRIC_COSINE,
	})
	if err != nil {
		return fmt.Errorf("create collection %q: %w", name, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("create collection %q failed: %s", name, resp.GetError())
	}
	return nil
}

// AddDocuments adds documents to a collection.
func (c *GrpcClient) AddDocuments(collectionName string, documents []GrpcDocument, namespace string) ([]string, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	protoDocs := make([]*sochdbv1.Document, 0, len(documents))
	for _, doc := range documents {
		protoDocs = append(protoDocs, &sochdbv1.Document{
			Id:        doc.ID,
			Content:   doc.Content,
			Embedding: doc.Embedding,
			Metadata:  doc.Metadata,
		})
	}

	resp, err := c.collectionClient.AddDocuments(ctx, &sochdbv1.AddDocumentsRequest{
		CollectionName: collectionName,
		Namespace:      namespace,
		Documents:      protoDocs,
	})
	if err != nil {
		return nil, fmt.Errorf("add documents to %q: %w", collectionName, err)
	}
	if resp.GetError() != "" {
		return resp.GetIds(), fmt.Errorf("add documents to %q failed: %s", collectionName, resp.GetError())
	}
	return resp.GetIds(), nil
}

// SearchCollection searches a collection for similar documents.
func (c *GrpcClient) SearchCollection(collectionName string, query []float32, k int, namespace string) ([]GrpcDocument, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.collectionClient.SearchCollection(ctx, &sochdbv1.SearchCollectionRequest{
		CollectionName: collectionName,
		Namespace:      namespace,
		Query:          query,
		K:              uint32(k),
	})
	if err != nil {
		return nil, fmt.Errorf("search collection %q: %w", collectionName, err)
	}
	if resp.GetError() != "" {
		return nil, fmt.Errorf("search collection %q failed: %s", collectionName, resp.GetError())
	}

	results := make([]GrpcDocument, 0, len(resp.GetResults()))
	for _, result := range resp.GetResults() {
		doc := result.GetDocument()
		if doc == nil {
			continue
		}
		results = append(results, GrpcDocument{
			ID:        doc.GetId(),
			Content:   doc.GetContent(),
			Embedding: doc.GetEmbedding(),
			Metadata:  doc.GetMetadata(),
		})
	}
	return results, nil
}

// HealthCheck returns server health for the vector index service.
func (c *GrpcClient) HealthCheck(indexName string) (string, string, []string, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.vectorIndexClient.HealthCheck(ctx, &sochdbv1.HealthCheckRequest{
		IndexName: indexName,
	})
	if err != nil {
		return "", "", nil, fmt.Errorf("health check failed: %w", err)
	}
	return resp.GetStatus().String(), resp.GetVersion(), resp.GetIndexes(), nil
}

func parseDistanceMetric(metric string) sochdbv1.DistanceMetric {
	switch metric {
	case "l2", "L2":
		return sochdbv1.DistanceMetric_DISTANCE_METRIC_L2
	case "dot", "dot_product", "DOT_PRODUCT":
		return sochdbv1.DistanceMetric_DISTANCE_METRIC_DOT_PRODUCT
	default:
		return sochdbv1.DistanceMetric_DISTANCE_METRIC_COSINE
	}
}

func flattenVectors(vectors [][]float32) ([]float32, error) {
	if len(vectors) == 0 {
		return []float32{}, nil
	}
	dimension := len(vectors[0])
	flat := make([]float32, 0, len(vectors)*dimension)
	for i, vector := range vectors {
		if len(vector) != dimension {
			return nil, fmt.Errorf("vector %d has dimension %d, expected %d", i, len(vector), dimension)
		}
		flat = append(flat, vector...)
	}
	return flat, nil
}

func graphNodeFromProto(node *sochdbv1.GraphNode) GrpcGraphNode {
	if node == nil {
		return GrpcGraphNode{}
	}
	return GrpcGraphNode{
		ID:         node.GetId(),
		NodeType:   node.GetNodeType(),
		Properties: node.GetProperties(),
	}
}

func graphEdgeFromProto(edge *sochdbv1.GraphEdge) GrpcGraphEdge {
	if edge == nil {
		return GrpcGraphEdge{}
	}
	return GrpcGraphEdge{
		FromID:     edge.GetFromId(),
		EdgeType:   edge.GetEdgeType(),
		ToID:       edge.GetToId(),
		Properties: edge.GetProperties(),
	}
}

func parseTraversalOrder(order string) sochdbv1.TraversalOrder {
	switch order {
	case "dfs", "DFS":
		return sochdbv1.TraversalOrder_TRAVERSAL_ORDER_DFS
	default:
		return sochdbv1.TraversalOrder_TRAVERSAL_ORDER_BFS
	}
}

func parseSpanStatus(status string) sochdbv1.SpanStatus {
	switch status {
	case "ok", "OK", "success", "SUCCESS":
		return sochdbv1.SpanStatus_SPAN_STATUS_OK
	case "error", "ERROR", "failed", "FAILED":
		return sochdbv1.SpanStatus_SPAN_STATUS_ERROR
	default:
		return sochdbv1.SpanStatus_SPAN_STATUS_UNSET
	}
}

// ===========================================================================
// Graph Operations
// ===========================================================================

// AddGraphNode adds a node to the graph.
func (c *GrpcClient) AddGraphNode(nodeID, nodeType string, properties map[string]string, namespace string) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.graphClient.AddNode(ctx, &sochdbv1.AddNodeRequest{
		Namespace: namespace,
		Node: &sochdbv1.GraphNode{
			Id:         nodeID,
			NodeType:   nodeType,
			Properties: properties,
		},
	})
	if err != nil {
		return fmt.Errorf("add graph node %q: %w", nodeID, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("add graph node %q failed: %s", nodeID, resp.GetError())
	}
	return nil
}

// AddGraphEdge adds an edge between nodes.
func (c *GrpcClient) AddGraphEdge(fromID, edgeType, toID string, properties map[string]string, namespace string) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.graphClient.AddEdge(ctx, &sochdbv1.AddEdgeRequest{
		Namespace: namespace,
		Edge: &sochdbv1.GraphEdge{
			FromId:     fromID,
			EdgeType:   edgeType,
			ToId:       toID,
			Properties: properties,
		},
	})
	if err != nil {
		return fmt.Errorf("add graph edge %q->%q (%s): %w", fromID, toID, edgeType, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("add graph edge %q->%q (%s) failed: %s", fromID, toID, edgeType, resp.GetError())
	}
	return nil
}

// TraverseGraph performs graph traversal from a starting node.
func (c *GrpcClient) TraverseGraph(startNode string, maxDepth int, order string, namespace string) ([]GrpcGraphNode, []GrpcGraphEdge, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.graphClient.Traverse(ctx, &sochdbv1.TraverseRequest{
		Namespace:   namespace,
		StartNodeId: startNode,
		MaxDepth:    uint32(maxDepth),
		Order:       parseTraversalOrder(order),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("traverse graph from %q: %w", startNode, err)
	}
	if resp.GetError() != "" {
		return nil, nil, fmt.Errorf("traverse graph from %q failed: %s", startNode, resp.GetError())
	}

	nodes := make([]GrpcGraphNode, 0, len(resp.GetNodes()))
	for _, node := range resp.GetNodes() {
		nodes = append(nodes, graphNodeFromProto(node))
	}

	edges := make([]GrpcGraphEdge, 0, len(resp.GetEdges()))
	for _, edge := range resp.GetEdges() {
		edges = append(edges, graphEdgeFromProto(edge))
	}

	return nodes, edges, nil
}

// ===========================================================================
// Semantic Cache Operations
// ===========================================================================

// CacheGet retrieves from semantic cache by similarity.
func (c *GrpcClient) CacheGet(cacheName string, queryEmbedding []float32, threshold float32) (string, bool, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.semanticCacheClient.Get(ctx, &sochdbv1.SemanticCacheGetRequest{
		CacheName:           cacheName,
		QueryEmbedding:      queryEmbedding,
		SimilarityThreshold: threshold,
	})
	if err != nil {
		return "", false, fmt.Errorf("semantic cache get %q: %w", cacheName, err)
	}
	return resp.GetCachedValue(), resp.GetHit(), nil
}

// CachePut stores a value in the semantic cache.
func (c *GrpcClient) CachePut(cacheName, key, value string, keyEmbedding []float32, ttlSeconds int) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.semanticCacheClient.Put(ctx, &sochdbv1.SemanticCachePutRequest{
		CacheName:    cacheName,
		Key:          key,
		Value:        value,
		KeyEmbedding: keyEmbedding,
		TtlSeconds:   uint64(ttlSeconds),
	})
	if err != nil {
		return fmt.Errorf("semantic cache put %q/%q: %w", cacheName, key, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("semantic cache put %q/%q failed: %s", cacheName, key, resp.GetError())
	}
	return nil
}

// ===========================================================================
// Trace Operations
// ===========================================================================

// StartTrace starts a new trace.
func (c *GrpcClient) StartTrace(name string) (traceID, rootSpanID string, err error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.traceClient.StartTrace(ctx, &sochdbv1.StartTraceRequest{
		Name:       name,
		Attributes: map[string]string{},
	})
	if err != nil {
		return "", "", fmt.Errorf("start trace %q: %w", name, err)
	}
	return resp.GetTraceId(), resp.GetRootSpanId(), nil
}

// StartSpan starts a span within a trace.
func (c *GrpcClient) StartSpan(traceID, parentSpanID, name string) (spanID string, err error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.traceClient.StartSpan(ctx, &sochdbv1.StartSpanRequest{
		TraceId:      traceID,
		ParentSpanId: parentSpanID,
		Name:         name,
		Attributes:   map[string]string{},
	})
	if err != nil {
		return "", fmt.Errorf("start span %q on trace %q: %w", name, traceID, err)
	}
	return resp.GetSpanId(), nil
}

// EndSpan ends a span.
func (c *GrpcClient) EndSpan(traceID, spanID, status string) (durationUs int64, err error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.traceClient.EndSpan(ctx, &sochdbv1.EndSpanRequest{
		TraceId:    traceID,
		SpanId:     spanID,
		Status:     parseSpanStatus(status),
		Attributes: map[string]string{},
	})
	if err != nil {
		return 0, fmt.Errorf("end span %q on trace %q: %w", spanID, traceID, err)
	}
	if !resp.GetSuccess() {
		return 0, fmt.Errorf("end span %q on trace %q failed", spanID, traceID)
	}
	return int64(resp.GetDurationUs()), nil
}

// ===========================================================================
// KV Operations
// ===========================================================================

// GrpcGet retrieves a value by key.
func (c *GrpcClient) GrpcGet(key []byte, namespace string) ([]byte, bool, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.kvClient.Get(ctx, &sochdbv1.KvGetRequest{
		Namespace: namespace,
		Key:       key,
	})
	if err != nil {
		return nil, false, fmt.Errorf("kv get %q: %w", string(key), err)
	}
	if resp.GetError() != "" {
		return nil, false, fmt.Errorf("kv get %q failed: %s", string(key), resp.GetError())
	}
	return resp.GetValue(), resp.GetFound(), nil
}

// GrpcPut stores a value.
func (c *GrpcClient) GrpcPut(key, value []byte, namespace string, ttlSeconds int) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.kvClient.Put(ctx, &sochdbv1.KvPutRequest{
		Namespace:  namespace,
		Key:        key,
		Value:      value,
		TtlSeconds: uint64(ttlSeconds),
	})
	if err != nil {
		return fmt.Errorf("kv put %q: %w", string(key), err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("kv put %q failed: %s", string(key), resp.GetError())
	}
	return nil
}

// GrpcDelete removes a key.
func (c *GrpcClient) GrpcDelete(key []byte, namespace string) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.kvClient.Delete(ctx, &sochdbv1.KvDeleteRequest{
		Namespace: namespace,
		Key:       key,
	})
	if err != nil {
		return fmt.Errorf("kv delete %q: %w", string(key), err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("kv delete %q failed: %s", string(key), resp.GetError())
	}
	return nil
}

// ===========================================================================
// Convenience Methods (Simpler API)
// ===========================================================================

// PutKv stores a key-value pair in the specified namespace.
func (c *GrpcClient) PutKv(ctx context.Context, namespace, key string, value []byte) error {
	return c.GrpcPut([]byte(key), value, namespace, 0)
}

// GetKv retrieves a value by key from the specified namespace.
func (c *GrpcClient) GetKv(ctx context.Context, namespace, key string) ([]byte, error) {
	value, found, err := c.GrpcGet([]byte(key), namespace)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return value, nil
}

// AddEdge adds an edge to the graph (convenience wrapper).
func (c *GrpcClient) AddEdge(ctx context.Context, namespace string, edge GrpcGraphEdge) error {
	return c.AddGraphEdge(edge.FromID, edge.EdgeType, edge.ToID, edge.Properties, namespace)
}

// QueryGraph queries the graph for edges (convenience wrapper).
func (c *GrpcClient) QueryGraph(ctx context.Context, namespace, fromID, edgeType string, limit int) ([]GrpcGraphEdge, error) {
	resp, err := c.graphClient.GetEdges(ctx, &sochdbv1.GetEdgesRequest{
		Namespace: namespace,
		NodeId:    fromID,
		EdgeType:  edgeType,
		Direction: sochdbv1.EdgeDirection_EDGE_DIRECTION_OUTGOING,
	})
	if err != nil {
		return nil, fmt.Errorf("query graph edges from %q: %w", fromID, err)
	}
	if resp.GetError() != "" {
		return nil, fmt.Errorf("query graph edges from %q failed: %s", fromID, resp.GetError())
	}

	edges := make([]GrpcGraphEdge, 0, len(resp.GetEdges()))
	for _, edge := range resp.GetEdges() {
		edges = append(edges, graphEdgeFromProto(edge))
		if limit > 0 && len(edges) >= limit {
			break
		}
	}
	return edges, nil
}
