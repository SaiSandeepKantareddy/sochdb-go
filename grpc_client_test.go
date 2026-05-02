package sochdb

import (
	"context"
	"net"
	"testing"
	"time"

	sochdbv1 "github.com/sochdb/sochdb-go/internal/gen/sochdbv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const testBufSize = 1024 * 1024

type testGraphServer struct {
	sochdbv1.UnimplementedGraphServiceServer
	lastAddNode  *sochdbv1.AddNodeRequest
	lastAddEdge  *sochdbv1.AddEdgeRequest
	lastTraverse *sochdbv1.TraverseRequest
	lastGetEdges *sochdbv1.GetEdgesRequest
}

func (s *testGraphServer) AddNode(_ context.Context, req *sochdbv1.AddNodeRequest) (*sochdbv1.AddNodeResponse, error) {
	s.lastAddNode = req
	return &sochdbv1.AddNodeResponse{Success: true}, nil
}

func (s *testGraphServer) AddEdge(_ context.Context, req *sochdbv1.AddEdgeRequest) (*sochdbv1.AddEdgeResponse, error) {
	s.lastAddEdge = req
	return &sochdbv1.AddEdgeResponse{Success: true}, nil
}

func (s *testGraphServer) Traverse(_ context.Context, req *sochdbv1.TraverseRequest) (*sochdbv1.TraverseResponse, error) {
	s.lastTraverse = req
	return &sochdbv1.TraverseResponse{
		Nodes: []*sochdbv1.GraphNode{
			{Id: "n1", NodeType: "doc", Properties: map[string]string{"topic": "sdk"}},
		},
		Edges: []*sochdbv1.GraphEdge{
			{FromId: "root", EdgeType: "links_to", ToId: "n1", Properties: map[string]string{"weight": "1"}},
		},
	}, nil
}

func (s *testGraphServer) GetEdges(_ context.Context, req *sochdbv1.GetEdgesRequest) (*sochdbv1.GetEdgesResponse, error) {
	s.lastGetEdges = req
	return &sochdbv1.GetEdgesResponse{
		Edges: []*sochdbv1.GraphEdge{
			{FromId: req.GetNodeId(), EdgeType: req.GetEdgeType(), ToId: "a"},
			{FromId: req.GetNodeId(), EdgeType: req.GetEdgeType(), ToId: "b"},
		},
	}, nil
}

type testSemanticCacheServer struct {
	sochdbv1.UnimplementedSemanticCacheServiceServer
	lastGet *sochdbv1.SemanticCacheGetRequest
	lastPut *sochdbv1.SemanticCachePutRequest
}

func (s *testSemanticCacheServer) Get(_ context.Context, req *sochdbv1.SemanticCacheGetRequest) (*sochdbv1.SemanticCacheGetResponse, error) {
	s.lastGet = req
	return &sochdbv1.SemanticCacheGetResponse{
		Hit:         true,
		CachedValue: "cached-response",
		MatchedKey:  "k1",
	}, nil
}

func (s *testSemanticCacheServer) Put(_ context.Context, req *sochdbv1.SemanticCachePutRequest) (*sochdbv1.SemanticCachePutResponse, error) {
	s.lastPut = req
	return &sochdbv1.SemanticCachePutResponse{Success: true}, nil
}

type testTraceServer struct {
	sochdbv1.UnimplementedTraceServiceServer
	lastStartTrace *sochdbv1.StartTraceRequest
	lastStartSpan  *sochdbv1.StartSpanRequest
	lastEndSpan    *sochdbv1.EndSpanRequest
}

func (s *testTraceServer) StartTrace(_ context.Context, req *sochdbv1.StartTraceRequest) (*sochdbv1.StartTraceResponse, error) {
	s.lastStartTrace = req
	return &sochdbv1.StartTraceResponse{TraceId: "trace-1", RootSpanId: "root-1"}, nil
}

func (s *testTraceServer) StartSpan(_ context.Context, req *sochdbv1.StartSpanRequest) (*sochdbv1.StartSpanResponse, error) {
	s.lastStartSpan = req
	return &sochdbv1.StartSpanResponse{SpanId: "span-2"}, nil
}

func (s *testTraceServer) EndSpan(_ context.Context, req *sochdbv1.EndSpanRequest) (*sochdbv1.EndSpanResponse, error) {
	s.lastEndSpan = req
	return &sochdbv1.EndSpanResponse{Success: true, DurationUs: 1234}, nil
}

type testKVServer struct {
	sochdbv1.UnimplementedKvServiceServer
	lastGet    *sochdbv1.KvGetRequest
	lastPut    *sochdbv1.KvPutRequest
	lastDelete *sochdbv1.KvDeleteRequest
}

func (s *testKVServer) Get(_ context.Context, req *sochdbv1.KvGetRequest) (*sochdbv1.KvGetResponse, error) {
	s.lastGet = req
	return &sochdbv1.KvGetResponse{Value: []byte("value-1"), Found: true}, nil
}

func (s *testKVServer) Put(_ context.Context, req *sochdbv1.KvPutRequest) (*sochdbv1.KvPutResponse, error) {
	s.lastPut = req
	return &sochdbv1.KvPutResponse{Success: true}, nil
}

func (s *testKVServer) Delete(_ context.Context, req *sochdbv1.KvDeleteRequest) (*sochdbv1.KvDeleteResponse, error) {
	s.lastDelete = req
	return &sochdbv1.KvDeleteResponse{Success: true}, nil
}

func newTestGRPCClient(t *testing.T) (*GrpcClient, *testGraphServer, *testSemanticCacheServer, *testTraceServer, *testKVServer) {
	t.Helper()

	lis := bufconn.Listen(testBufSize)
	server := grpc.NewServer()

	graphSrv := &testGraphServer{}
	cacheSrv := &testSemanticCacheServer{}
	traceSrv := &testTraceServer{}
	kvSrv := &testKVServer{}

	sochdbv1.RegisterGraphServiceServer(server, graphSrv)
	sochdbv1.RegisterSemanticCacheServiceServer(server, cacheSrv)
	sochdbv1.RegisterTraceServiceServer(server, traceSrv)
	sochdbv1.RegisterKvServiceServer(server, kvSrv)

	go func() {
		_ = server.Serve(lis)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
	})

	client := &GrpcClient{
		conn:                conn,
		address:             "bufnet",
		timeout:             5 * time.Second,
		graphClient:         sochdbv1.NewGraphServiceClient(conn),
		semanticCacheClient: sochdbv1.NewSemanticCacheServiceClient(conn),
		traceClient:         sochdbv1.NewTraceServiceClient(conn),
		kvClient:            sochdbv1.NewKvServiceClient(conn),
	}

	return client, graphSrv, cacheSrv, traceSrv, kvSrv
}

func TestGrpcClientGraphOperations(t *testing.T) {
	client, graphSrv, _, _, _ := newTestGRPCClient(t)

	if err := client.AddGraphNode("root", "collection", map[string]string{"env": "test"}, "ns1"); err != nil {
		t.Fatalf("AddGraphNode failed: %v", err)
	}
	if graphSrv.lastAddNode.GetNamespace() != "ns1" || graphSrv.lastAddNode.GetNode().GetId() != "root" {
		t.Fatalf("unexpected AddNode request: %+v", graphSrv.lastAddNode)
	}

	if err := client.AddGraphEdge("root", "links_to", "leaf", map[string]string{"kind": "demo"}, "ns1"); err != nil {
		t.Fatalf("AddGraphEdge failed: %v", err)
	}
	if graphSrv.lastAddEdge.GetEdge().GetToId() != "leaf" {
		t.Fatalf("unexpected AddEdge request: %+v", graphSrv.lastAddEdge)
	}

	nodes, edges, err := client.TraverseGraph("root", 3, "dfs", "ns1")
	if err != nil {
		t.Fatalf("TraverseGraph failed: %v", err)
	}
	if graphSrv.lastTraverse.GetStartNodeId() != "root" || graphSrv.lastTraverse.GetOrder() != sochdbv1.TraversalOrder_TRAVERSAL_ORDER_DFS {
		t.Fatalf("unexpected Traverse request: %+v", graphSrv.lastTraverse)
	}
	if len(nodes) != 1 || nodes[0].ID != "n1" || len(edges) != 1 || edges[0].ToID != "n1" {
		t.Fatalf("unexpected TraverseGraph response: nodes=%+v edges=%+v", nodes, edges)
	}

	queryEdges, err := client.QueryGraph(context.Background(), "ns1", "root", "links_to", 1)
	if err != nil {
		t.Fatalf("QueryGraph failed: %v", err)
	}
	if graphSrv.lastGetEdges.GetDirection() != sochdbv1.EdgeDirection_EDGE_DIRECTION_OUTGOING {
		t.Fatalf("unexpected GetEdges direction: %+v", graphSrv.lastGetEdges)
	}
	if len(queryEdges) != 1 || queryEdges[0].ToID != "a" {
		t.Fatalf("unexpected QueryGraph response: %+v", queryEdges)
	}
}

func TestGrpcClientSemanticCacheOperations(t *testing.T) {
	client, _, cacheSrv, _, _ := newTestGRPCClient(t)

	value, hit, err := client.CacheGet("cache1", []float32{0.1, 0.2}, 0.85)
	if err != nil {
		t.Fatalf("CacheGet failed: %v", err)
	}
	if !hit || value != "cached-response" {
		t.Fatalf("unexpected CacheGet response: hit=%v value=%q", hit, value)
	}
	if cacheSrv.lastGet.GetSimilarityThreshold() != 0.85 {
		t.Fatalf("unexpected CacheGet request: %+v", cacheSrv.lastGet)
	}

	if err := client.CachePut("cache1", "k1", "v1", []float32{0.3, 0.4}, 30); err != nil {
		t.Fatalf("CachePut failed: %v", err)
	}
	if cacheSrv.lastPut.GetTtlSeconds() != 30 || cacheSrv.lastPut.GetKey() != "k1" {
		t.Fatalf("unexpected CachePut request: %+v", cacheSrv.lastPut)
	}
}

func TestGrpcClientTraceAndKVOperations(t *testing.T) {
	client, _, _, traceSrv, kvSrv := newTestGRPCClient(t)

	traceID, rootSpanID, err := client.StartTrace("sdk-test")
	if err != nil {
		t.Fatalf("StartTrace failed: %v", err)
	}
	if traceID != "trace-1" || rootSpanID != "root-1" || traceSrv.lastStartTrace.GetName() != "sdk-test" {
		t.Fatalf("unexpected StartTrace state: traceID=%q rootSpanID=%q req=%+v", traceID, rootSpanID, traceSrv.lastStartTrace)
	}

	spanID, err := client.StartSpan(traceID, rootSpanID, "child")
	if err != nil {
		t.Fatalf("StartSpan failed: %v", err)
	}
	if spanID != "span-2" || traceSrv.lastStartSpan.GetParentSpanId() != "root-1" {
		t.Fatalf("unexpected StartSpan state: spanID=%q req=%+v", spanID, traceSrv.lastStartSpan)
	}

	durationUs, err := client.EndSpan(traceID, spanID, "ok")
	if err != nil {
		t.Fatalf("EndSpan failed: %v", err)
	}
	if durationUs != 1234 || traceSrv.lastEndSpan.GetStatus() != sochdbv1.SpanStatus_SPAN_STATUS_OK {
		t.Fatalf("unexpected EndSpan state: duration=%d req=%+v", durationUs, traceSrv.lastEndSpan)
	}

	if err := client.GrpcPut([]byte("k1"), []byte("v1"), "ns1", 60); err != nil {
		t.Fatalf("GrpcPut failed: %v", err)
	}
	if kvSrv.lastPut.GetTtlSeconds() != 60 {
		t.Fatalf("unexpected GrpcPut request: %+v", kvSrv.lastPut)
	}

	value, found, err := client.GrpcGet([]byte("k1"), "ns1")
	if err != nil {
		t.Fatalf("GrpcGet failed: %v", err)
	}
	if !found || string(value) != "value-1" || string(kvSrv.lastGet.GetKey()) != "k1" {
		t.Fatalf("unexpected GrpcGet state: found=%v value=%q req=%+v", found, string(value), kvSrv.lastGet)
	}

	if err := client.GrpcDelete([]byte("k1"), "ns1"); err != nil {
		t.Fatalf("GrpcDelete failed: %v", err)
	}
	if string(kvSrv.lastDelete.GetKey()) != "k1" {
		t.Fatalf("unexpected GrpcDelete request: %+v", kvSrv.lastDelete)
	}
}
