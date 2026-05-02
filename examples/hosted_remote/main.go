package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sochdb/sochdb-go"
)

const (
	defaultGrpcAddress = "studio.agentslab.host:50053"
	defaultNamespace   = "default"
)

func main() {
	grpcAddress := envOrDefault("SOCHDB_GRPC_ADDRESS", defaultGrpcAddress)
	namespace := envOrDefault("SOCHDB_NAMESPACE", defaultNamespace)
	runID := fmt.Sprintf("go-sdk-demo-%d", time.Now().Unix())
	collectionName := "sdk_demo_docs_go_" + runID

	client, err := sochdb.GrpcConnect(grpcAddress)
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	fmt.Printf("Connecting to remote SochDB at %s\n", grpcAddress)
	if err := client.CreateCollection(collectionName, 4, namespace); err != nil {
		log.Fatalf("create collection failed: %v", err)
	}

	insertedIDs, err := client.AddDocuments(collectionName, []sochdb.GrpcDocument{
		{
			ID:        runID + "-doc-1",
			Content:   "Go SDK can write to the hosted SochDB collection.",
			Embedding: []float32{1, 0, 0, 0},
			Metadata:  map[string]string{"source": "go-sdk", "run_id": runID, "topic": "studio"},
		},
		{
			ID:        runID + "-doc-2",
			Content:   "Go remote parity should match Python and Node behavior.",
			Embedding: []float32{0, 1, 0, 0},
			Metadata:  map[string]string{"source": "go-sdk", "run_id": runID, "topic": "parity"},
		},
		{
			ID:        runID + "-doc-3",
			Content:   "Hosted remote validation should use the same gRPC endpoint across SDKs.",
			Embedding: []float32{0, 0, 1, 0},
			Metadata:  map[string]string{"source": "go-sdk", "run_id": runID, "topic": "remote"},
		},
	}, namespace)
	if err != nil {
		log.Fatalf("add documents failed: %v", err)
	}

	results, err := client.SearchCollection(collectionName, []float32{1, 0, 0, 0}, 2, namespace)
	if err != nil {
		log.Fatalf("search collection failed: %v", err)
	}

	fmt.Printf("Inserted %d documents into %s\n", len(insertedIDs), collectionName)
	fmt.Printf("Search returned %d document(s)\n", len(results))
	for i, doc := range results {
		fmt.Printf("  %d. %s\n", i+1, doc.ID)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
