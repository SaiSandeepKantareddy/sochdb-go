// Package sochdb provides namespace and collection APIs for type-safe isolation
//
// Provides type-safe namespace isolation with first-class namespace handles.
//
// Example:
//
//	import "github.com/sochdb/sochdb-go"
//	import "github.com/sochdb/sochdb-go/embedded"
//
//	db, err := embedded.Open("./mydb")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	ns, err := db.CreateNamespace("tenant_123")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	collection, err := ns.CreateCollection(CollectionConfig{
//	    Name:      "documents",
//	    Dimension: 384,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	err = collection.Insert([]float32{1.0, 2.0, ...}, map[string]interface{}{"source": "web"}, "")
package sochdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"
)

// ============================================================================
// Namespace Configuration
// ============================================================================

// NamespaceConfig represents namespace configuration
type NamespaceConfig struct {
	Name        string            `json:"name"`
	DisplayName string            `json:"display_name,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	ReadOnly    bool              `json:"read_only"`
}

// NamespaceNotFoundError is returned when a namespace doesn't exist
type NamespaceNotFoundError struct {
	Namespace string
}

func (e *NamespaceNotFoundError) Error() string {
	return fmt.Sprintf("namespace not found: %s", e.Namespace)
}

// NamespaceExistsError is returned when trying to create an existing namespace
type NamespaceExistsError struct {
	Namespace string
}

func (e *NamespaceExistsError) Error() string {
	return fmt.Sprintf("namespace already exists: %s", e.Namespace)
}

// CollectionNotFoundError is returned when a collection doesn't exist
type CollectionNotFoundError struct {
	Collection string
}

func (e *CollectionNotFoundError) Error() string {
	return fmt.Sprintf("collection not found: %s", e.Collection)
}

// CollectionExistsError is returned when trying to create an existing collection
type CollectionExistsError struct {
	Collection string
}

func (e *CollectionExistsError) Error() string {
	return fmt.Sprintf("collection already exists: %s", e.Collection)
}

// ============================================================================
// Collection Configuration
// ============================================================================

// DistanceMetric represents the distance metric for vector similarity
type DistanceMetric string

const (
	DistanceMetricCosine     DistanceMetric = "cosine"
	DistanceMetricEuclidean  DistanceMetric = "euclidean"
	DistanceMetricDotProduct DistanceMetric = "dot"
)

// CollectionConfig represents collection configuration
type CollectionConfig struct {
	Name               string                 `json:"name"`
	Dimension          int                    `json:"dimension,omitempty"`
	Metric             DistanceMetric         `json:"metric,omitempty"`
	Indexed            bool                   `json:"indexed"`
	HNSWM              int                    `json:"hnsw_m,omitempty"`
	HNSWEfConstruction int                    `json:"hnsw_ef_construction,omitempty"`
	Metadata           map[string]interface{} `json:"metadata,omitempty"`
}

// SearchRequest represents a vector search request
type SearchRequest struct {
	QueryVector     []float32              `json:"query_vector"`
	K               int                    `json:"k"`
	Filter          map[string]interface{} `json:"filter,omitempty"`
	IncludeMetadata bool                   `json:"include_metadata"`
}

// SearchResult represents a single search result
type SearchResult struct {
	ID       string                 `json:"id"`
	Score    float32                `json:"score"`
	Vector   []float32              `json:"vector,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// ============================================================================
// Collection Handle
// ============================================================================

// Collection represents a vector collection
type Collection struct {
	db        interface{}
	namespace string
	name      string
	config    CollectionConfig
}

// vectorData represents stored vector data
type vectorData struct {
	Vector    []float32              `json:"vector"`
	Metadata  map[string]interface{} `json:"metadata"`
	Timestamp int64                  `json:"timestamp"`
}

// Insert adds a vector to the collection
func (c *Collection) Insert(vector []float32, metadata map[string]interface{}, id string) (string, error) {
	if c.config.Dimension > 0 && len(vector) != c.config.Dimension {
		return "", fmt.Errorf("vector dimension mismatch: expected %d, got %d", c.config.Dimension, len(vector))
	}

	vectorID := id
	if vectorID == "" {
		vectorID = c.generateID()
	}

	key := c.vectorKey(vectorID)
	data := vectorData{
		Vector:    vector,
		Metadata:  metadata,
		Timestamp: time.Now().UnixMilli(),
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	// Put to database (supports both embedded and client interfaces)
	switch db := c.db.(type) {
	case interface{ Put([]byte, []byte) error }:
		err = db.Put([]byte(key), dataBytes)
	default:
		return "", errors.New("unsupported database type")
	}

	if err != nil {
		return "", err
	}

	return vectorID, nil
}

// InsertMany adds multiple vectors to the collection
func (c *Collection) InsertMany(vectors [][]float32, metadatas []map[string]interface{}, ids []string) ([]string, error) {
	resultIDs := make([]string, 0, len(vectors))

	for i, vector := range vectors {
		var id string
		if ids != nil && i < len(ids) {
			id = ids[i]
		}

		var metadata map[string]interface{}
		if metadatas != nil && i < len(metadatas) {
			metadata = metadatas[i]
		}

		resultID, err := c.Insert(vector, metadata, id)
		if err != nil {
			return resultIDs, err
		}

		resultIDs = append(resultIDs, resultID)
	}

	return resultIDs, nil
}

// Search finds similar vectors using brute-force cosine similarity
func (c *Collection) Search(request SearchRequest) ([]SearchResult, error) {
	results := make([]SearchResult, 0)
	prefix := []byte(c.vectorKeyPrefix())

	// Try scan-based approach
	type scanDB interface {
		ScanPrefix(prefix []byte) ScanIteratorInterface
	}
	if db, ok := c.db.(scanDB); ok {
		iter := db.ScanPrefix(prefix)
		defer iter.Close()

		for {
			keyBuf, valueBuf, ok := iter.Next()
			if !ok {
				break
			}
			var data vectorData
			if json.Unmarshal(valueBuf, &data) != nil {
				continue
			}

			// Apply metadata filter
			if request.Filter != nil && !matchesFilter(data.Metadata, request.Filter) {
				continue
			}

			score := computeCosineSimilarity(request.QueryVector, data.Vector)
			keyStr := string(keyBuf)
			id := keyStr[len(c.vectorKeyPrefix()):]

			result := SearchResult{ID: id, Score: score}
			if request.IncludeMetadata {
				result.Vector = data.Vector
				result.Metadata = data.Metadata
			}
			results = append(results, result)
		}

		if iter.Err() != nil {
			return nil, iter.Err()
		}
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit to K results
	if len(results) > request.K {
		results = results[:request.K]
	}

	return results, nil
}

// Get retrieves a vector by ID
func (c *Collection) Get(id string) (*vectorData, error) {
	key := c.vectorKey(id)

	var value []byte
	switch db := c.db.(type) {
	case interface{ Get([]byte) ([]byte, error) }:
		var err error
		value, err = db.Get([]byte(key))
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported database type")
	}

	if value == nil {
		return nil, nil
	}

	var data vectorData
	if err := json.Unmarshal(value, &data); err != nil {
		return nil, err
	}

	return &data, nil
}

// Delete removes a vector by ID
func (c *Collection) Delete(id string) error {
	key := c.vectorKey(id)

	switch db := c.db.(type) {
	case interface{ Delete([]byte) error }:
		return db.Delete([]byte(key))
	default:
		return errors.New("unsupported database type")
	}
}

// Count returns the number of vectors in the collection
func (c *Collection) Count() (int, error) {
	prefix := []byte(c.vectorKeyPrefix())
	count := 0

	type scanDB interface {
		ScanPrefix(prefix []byte) ScanIteratorInterface
	}
	if db, ok := c.db.(scanDB); ok {
		iter := db.ScanPrefix(prefix)
		defer iter.Close()
		for {
			_, _, ok := iter.Next()
			if !ok {
				break
			}
			count++
		}
	}

	return count, nil
}

// Helper methods
func (c *Collection) vectorKey(id string) string {
	return fmt.Sprintf("_collection/%s/%s/vectors/%s", c.namespace, c.name, id)
}

func (c *Collection) vectorKeyPrefix() string {
	return fmt.Sprintf("_collection/%s/%s/vectors/", c.namespace, c.name)
}

func (c *Collection) metadataKey() string {
	return fmt.Sprintf("_collection/%s/%s/metadata", c.namespace, c.name)
}

func (c *Collection) generateID() string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), randomString(9))
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// ============================================================================
// Namespace Handle
// ============================================================================

// Namespace represents a namespace handle
type Namespace struct {
	db     interface{}
	name   string
	config NamespaceConfig
}

// CreateCollection creates a new collection in this namespace
func (ns *Namespace) CreateCollection(config CollectionConfig) (*Collection, error) {
	metadataKey := fmt.Sprintf("_collection/%s/%s/metadata", ns.name, config.Name)

	// Check if collection already exists
	var existing []byte
	switch db := ns.db.(type) {
	case interface{ Get([]byte) ([]byte, error) }:
		var err error
		existing, err = db.Get([]byte(metadataKey))
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported database type")
	}

	if existing != nil {
		return nil, &CollectionExistsError{Collection: config.Name}
	}

	// Store collection metadata
	metadata := map[string]interface{}{
		"name":      config.Name,
		"dimension": config.Dimension,
		"metric":    config.Metric,
		"indexed":   config.Indexed,
		"createdAt": time.Now().UnixMilli(),
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}

	switch db := ns.db.(type) {
	case interface{ Put([]byte, []byte) error }:
		err = db.Put([]byte(metadataKey), metadataBytes)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported database type")
	}

	return &Collection{
		db:        ns.db,
		namespace: ns.name,
		name:      config.Name,
		config:    config,
	}, nil
}

// Collection gets an existing collection
func (ns *Namespace) Collection(name string) (*Collection, error) {
	metadataKey := fmt.Sprintf("_collection/%s/%s/metadata", ns.name, name)

	var metadata []byte
	switch db := ns.db.(type) {
	case interface{ Get([]byte) ([]byte, error) }:
		var err error
		metadata, err = db.Get([]byte(metadataKey))
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported database type")
	}

	if metadata == nil {
		return nil, &CollectionNotFoundError{Collection: name}
	}

	var config CollectionConfig
	if err := json.Unmarshal(metadata, &config); err != nil {
		return nil, err
	}

	return &Collection{
		db:        ns.db,
		namespace: ns.name,
		name:      name,
		config:    config,
	}, nil
}

// GetOrCreateCollection gets or creates a collection
func (ns *Namespace) GetOrCreateCollection(config CollectionConfig) (*Collection, error) {
	collection, err := ns.Collection(config.Name)
	if err != nil {
		if _, ok := err.(*CollectionNotFoundError); ok {
			return ns.CreateCollection(config)
		}
		return nil, err
	}
	return collection, nil
}

// DeleteCollection deletes a collection and all its data
func (ns *Namespace) DeleteCollection(name string) error {
	prefix := []byte(fmt.Sprintf("_collection/%s/%s/", ns.name, name))

	// Try scan-based approach to delete all collection keys
	type scanDeleteDB interface {
		ScanPrefix(prefix []byte) ScanIteratorInterface
		Delete([]byte) error
	}
	if db, ok := ns.db.(scanDeleteDB); ok {
		var toDelete [][]byte
		iter := db.ScanPrefix(prefix)
		for {
			keyBuf, _, ok := iter.Next()
			if !ok {
				break
			}
			keyCopy := make([]byte, len(keyBuf))
			copy(keyCopy, keyBuf)
			toDelete = append(toDelete, keyCopy)
		}
		iter.Close()

		for _, key := range toDelete {
			db.Delete(key)
		}
		return nil
	}

	// Fallback: just delete the metadata key
	metadataKey := fmt.Sprintf("_collection/%s/%s/metadata", ns.name, name)
	switch db := ns.db.(type) {
	case interface{ Delete([]byte) error }:
		return db.Delete([]byte(metadataKey))
	default:
		return errors.New("unsupported database type")
	}
}

// ListCollections lists all collections in this namespace
func (ns *Namespace) ListCollections() ([]string, error) {
	prefix := []byte(fmt.Sprintf("_collection/%s/", ns.name))
	prefixStr := string(prefix)
	collections := make(map[string]bool)

	// Try scan-based approach
	type scanDB interface {
		ScanPrefix(prefix []byte) ScanIteratorInterface
	}
	if db, ok := ns.db.(scanDB); ok {
		iter := db.ScanPrefix(prefix)
		defer iter.Close()

		for {
			keyBuf, _, ok := iter.Next()
			if !ok {
				break
			}
			key := string(keyBuf)
			if len(key) > len(prefixStr) {
				afterPrefix := key[len(prefixStr):]
				parts := strings.SplitN(afterPrefix, "/", 2)
				if len(parts) > 0 && parts[0] != "" {
					collections[parts[0]] = true
				}
			}
		}
	}

	result := make([]string, 0, len(collections))
	for name := range collections {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

// GetName returns the namespace name
func (ns *Namespace) GetName() string {
	return ns.name
}

// GetConfig returns the namespace config
func (ns *Namespace) GetConfig() NamespaceConfig {
	return ns.config
}

// ============================================================================
// Helper Functions
// ============================================================================

// computeCosineSimilarity computes cosine similarity between two vectors
func computeCosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	normA = math.Sqrt(normA)
	normB = math.Sqrt(normB)
	if normA == 0 || normB == 0 {
		return 0
	}
	return float32(dot / (normA * normB))
}

// matchesFilter checks if metadata matches a filter
func matchesFilter(metadata map[string]interface{}, filter map[string]interface{}) bool {
	if metadata == nil {
		return false
	}
	for key, expected := range filter {
		actual, ok := metadata[key]
		if !ok || fmt.Sprintf("%v", actual) != fmt.Sprintf("%v", expected) {
			return false
		}
	}
	return true
}
