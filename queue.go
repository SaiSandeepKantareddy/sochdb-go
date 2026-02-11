// Package sochdb provides priority queue functionality
//
// First-class queue API with ordered-key task entries, providing efficient
// priority queue operations without the O(N) blob rewrite anti-pattern.
//
// Features:
// - Ordered-key representation: Each task has its own key, no blob parsing
// - O(log N) enqueue/dequeue with ordered scans
// - Atomic claim protocol for concurrent workers
// - Visibility timeout for crash recovery
//
// Example:
//
//	import "github.com/sochdb/sochdb-go"
//	import "github.com/sochdb/sochdb-go/embedded"
//
//	db, err := embedded.Open("./queue_db")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	queue := sochdb.NewPriorityQueue(db, "tasks", nil)
//
//	// Enqueue task
//	taskID, err := queue.Enqueue(1, []byte("high priority task"), nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Dequeue and process
//	task, err := queue.Dequeue("worker-1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if task != nil {
//	    // Process task...
//	    err = queue.Ack(task.TaskID)
//	}
package sochdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// ============================================================================
// Task State
// ============================================================================

// TaskState represents the state of a queue task
type TaskState string

const (
	TaskStatePending      TaskState = "pending"
	TaskStateClaimed      TaskState = "claimed"
	TaskStateCompleted    TaskState = "completed"
	TaskStateDeadLettered TaskState = "dead_lettered"
)

// ============================================================================
// Queue Configuration
// ============================================================================

// QueueConfig represents queue configuration
type QueueConfig struct {
	Name              string
	VisibilityTimeout int    // milliseconds, default 30000
	MaxRetries        int    // default 3
	DeadLetterQueue   string // optional
}

// ============================================================================
// Queue Key Encoding
// ============================================================================

// encodeU64BE encodes a u64 as big-endian for lexicographic ordering
func encodeU64BE(value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}

// decodeU64BE decodes a big-endian u64
func decodeU64BE(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf[:8])
}

// encodeI64BE encodes an i64 as big-endian preserving order
func encodeI64BE(value int64) []byte {
	// Map i64 to u64 by adding offset
	mapped := uint64(value) + (1 << 63)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, mapped)
	return buf
}

// decodeI64BE decodes a big-endian i64
func decodeI64BE(buf []byte) int64 {
	mapped := binary.BigEndian.Uint64(buf[:8])
	return int64(mapped - (1 << 63))
}

// ============================================================================
// Queue Key
// ============================================================================

// QueueKey represents a composite key for queue entries
type QueueKey struct {
	QueueID  string
	Priority int64
	ReadyTs  int64 // timestamp in milliseconds
	Sequence uint64
	TaskID   string
}

// Encode encodes the queue key to bytes
func (qk *QueueKey) Encode() []byte {
	parts := [][]byte{
		[]byte("queue/"),
		[]byte(qk.QueueID),
		[]byte("/"),
		encodeI64BE(qk.Priority),
		[]byte("/"),
		encodeU64BE(uint64(qk.ReadyTs)),
		[]byte("/"),
		encodeU64BE(qk.Sequence),
		[]byte("/"),
		[]byte(qk.TaskID),
	}

	totalLen := 0
	for _, part := range parts {
		totalLen += len(part)
	}

	result := make([]byte, totalLen)
	offset := 0
	for _, part := range parts {
		copy(result[offset:], part)
		offset += len(part)
	}

	return result
}

// ============================================================================
// Task
// ============================================================================

// Task represents a queue task
type Task struct {
	TaskID      string                 `json:"task_id"`
	Priority    int64                  `json:"priority"`
	Payload     []byte                 `json:"payload"`
	State       TaskState              `json:"state"`
	EnqueuedAt  int64                  `json:"enqueued_at"`
	ClaimedAt   *int64                 `json:"claimed_at,omitempty"`
	ClaimedBy   string                 `json:"claimed_by,omitempty"`
	CompletedAt *int64                 `json:"completed_at,omitempty"`
	Retries     int                    `json:"retries"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// ============================================================================
// Queue Statistics
// ============================================================================

// QueueStats represents queue statistics
type QueueStats struct {
	Pending       int `json:"pending"`
	Claimed       int `json:"claimed"`
	Completed     int `json:"completed"`
	DeadLettered  int `json:"dead_lettered"`
	TotalEnqueued int `json:"total_enqueued"`
	TotalDequeued int `json:"total_dequeued"`
}

// ============================================================================
// Priority Queue
// ============================================================================

// PriorityQueue represents a priority queue
type PriorityQueue struct {
	db              interface{}
	config          QueueConfig
	sequenceCounter uint64
}

// NewPriorityQueue creates a new priority queue
func NewPriorityQueue(db interface{}, name string, config *QueueConfig) *PriorityQueue {
	cfg := QueueConfig{
		Name:              name,
		VisibilityTimeout: 30000,
		MaxRetries:        3,
	}

	if config != nil {
		if config.VisibilityTimeout > 0 {
			cfg.VisibilityTimeout = config.VisibilityTimeout
		}
		if config.MaxRetries > 0 {
			cfg.MaxRetries = config.MaxRetries
		}
		cfg.DeadLetterQueue = config.DeadLetterQueue
	}

	return &PriorityQueue{
		db:              db,
		config:          cfg,
		sequenceCounter: 0,
	}
}

// Enqueue adds a task to the queue with priority
// Lower priority number = higher urgency
func (pq *PriorityQueue) Enqueue(priority int64, payload []byte, metadata map[string]interface{}) (string, error) {
	taskID := pq.generateTaskID()
	now := time.Now().UnixMilli()

	key := QueueKey{
		QueueID:  pq.config.Name,
		Priority: priority,
		ReadyTs:  now,
		Sequence: pq.sequenceCounter,
		TaskID:   taskID,
	}
	pq.sequenceCounter++

	task := Task{
		TaskID:     taskID,
		Priority:   priority,
		Payload:    payload,
		State:      TaskStatePending,
		EnqueuedAt: now,
		Retries:    0,
		Metadata:   metadata,
	}

	keyBuf := key.Encode()
	valueBuf, err := json.Marshal(task)
	if err != nil {
		return "", err
	}

	switch db := pq.db.(type) {
	case interface{ Put([]byte, []byte) error }:
		err = db.Put(keyBuf, valueBuf)
		if err != nil {
			return "", err
		}
	default:
		return "", errors.New("unsupported database type")
	}

	// Update stats
	pq.incrementStat("totalEnqueued")
	pq.incrementStat("pending")

	return taskID, nil
}

// ScanPrefixDB is the interface databases must implement for scan-based operations
type ScanPrefixDB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	ScanPrefix(prefix []byte) ScanIteratorInterface
}

// ScanIteratorInterface is the interface for scan iterators
type ScanIteratorInterface interface {
	Next() (key []byte, value []byte, ok bool)
	Close()
	Err() error
}

// Dequeue gets the highest priority task
// Returns nil if no tasks available
func (pq *PriorityQueue) Dequeue(workerID string) (*Task, error) {
	nowMs := time.Now().UnixMilli()
	prefix := []byte(fmt.Sprintf("queue/%s/", pq.config.Name))

	// Try scan-based approach first (embedded database with ScanPrefix on txn)
	type scanDB interface {
		ScanPrefix(prefix []byte) ScanIteratorInterface
		Put(key, value []byte) error
	}

	if db, ok := pq.db.(scanDB); ok {
		iter := db.ScanPrefix(prefix)
		defer iter.Close()

		for {
			keyBuf, valueBuf, ok := iter.Next()
			if !ok {
				break
			}

			var task Task
			if err := json.Unmarshal(valueBuf, &task); err != nil {
				continue
			}

			if task.State != TaskStatePending {
				continue
			}

			// Decode key to check readyTs
			qk, err := DecodeQueueKey(keyBuf)
			if err != nil {
				continue
			}
			if qk.ReadyTs > nowMs {
				continue
			}

			// Claim this task
			task.State = TaskStateClaimed
			claimedAt := nowMs
			task.ClaimedAt = &claimedAt
			task.ClaimedBy = workerID

			updatedValue, _ := json.Marshal(task)
			if err := db.Put(keyBuf, updatedValue); err != nil {
				return nil, err
			}

			pq.decrementStat("pending")
			pq.incrementStat("claimed")
			pq.incrementStat("totalDequeued")

			return &task, nil
		}

		if iter.Err() != nil {
			return nil, iter.Err()
		}
	}

	// Fallback: use IPC scan if available
	type scanIPCDB interface {
		Scan(prefix string) ([]KeyValue, error)
		Put(key, value []byte) error
	}

	if db, ok := pq.db.(scanIPCDB); ok {
		kvs, err := db.Scan(string(prefix))
		if err != nil {
			return nil, err
		}

		for _, kv := range kvs {
			var task Task
			if err := json.Unmarshal(kv.Value, &task); err != nil {
				continue
			}

			if task.State != TaskStatePending {
				continue
			}

			task.State = TaskStateClaimed
			claimedAt := nowMs
			task.ClaimedAt = &claimedAt
			task.ClaimedBy = workerID

			updatedValue, _ := json.Marshal(task)
			if err := db.Put(kv.Key, updatedValue); err != nil {
				return nil, err
			}

			pq.decrementStat("pending")
			pq.incrementStat("claimed")
			pq.incrementStat("totalDequeued")

			return &task, nil
		}
	}

	return nil, nil
}

// DecodeQueueKey decodes a queue key from bytes using positional parsing.
// Key format: "queue/" + queueId + "/" + i64BE(priority) + "/" + u64BE(readyTs) + "/" + u64BE(sequence) + "/" + taskId
// Binary fields may contain 0x2F ('/'), so split('/') is NOT safe.
func DecodeQueueKey(data []byte) (*QueueKey, error) {
	prefix := []byte("queue/")
	if len(data) < len(prefix) || !bytes.Equal(data[:len(prefix)], prefix) {
		return nil, errors.New("invalid queue key: missing queue/ prefix")
	}

	offset := len(prefix)

	// Find queueId: scan for '/' that leaves enough room for 8+1+8+1+8+1+taskId = at least 28 bytes
	queueIDEnd := -1
	for i := offset; i < len(data); i++ {
		if data[i] == '/' {
			remaining := len(data) - i - 1
			if remaining >= 28 { // 8+1+8+1+8+1+1 minimum
				queueIDEnd = i
				break
			}
		}
	}
	if queueIDEnd < 0 {
		return nil, errors.New("invalid queue key: cannot find queueId boundary")
	}

	queueID := string(data[offset:queueIDEnd])
	offset = queueIDEnd + 1

	// Read priority (8 bytes)
	if offset+8 > len(data) {
		return nil, errors.New("invalid queue key: truncated priority")
	}
	priority := decodeI64BE(data[offset : offset+8])
	offset += 8

	if offset >= len(data) || data[offset] != '/' {
		return nil, errors.New("invalid queue key: expected / after priority")
	}
	offset++

	// Read readyTs (8 bytes)
	if offset+8 > len(data) {
		return nil, errors.New("invalid queue key: truncated readyTs")
	}
	readyTs := int64(decodeU64BE(data[offset : offset+8]))
	offset += 8

	if offset >= len(data) || data[offset] != '/' {
		return nil, errors.New("invalid queue key: expected / after readyTs")
	}
	offset++

	// Read sequence (8 bytes)
	if offset+8 > len(data) {
		return nil, errors.New("invalid queue key: truncated sequence")
	}
	sequence := decodeU64BE(data[offset : offset+8])
	offset += 8

	if offset >= len(data) || data[offset] != '/' {
		return nil, errors.New("invalid queue key: expected / after sequence")
	}
	offset++

	taskID := string(data[offset:])

	return &QueueKey{
		QueueID:  queueID,
		Priority: priority,
		ReadyTs:  readyTs,
		Sequence: sequence,
		TaskID:   taskID,
	}, nil
}

// Ack acknowledges task completion
func (pq *PriorityQueue) Ack(taskID string) error {
	task, err := pq.getTask(taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("task not found: %s", taskID)
	}

	if task.State != TaskStateClaimed {
		return fmt.Errorf("task not in claimed state: %s", taskID)
	}

	// Update task state
	task.State = TaskStateCompleted
	completedAt := time.Now().UnixMilli()
	task.CompletedAt = &completedAt

	if err := pq.updateTask(task); err != nil {
		return err
	}

	// Update stats
	pq.decrementStat("claimed")
	pq.incrementStat("completed")

	return nil
}

// Nack returns a task to the queue (negative acknowledge)
func (pq *PriorityQueue) Nack(taskID string) error {
	task, err := pq.getTask(taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("task not found: %s", taskID)
	}

	task.Retries++

	if task.Retries >= pq.config.MaxRetries {
		// Move to dead letter queue
		task.State = TaskStateDeadLettered
		if err := pq.updateTask(task); err != nil {
			return err
		}
		pq.decrementStat("claimed")
		pq.incrementStat("deadLettered")
	} else {
		// Return to pending
		task.State = TaskStatePending
		task.ClaimedAt = nil
		task.ClaimedBy = ""
		if err := pq.updateTask(task); err != nil {
			return err
		}
		pq.decrementStat("claimed")
		pq.incrementStat("pending")
	}

	return nil
}

// Stats returns queue statistics
func (pq *PriorityQueue) Stats() (*QueueStats, error) {
	return &QueueStats{
		Pending:       pq.getStat("pending"),
		Claimed:       pq.getStat("claimed"),
		Completed:     pq.getStat("completed"),
		DeadLettered:  pq.getStat("deadLettered"),
		TotalEnqueued: pq.getStat("totalEnqueued"),
		TotalDequeued: pq.getStat("totalDequeued"),
	}, nil
}

// Purge removes completed and dead-lettered tasks
func (pq *PriorityQueue) Purge() (int, error) {
	prefix := []byte(fmt.Sprintf("queue/%s/", pq.config.Name))
	purged := 0

	// Try scan-based approach
	type scanDeleteDB interface {
		ScanPrefix(prefix []byte) ScanIteratorInterface
		Delete(key []byte) error
	}

	if db, ok := pq.db.(scanDeleteDB); ok {
		var toDelete [][]byte
		iter := db.ScanPrefix(prefix)
		for {
			keyBuf, valueBuf, ok := iter.Next()
			if !ok {
				break
			}
			var task Task
			if json.Unmarshal(valueBuf, &task) != nil {
				continue
			}
			if task.State == TaskStateCompleted || task.State == TaskStateDeadLettered {
				keysCopy := make([]byte, len(keyBuf))
				copy(keysCopy, keyBuf)
				toDelete = append(toDelete, keysCopy)
			}
		}
		iter.Close()

		for _, key := range toDelete {
			if db.Delete(key) == nil {
				purged++
			}
		}
		return purged, nil
	}

	// Fallback: IPC scan
	type scanIPCDeleteDB interface {
		Scan(prefix string) ([]KeyValue, error)
		Delete(key []byte) error
	}

	if db, ok := pq.db.(scanIPCDeleteDB); ok {
		kvs, err := db.Scan(string(prefix))
		if err != nil {
			return 0, err
		}
		for _, kv := range kvs {
			var task Task
			if json.Unmarshal(kv.Value, &task) != nil {
				continue
			}
			if task.State == TaskStateCompleted || task.State == TaskStateDeadLettered {
				if db.Delete(kv.Key) == nil {
					purged++
				}
			}
		}
		return purged, nil
	}

	return 0, nil
}

// Helper methods
func (pq *PriorityQueue) generateTaskID() string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), randomTaskString(9))
}

func randomTaskString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (pq *PriorityQueue) getTask(taskID string) (*Task, error) {
	prefix := []byte(fmt.Sprintf("queue/%s/", pq.config.Name))

	// Try scan-based lookup
	type scanDB interface {
		ScanPrefix(prefix []byte) ScanIteratorInterface
	}

	if db, ok := pq.db.(scanDB); ok {
		iter := db.ScanPrefix(prefix)
		defer iter.Close()

		for {
			_, valueBuf, ok := iter.Next()
			if !ok {
				break
			}
			var task Task
			if json.Unmarshal(valueBuf, &task) != nil {
				continue
			}
			if task.TaskID == taskID {
				return &task, nil
			}
		}
		return nil, nil
	}

	// Fallback: IPC scan
	type scanIPCDB interface {
		Scan(prefix string) ([]KeyValue, error)
	}

	if db, ok := pq.db.(scanIPCDB); ok {
		kvs, err := db.Scan(string(prefix))
		if err != nil {
			return nil, err
		}
		for _, kv := range kvs {
			var task Task
			if json.Unmarshal(kv.Value, &task) != nil {
				continue
			}
			if task.TaskID == taskID {
				return &task, nil
			}
		}
	}

	return nil, nil
}

func (pq *PriorityQueue) updateTask(task *Task) error {
	prefix := []byte(fmt.Sprintf("queue/%s/", pq.config.Name))

	// Try scan-based lookup and update
	type scanPutDB interface {
		ScanPrefix(prefix []byte) ScanIteratorInterface
		Put(key, value []byte) error
	}

	if db, ok := pq.db.(scanPutDB); ok {
		iter := db.ScanPrefix(prefix)
		defer iter.Close()

		for {
			keyBuf, valueBuf, ok := iter.Next()
			if !ok {
				break
			}
			var existing Task
			if json.Unmarshal(valueBuf, &existing) != nil {
				continue
			}
			if existing.TaskID == task.TaskID {
				updatedValue, _ := json.Marshal(task)
				return db.Put(keyBuf, updatedValue)
			}
		}
		return nil
	}

	// Fallback: IPC scan
	type scanIPCPutDB interface {
		Scan(prefix string) ([]KeyValue, error)
		Put(key, value []byte) error
	}

	if db, ok := pq.db.(scanIPCPutDB); ok {
		kvs, err := db.Scan(string(prefix))
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			var existing Task
			if json.Unmarshal(kv.Value, &existing) != nil {
				continue
			}
			if existing.TaskID == task.TaskID {
				updatedValue, _ := json.Marshal(task)
				return db.Put(kv.Key, updatedValue)
			}
		}
	}

	return nil
}

func (pq *PriorityQueue) getStat(name string) int {
	key := fmt.Sprintf("_queue_stats/%s/%s", pq.config.Name, name)

	var value []byte
	switch db := pq.db.(type) {
	case interface{ Get([]byte) ([]byte, error) }:
		var err error
		value, err = db.Get([]byte(key))
		if err != nil {
			return 0
		}
	default:
		return 0
	}

	if value == nil {
		return 0
	}

	var count int
	json.Unmarshal(value, &count)
	return count
}

func (pq *PriorityQueue) incrementStat(name string) {
	current := pq.getStat(name)
	key := fmt.Sprintf("_queue_stats/%s/%s", pq.config.Name, name)
	valueBytes, _ := json.Marshal(current + 1)

	switch db := pq.db.(type) {
	case interface{ Put([]byte, []byte) error }:
		db.Put([]byte(key), valueBytes)
	}
}

func (pq *PriorityQueue) decrementStat(name string) {
	current := pq.getStat(name)
	if current > 0 {
		key := fmt.Sprintf("_queue_stats/%s/%s", pq.config.Name, name)
		valueBytes, _ := json.Marshal(current - 1)

		switch db := pq.db.(type) {
		case interface{ Put([]byte, []byte) error }:
			db.Put([]byte(key), valueBytes)
		}
	}
}

// CreateQueue creates a new queue instance (convenience function)
func CreateQueue(db interface{}, name string, config *QueueConfig) *PriorityQueue {
	return NewPriorityQueue(db, name, config)
}
