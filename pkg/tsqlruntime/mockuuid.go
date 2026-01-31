package tsqlruntime

import (
	"fmt"
	"sync/atomic"
)

// mockUUIDCounter is used to generate predictable sequential UUIDs for testing.
var mockUUIDCounter uint64

// NextMockUUID generates a predictable sequential UUID for testing.
// UUIDs are formatted as: 00000000-0000-0000-0000-000000000001, etc.
// This allows tests to assert on specific UUID values.
func NextMockUUID() string {
	n := atomic.AddUint64(&mockUUIDCounter, 1)
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", n)
}

// ResetMockUUID resets the mock UUID counter to zero.
// Call this at the start of each test to ensure predictable UUIDs.
func ResetMockUUID() {
	atomic.StoreUint64(&mockUUIDCounter, 0)
}

// SetMockUUID sets the mock UUID counter to a specific value.
// The next call to NextMockUUID will return value+1.
func SetMockUUID(value uint64) {
	atomic.StoreUint64(&mockUUIDCounter, value)
}

// GetMockUUIDCounter returns the current mock UUID counter value.
func GetMockUUIDCounter() uint64 {
	return atomic.LoadUint64(&mockUUIDCounter)
}
