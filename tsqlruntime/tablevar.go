// Package tsqlruntime provides table variable support for transpiled T-SQL code.
package tsqlruntime

import (
	"database/sql"
	"reflect"
)

// -----------------------------------------------------------------------------
// TableRow Interface
// -----------------------------------------------------------------------------

// TableRow is the interface implemented by all generated table variable row types.
// It provides reflection-free access to column metadata and values, enabling
// generic operations on table variable rows without sacrificing type safety
// in the generated code.
type TableRow interface {
	// ColumnNames returns the names of all columns in the row.
	ColumnNames() []string

	// ColumnValues returns the values of all columns as a slice of any.
	// The order matches ColumnNames().
	ColumnValues() []any

	// ScanFrom populates the row from a database row.
	// Returns an error if the scan fails.
	ScanFrom(row *sql.Row) error

	// ScanFromRows populates the row from a database rows cursor.
	// Returns an error if the scan fails.
	ScanFromRows(rows *sql.Rows) error
}

// -----------------------------------------------------------------------------
// Generic Slice Helpers
// -----------------------------------------------------------------------------

// First returns the first element of a slice and true, or the zero value and
// false if the slice is empty. Equivalent to T-SQL TOP 1.
func First[T any](slice []T) (T, bool) {
	if len(slice) == 0 {
		var zero T
		return zero, false
	}
	return slice[0], true
}

// Last returns the last element of a slice and true, or the zero value and
// false if the slice is empty.
func Last[T any](slice []T) (T, bool) {
	if len(slice) == 0 {
		var zero T
		return zero, false
	}
	return slice[len(slice)-1], true
}

// FirstN returns up to n elements from the start of a slice.
// Equivalent to T-SQL TOP n.
func FirstN[T any](slice []T, n int) []T {
	if n <= 0 {
		return nil
	}
	if n >= len(slice) {
		return slice
	}
	return slice[:n]
}

// Filter returns a new slice containing only elements that satisfy the predicate.
// Equivalent to T-SQL WHERE clause on a table variable.
func Filter[T any](slice []T, predicate func(T) bool) []T {
	var result []T
	for _, item := range slice {
		if predicate(item) {
			result = append(result, item)
		}
	}
	return result
}

// Any returns true if any element in the slice satisfies the predicate.
// Equivalent to T-SQL EXISTS (SELECT 1 FROM @TableVar WHERE ...).
func Any[T any](slice []T, predicate func(T) bool) bool {
	for _, item := range slice {
		if predicate(item) {
			return true
		}
	}
	return false
}

// All returns true if all elements in the slice satisfy the predicate.
// Returns true for empty slices (vacuous truth).
func All[T any](slice []T, predicate func(T) bool) bool {
	for _, item := range slice {
		if !predicate(item) {
			return false
		}
	}
	return true
}

// None returns true if no elements in the slice satisfy the predicate.
// Equivalent to T-SQL NOT EXISTS.
func None[T any](slice []T, predicate func(T) bool) bool {
	return !Any(slice, predicate)
}

// Count returns the number of elements that satisfy the predicate.
// Equivalent to T-SQL SELECT COUNT(*) FROM @TableVar WHERE ...
func Count[T any](slice []T, predicate func(T) bool) int {
	count := 0
	for _, item := range slice {
		if predicate(item) {
			count++
		}
	}
	return count
}

// CountAll returns the total number of elements in the slice.
// Equivalent to T-SQL SELECT COUNT(*) FROM @TableVar.
func CountAll[T any](slice []T) int {
	return len(slice)
}

// Find returns the first element that satisfies the predicate and true,
// or the zero value and false if no element matches.
func Find[T any](slice []T, predicate func(T) bool) (T, bool) {
	for _, item := range slice {
		if predicate(item) {
			return item, true
		}
	}
	var zero T
	return zero, false
}

// FindIndex returns the index of the first element that satisfies the predicate,
// or -1 if no element matches.
func FindIndex[T any](slice []T, predicate func(T) bool) int {
	for i, item := range slice {
		if predicate(item) {
			return i
		}
	}
	return -1
}

// Map transforms each element of a slice using the provided function.
// Equivalent to T-SQL SELECT expression FROM @TableVar.
func Map[T any, R any](slice []T, transform func(T) R) []R {
	result := make([]R, len(slice))
	for i, item := range slice {
		result[i] = transform(item)
	}
	return result
}

// Reduce aggregates all elements of a slice into a single value.
// Equivalent to T-SQL aggregate functions over a table variable.
func Reduce[T any, R any](slice []T, initial R, accumulator func(R, T) R) R {
	result := initial
	for _, item := range slice {
		result = accumulator(result, item)
	}
	return result
}

// Distinct returns a new slice with duplicate elements removed.
// Elements are compared using reflect.DeepEqual.
// Equivalent to T-SQL SELECT DISTINCT.
func Distinct[T any](slice []T) []T {
	if len(slice) == 0 {
		return nil
	}
	seen := make(map[any]bool)
	var result []T
	for _, item := range slice {
		// Use reflect for comparison since T might not be comparable
		key := item
		if !seen[key] {
			// For non-comparable types, we need a different approach
			found := false
			for _, r := range result {
				if reflect.DeepEqual(item, r) {
					found = true
					break
				}
			}
			if !found {
				result = append(result, item)
			}
		}
	}
	return result
}

// GroupBy groups elements by a key extracted from each element.
// Returns a map from keys to slices of elements with that key.
// Equivalent to T-SQL GROUP BY.
func GroupBy[T any, K comparable](slice []T, keyFunc func(T) K) map[K][]T {
	result := make(map[K][]T)
	for _, item := range slice {
		key := keyFunc(item)
		result[key] = append(result[key], item)
	}
	return result
}

// -----------------------------------------------------------------------------
// Aggregate Helpers
// -----------------------------------------------------------------------------

// SumInt sums integer values extracted from slice elements.
func SumInt[T any](slice []T, getter func(T) int) int {
	var sum int
	for _, item := range slice {
		sum += getter(item)
	}
	return sum
}

// SumInt32 sums int32 values extracted from slice elements.
func SumInt32[T any](slice []T, getter func(T) int32) int32 {
	var sum int32
	for _, item := range slice {
		sum += getter(item)
	}
	return sum
}

// SumInt64 sums int64 values extracted from slice elements.
func SumInt64[T any](slice []T, getter func(T) int64) int64 {
	var sum int64
	for _, item := range slice {
		sum += getter(item)
	}
	return sum
}

// SumFloat64 sums float64 values extracted from slice elements.
func SumFloat64[T any](slice []T, getter func(T) float64) float64 {
	var sum float64
	for _, item := range slice {
		sum += getter(item)
	}
	return sum
}

// AvgFloat64 computes the average of float64 values extracted from slice elements.
// Returns 0 for empty slices.
func AvgFloat64[T any](slice []T, getter func(T) float64) float64 {
	if len(slice) == 0 {
		return 0
	}
	return SumFloat64(slice, getter) / float64(len(slice))
}

// MaxInt returns the maximum int value from the slice.
// Returns 0 and false for empty slices.
func MaxInt[T any](slice []T, getter func(T) int) (int, bool) {
	if len(slice) == 0 {
		return 0, false
	}
	max := getter(slice[0])
	for i := 1; i < len(slice); i++ {
		if v := getter(slice[i]); v > max {
			max = v
		}
	}
	return max, true
}

// MinInt returns the minimum int value from the slice.
// Returns 0 and false for empty slices.
func MinInt[T any](slice []T, getter func(T) int) (int, bool) {
	if len(slice) == 0 {
		return 0, false
	}
	min := getter(slice[0])
	for i := 1; i < len(slice); i++ {
		if v := getter(slice[i]); v < min {
			min = v
		}
	}
	return min, true
}

// MaxFloat64 returns the maximum float64 value from the slice.
// Returns 0 and false for empty slices.
func MaxFloat64[T any](slice []T, getter func(T) float64) (float64, bool) {
	if len(slice) == 0 {
		return 0, false
	}
	max := getter(slice[0])
	for i := 1; i < len(slice); i++ {
		if v := getter(slice[i]); v > max {
			max = v
		}
	}
	return max, true
}

// MinFloat64 returns the minimum float64 value from the slice.
// Returns 0 and false for empty slices.
func MinFloat64[T any](slice []T, getter func(T) float64) (float64, bool) {
	if len(slice) == 0 {
		return 0, false
	}
	min := getter(slice[0])
	for i := 1; i < len(slice); i++ {
		if v := getter(slice[i]); v < min {
			min = v
		}
	}
	return min, true
}

// -----------------------------------------------------------------------------
// Batch Loading Helpers
// -----------------------------------------------------------------------------

// LoadFromRows loads all rows from a sql.Rows cursor into a slice.
// The scanner function should create a new row and scan values into it.
func LoadFromRows[T any](rows *sql.Rows, scanner func(*sql.Rows) (T, error)) ([]T, error) {
	var result []T
	for rows.Next() {
		row, err := scanner(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// LoadFromRowsWithCapacity is like LoadFromRows but pre-allocates the slice
// with the given capacity hint for better performance.
func LoadFromRowsWithCapacity[T any](rows *sql.Rows, capacity int, scanner func(*sql.Rows) (T, error)) ([]T, error) {
	result := make([]T, 0, capacity)
	for rows.Next() {
		row, err := scanner(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
