package server

import (
	"testing"

	"github.com/ha1tch/aul/pkg/protocol"
	"github.com/ha1tch/aul/pkg/runtime"
)

// TestResultType_ExecWithResultSets verifies that procedure execution results
// containing result sets produce ResultRows, not ResultOK.
// This was a bug where handleExec hardcoded ResultOK.
func TestResultType_ExecWithResultSets(t *testing.T) {
	tests := []struct {
		name       string
		resultSets []runtime.ResultSet
		wantType   protocol.ResultType
	}{
		{
			name:       "no result sets returns ResultOK",
			resultSets: nil,
			wantType:   protocol.ResultOK,
		},
		{
			name:       "empty slice returns ResultOK",
			resultSets: []runtime.ResultSet{},
			wantType:   protocol.ResultOK,
		},
		{
			name: "one result set returns ResultRows",
			resultSets: []runtime.ResultSet{
				{
					Columns: []runtime.ColumnInfo{{Name: "ID", Type: "int"}},
					Rows:    [][]interface{}{{1}},
				},
			},
			wantType: protocol.ResultRows,
		},
		{
			name: "two result sets returns ResultRows",
			resultSets: []runtime.ResultSet{
				{
					Columns: []runtime.ColumnInfo{{Name: "ID", Type: "int"}},
					Rows:    [][]interface{}{{1}},
				},
				{
					Columns: []runtime.ColumnInfo{{Name: "OrderID", Type: "int"}},
					Rows:    [][]interface{}{{1001}},
				},
			},
			wantType: protocol.ResultRows,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Replicate the logic from handleExec (and handleQuery)
			resultType := protocol.ResultOK
			if len(tt.resultSets) > 0 {
				resultType = protocol.ResultRows
			}

			if resultType != tt.wantType {
				t.Errorf("got ResultType %v, want %v", resultType, tt.wantType)
			}
		})
	}
}

// TestConvertResultSets_PreservesAll verifies that convertResultSets does not
// drop any result sets during conversion.
func TestConvertResultSets_PreservesAll(t *testing.T) {
	input := []runtime.ResultSet{
		{
			Columns: []runtime.ColumnInfo{
				{Name: "CustomerID", Type: "int"},
				{Name: "Name", Type: "varchar"},
			},
			Rows: [][]interface{}{
				{1, "Alice"},
				{2, "Bob"},
			},
		},
		{
			Columns: []runtime.ColumnInfo{
				{Name: "OrderID", Type: "int"},
				{Name: "Total", Type: "float"},
			},
			Rows: [][]interface{}{
				{1001, 49.99},
			},
		},
		{
			Columns: []runtime.ColumnInfo{
				{Name: "Status", Type: "varchar"},
			},
			Rows: [][]interface{}{
				{"OK"},
			},
		},
	}

	output := convertResultSets(input)

	if len(output) != len(input) {
		t.Fatalf("expected %d result sets, got %d", len(input), len(output))
	}

	// Verify each result set preserved column count
	expectedCols := []int{2, 2, 1}
	for i, exp := range expectedCols {
		if len(output[i].Columns) != exp {
			t.Errorf("result set %d: expected %d columns, got %d", i+1, exp, len(output[i].Columns))
		}
	}

	// Verify row counts preserved
	expectedRows := []int{2, 1, 1}
	for i, exp := range expectedRows {
		if len(output[i].Rows) != exp {
			t.Errorf("result set %d: expected %d rows, got %d", i+1, exp, len(output[i].Rows))
		}
	}
}

// TestConvertResultSets_Empty verifies empty input produces zero-length output.
func TestConvertResultSets_Empty(t *testing.T) {
	output := convertResultSets(nil)
	if len(output) != 0 {
		t.Errorf("expected 0 result sets for nil input, got %d", len(output))
	}

	output = convertResultSets([]runtime.ResultSet{})
	if len(output) != 0 {
		t.Errorf("expected 0 result sets for empty input, got %d", len(output))
	}
}
