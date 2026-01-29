package annotations

import (
	"testing"
	"time"
)

func TestParser_SingleAnnotation(t *testing.T) {
	source := `-- @aul:isolated
CREATE TABLE AuditLog (ID INT)`

	parser := NewParser()
	results := parser.Extract(source)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	sa := results[0]
	if !sa.Annotations.Has("isolated") {
		t.Error("expected 'isolated' annotation")
	}
	if sa.StartLine != 1 {
		t.Errorf("expected StartLine=1, got %d", sa.StartLine)
	}
	if sa.StmtLine != 2 {
		t.Errorf("expected StmtLine=2, got %d", sa.StmtLine)
	}
}

func TestParser_MultipleAnnotations(t *testing.T) {
	source := `-- @aul:isolated
-- @aul:journal-mode=WAL
-- @aul:cache-size=5000
CREATE TABLE AuditLog (ID INT)`

	parser := NewParser()
	results := parser.Extract(source)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	ann := results[0].Annotations
	if !ann.Has("isolated") {
		t.Error("expected 'isolated' annotation")
	}
	if v, _ := ann.Get("journal-mode"); v != "WAL" {
		t.Errorf("expected journal-mode=WAL, got %q", v)
	}
	if ann.GetInt("cache-size", 0) != 5000 {
		t.Errorf("expected cache-size=5000, got %d", ann.GetInt("cache-size", 0))
	}
}

func TestParser_KeyValueParsing(t *testing.T) {
	tests := []struct {
		line     string
		wantKey  string
		wantVal  string
	}{
		{"-- @aul:timeout=5s", "timeout", "5s"},
		{"-- @aul:cache-size=1000", "cache-size", "1000"},
		{"-- @aul:journal-mode=WAL", "journal-mode", "WAL"},
		{"-- @aul:isolated", "isolated", ""},
		{"-- @aul:no-jit", "no-jit", ""},
		{"-- @aul:key=value with spaces", "key", "value with spaces"},
		{"-- @aul:key = value", "key", "value"},
	}

	parser := NewParser()
	for _, tt := range tests {
		source := tt.line + "\nSELECT 1"
		results := parser.Extract(source)

		if len(results) != 1 {
			t.Errorf("%q: expected 1 result, got %d", tt.line, len(results))
			continue
		}

		ann := results[0].Annotations
		val, ok := ann.Get(tt.wantKey)
		if !ok {
			t.Errorf("%q: key %q not found", tt.line, tt.wantKey)
			continue
		}
		if val != tt.wantVal {
			t.Errorf("%q: expected value %q, got %q", tt.line, tt.wantVal, val)
		}
	}
}

func TestParser_BooleanFlags(t *testing.T) {
	source := `-- @aul:isolated
-- @aul:no-jit
-- @aul:deprecated
CREATE PROCEDURE Test AS SELECT 1`

	parser := NewParser()
	results := parser.Extract(source)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	ann := results[0].Annotations
	if !ann.GetBool("isolated") {
		t.Error("expected isolated=true")
	}
	if !ann.GetBool("no-jit") {
		t.Error("expected no-jit=true")
	}
	if !ann.GetBool("deprecated") {
		t.Error("expected deprecated=true")
	}
	if ann.GetBool("nonexistent") {
		t.Error("expected nonexistent=false")
	}
}

func TestParser_BlankLineBreaksBlock(t *testing.T) {
	source := `-- @aul:isolated

CREATE TABLE AuditLog (ID INT)`

	parser := NewParser()
	results := parser.Extract(source)

	// Blank line should break the association
	if len(results) != 0 {
		t.Errorf("expected 0 results (blank line breaks), got %d", len(results))
	}
}

func TestParser_NonAulCommentsPreserved(t *testing.T) {
	source := `-- @aul:isolated
-- This is a regular comment
-- @aul:cache-size=1000
CREATE TABLE Test (ID INT)`

	parser := NewParser()
	results := parser.Extract(source)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	ann := results[0].Annotations
	// Should have both annotations - regular comments don't break the block
	if !ann.Has("isolated") {
		t.Error("expected 'isolated' annotation")
	}
	if !ann.Has("cache-size") {
		t.Error("expected 'cache-size' annotation")
	}
}

func TestParser_MultipleStatements(t *testing.T) {
	source := `-- @aul:isolated
CREATE TABLE AuditLog (ID INT)

-- @aul:jit-threshold=50
-- @aul:timeout=5s
CREATE PROCEDURE usp_Test AS SELECT 1

CREATE TABLE NoAnnotations (ID INT)`

	parser := NewParser()
	results := parser.Extract(source)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// First statement
	if !results[0].Annotations.Has("isolated") {
		t.Error("first statement should have 'isolated'")
	}

	// Second statement
	if !results[1].Annotations.Has("jit-threshold") {
		t.Error("second statement should have 'jit-threshold'")
	}
	if !results[1].Annotations.Has("timeout") {
		t.Error("second statement should have 'timeout'")
	}

	// Third statement has no annotations (blank line before it)
}

func TestAnnotationSet_GetInt(t *testing.T) {
	ann := AnnotationSet{
		"valid":   "100",
		"invalid": "abc",
		"empty":   "",
	}

	if v := ann.GetInt("valid", 0); v != 100 {
		t.Errorf("expected 100, got %d", v)
	}
	if v := ann.GetInt("invalid", 42); v != 42 {
		t.Errorf("expected default 42 for invalid, got %d", v)
	}
	if v := ann.GetInt("missing", 99); v != 99 {
		t.Errorf("expected default 99 for missing, got %d", v)
	}
	if v := ann.GetInt("empty", 55); v != 55 {
		t.Errorf("expected default 55 for empty, got %d", v)
	}
}

func TestAnnotationSet_GetDuration(t *testing.T) {
	ann := AnnotationSet{
		"timeout":    "5s",
		"interval":   "100ms",
		"long":       "2m30s",
		"invalid":    "not-a-duration",
	}

	if v := ann.GetDuration("timeout", 0); v != 5*time.Second {
		t.Errorf("expected 5s, got %v", v)
	}
	if v := ann.GetDuration("interval", 0); v != 100*time.Millisecond {
		t.Errorf("expected 100ms, got %v", v)
	}
	if v := ann.GetDuration("long", 0); v != 2*time.Minute+30*time.Second {
		t.Errorf("expected 2m30s, got %v", v)
	}
	if v := ann.GetDuration("invalid", 10*time.Second); v != 10*time.Second {
		t.Errorf("expected default 10s for invalid, got %v", v)
	}
	if v := ann.GetDuration("missing", 1*time.Hour); v != 1*time.Hour {
		t.Errorf("expected default 1h for missing, got %v", v)
	}
}

func TestAnnotationSet_GetBool_ExplicitValues(t *testing.T) {
	ann := AnnotationSet{
		"true_val":  "true",
		"false_val": "false",
		"one_val":   "1",
		"zero_val":  "0",
		"yes_val":   "yes",
		"no_val":    "no",
		"on_val":    "on",
		"flag":      "",  // Boolean flag with no value
	}

	if !ann.GetBool("true_val") {
		t.Error("expected true for 'true'")
	}
	if ann.GetBool("false_val") {
		t.Error("expected false for 'false'")
	}
	if !ann.GetBool("one_val") {
		t.Error("expected true for '1'")
	}
	if ann.GetBool("zero_val") {
		t.Error("expected false for '0'")
	}
	if !ann.GetBool("yes_val") {
		t.Error("expected true for 'yes'")
	}
	if ann.GetBool("no_val") {
		t.Error("expected false for 'no'")
	}
	if !ann.GetBool("on_val") {
		t.Error("expected true for 'on'")
	}
	if !ann.GetBool("flag") {
		t.Error("expected true for empty value (boolean flag)")
	}
}

func TestAnnotationSet_Clone(t *testing.T) {
	original := AnnotationSet{
		"key1": "value1",
		"key2": "value2",
	}

	clone := original.Clone()

	// Modify clone
	clone["key1"] = "modified"
	clone["key3"] = "new"

	// Original should be unchanged
	if v, _ := original.Get("key1"); v != "value1" {
		t.Errorf("original modified: key1=%q", v)
	}
	if original.Has("key3") {
		t.Error("original has key3 from clone")
	}
}

func TestAnnotationSet_Merge(t *testing.T) {
	base := AnnotationSet{
		"key1": "base1",
		"key2": "base2",
	}

	other := AnnotationSet{
		"key2": "other2", // Override
		"key3": "other3", // New
	}

	base.Merge(other)

	if v, _ := base.Get("key1"); v != "base1" {
		t.Errorf("key1 should be unchanged: %q", v)
	}
	if v, _ := base.Get("key2"); v != "other2" {
		t.Errorf("key2 should be overwritten: %q", v)
	}
	if v, _ := base.Get("key3"); v != "other3" {
		t.Errorf("key3 should be added: %q", v)
	}
}

func TestExtractForLine(t *testing.T) {
	source := `-- @aul:isolated
CREATE TABLE First (ID INT)

-- @aul:timeout=5s
CREATE PROCEDURE Second AS SELECT 1`

	parser := NewParser()

	// Line 2 should have 'isolated'
	ann := parser.ExtractForLine(source, 2)
	if !ann.Has("isolated") {
		t.Error("line 2 should have 'isolated'")
	}

	// Line 5 should have 'timeout'
	ann = parser.ExtractForLine(source, 5)
	if !ann.Has("timeout") {
		t.Error("line 5 should have 'timeout'")
	}

	// Line 10 should be empty
	ann = parser.ExtractForLine(source, 10)
	if len(ann) != 0 {
		t.Errorf("line 10 should have no annotations, got %d", len(ann))
	}
}

func TestParseSingle(t *testing.T) {
	annotations := `-- @aul:isolated
-- @aul:cache-size=2000`

	ann := ParseSingle(annotations)

	if !ann.Has("isolated") {
		t.Error("expected 'isolated'")
	}
	if ann.GetInt("cache-size", 0) != 2000 {
		t.Errorf("expected cache-size=2000, got %d", ann.GetInt("cache-size", 0))
	}
}

func TestValidateProcAnnotations(t *testing.T) {
	ann := AnnotationSet{
		"jit-threshold": "50",
		"no-jit":        "",
		"unknown-key":   "value",
	}

	unknown := ValidateProcAnnotations(ann)

	if len(unknown) != 1 {
		t.Fatalf("expected 1 unknown key, got %d", len(unknown))
	}
	if unknown[0] != "unknown-key" {
		t.Errorf("expected 'unknown-key', got %q", unknown[0])
	}
}

func TestValidateTableAnnotations(t *testing.T) {
	ann := AnnotationSet{
		"isolated":     "",
		"journal-mode": "WAL",
		"invalid":      "value",
	}

	unknown := ValidateTableAnnotations(ann)

	if len(unknown) != 1 {
		t.Fatalf("expected 1 unknown key, got %d", len(unknown))
	}
	if unknown[0] != "invalid" {
		t.Errorf("expected 'invalid', got %q", unknown[0])
	}
}

func TestParser_RealWorldProcedure(t *testing.T) {
	source := `-- @aul:jit-threshold=50
-- @aul:timeout=5s
-- @aul:log-params
CREATE PROCEDURE usp_QuickLookup
    @Key VARCHAR(100)
AS
BEGIN
    SELECT Value FROM LookupCache WHERE Key = @Key
END`

	parser := NewParser()
	results := parser.Extract(source)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	ann := results[0].Annotations
	if ann.GetInt("jit-threshold", 0) != 50 {
		t.Error("expected jit-threshold=50")
	}
	if ann.GetDuration("timeout", 0) != 5*time.Second {
		t.Error("expected timeout=5s")
	}
	if !ann.GetBool("log-params") {
		t.Error("expected log-params=true")
	}
}

func TestParser_EmptySource(t *testing.T) {
	parser := NewParser()
	
	results := parser.Extract("")
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty source, got %d", len(results))
	}

	results = parser.Extract("   \n\n   ")
	if len(results) != 0 {
		t.Errorf("expected 0 results for whitespace-only source, got %d", len(results))
	}
}

func TestParser_AnnotationWithoutStatement(t *testing.T) {
	source := `-- @aul:isolated
-- @aul:cache-size=1000
-- Just annotations, no statement`

	parser := NewParser()
	results := parser.Extract(source)

	// No statement follows, so no association
	if len(results) != 0 {
		t.Errorf("expected 0 results (no statement), got %d", len(results))
	}
}
