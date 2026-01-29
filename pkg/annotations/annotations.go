// Package annotations provides parsing and management of aul annotations.
//
// aul annotations are SQL comments with a special prefix that configure
// aul-specific behaviour without breaking SQL Server compatibility:
//
//	-- @aul:isolated
//	-- @aul:journal-mode=WAL
//	-- @aul:cache-size=5000
//	CREATE TABLE AuditLog (...)
//
// Syntax:
//   - `-- @aul:<key>` — Boolean flag (presence means true)
//   - `-- @aul:<key>=<value>` — Key-value setting
//   - Contiguous `-- @aul:` lines apply to the immediately following statement
//   - A blank line breaks the association
package annotations

import (
	"strconv"
	"strings"
	"time"
)

const (
	// Prefix is the annotation prefix that identifies aul directives.
	Prefix = "-- @aul:"
)

// Annotation represents a single parsed annotation.
type Annotation struct {
	Key   string
	Value string // Empty for boolean flags
	Line  int    // 1-indexed line number
}

// AnnotationSet is a collection of annotations with helper methods.
type AnnotationSet map[string]string

// Has returns true if the key is present (for boolean flags).
func (a AnnotationSet) Has(key string) bool {
	_, ok := a[key]
	return ok
}

// Get returns the value for a key and whether it was found.
func (a AnnotationSet) Get(key string) (string, bool) {
	v, ok := a[key]
	return v, ok
}

// GetString returns the value for a key, or defaultVal if not found.
func (a AnnotationSet) GetString(key, defaultVal string) string {
	if v, ok := a[key]; ok {
		return v
	}
	return defaultVal
}

// GetInt returns the integer value for a key, or defaultVal if not found or invalid.
func (a AnnotationSet) GetInt(key string, defaultVal int) int {
	if v, ok := a[key]; ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

// GetBool returns true if the key is present.
// For boolean flags, presence alone indicates true.
// For explicit values, parses "true", "1", "yes" as true.
func (a AnnotationSet) GetBool(key string) bool {
	v, ok := a[key]
	if !ok {
		return false
	}
	if v == "" {
		return true // Boolean flag present
	}
	switch strings.ToLower(v) {
	case "true", "1", "yes", "on":
		return true
	default:
		return false
	}
}

// GetDuration parses a duration value like "5s", "100ms", "2m".
func (a AnnotationSet) GetDuration(key string, defaultVal time.Duration) time.Duration {
	if v, ok := a[key]; ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return defaultVal
}

// Clone returns a copy of the annotation set.
func (a AnnotationSet) Clone() AnnotationSet {
	clone := make(AnnotationSet, len(a))
	for k, v := range a {
		clone[k] = v
	}
	return clone
}

// Merge adds all annotations from other, overwriting existing keys.
func (a AnnotationSet) Merge(other AnnotationSet) {
	for k, v := range other {
		a[k] = v
	}
}

// StatementAnnotations associates annotations with a statement by line number.
type StatementAnnotations struct {
	Annotations AnnotationSet
	StartLine   int // First line of annotation block (1-indexed)
	EndLine     int // Last annotation line before statement
	StmtLine    int // Line where statement begins
}

// Parser extracts annotations from SQL source.
type Parser struct {
	// StopOnBlank controls whether blank lines break annotation blocks.
	// Default: true
	StopOnBlank bool
}

// NewParser creates a new annotation parser with default settings.
func NewParser() *Parser {
	return &Parser{
		StopOnBlank: true,
	}
}

// Extract parses the source and returns annotations for each statement.
// Annotations are grouped by the statement they precede.
func (p *Parser) Extract(source string) []StatementAnnotations {
	lines := strings.Split(source, "\n")
	var results []StatementAnnotations

	var currentAnnotations []Annotation
	var blockStartLine int

	for i, line := range lines {
		lineNum := i + 1 // 1-indexed
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, Prefix) {
			// Parse annotation
			ann := p.parseLine(trimmed, lineNum)
			if ann != nil {
				if len(currentAnnotations) == 0 {
					blockStartLine = lineNum
				}
				currentAnnotations = append(currentAnnotations, *ann)
			}
		} else if trimmed == "" && p.StopOnBlank {
			// Blank line breaks the annotation block
			currentAnnotations = nil
			blockStartLine = 0
		} else if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
			// Non-comment, non-blank line: this is a statement
			if len(currentAnnotations) > 0 {
				// Build annotation set
				set := make(AnnotationSet)
				endLine := 0
				for _, ann := range currentAnnotations {
					set[ann.Key] = ann.Value
					if ann.Line > endLine {
						endLine = ann.Line
					}
				}

				results = append(results, StatementAnnotations{
					Annotations: set,
					StartLine:   blockStartLine,
					EndLine:     endLine,
					StmtLine:    lineNum,
				})

				// Reset for next block
				currentAnnotations = nil
				blockStartLine = 0
			}
		}
		// Regular comments (not @aul:) are ignored but don't break the block
	}

	return results
}

// ExtractForLine finds annotations that apply to a specific line number.
func (p *Parser) ExtractForLine(source string, targetLine int) AnnotationSet {
	all := p.Extract(source)
	for _, sa := range all {
		if sa.StmtLine == targetLine {
			return sa.Annotations
		}
	}
	return make(AnnotationSet)
}

// parseLine parses a single annotation line.
func (p *Parser) parseLine(line string, lineNum int) *Annotation {
	// Remove prefix
	content := strings.TrimPrefix(line, Prefix)
	content = strings.TrimSpace(content)

	if content == "" {
		return nil
	}

	// Check for key=value
	if idx := strings.Index(content, "="); idx > 0 {
		key := strings.TrimSpace(content[:idx])
		value := strings.TrimSpace(content[idx+1:])
		return &Annotation{
			Key:   key,
			Value: value,
			Line:  lineNum,
		}
	}

	// Boolean flag (key only)
	return &Annotation{
		Key:   content,
		Value: "",
		Line:  lineNum,
	}
}

// ParseSingle parses annotations from a string containing only annotation lines.
// Useful for parsing annotations already extracted from context.
func ParseSingle(annotations string) AnnotationSet {
	parser := NewParser()
	parser.StopOnBlank = false

	// Add a dummy statement line so Extract finds something
	source := annotations + "\nSELECT 1"
	results := parser.Extract(source)

	if len(results) > 0 {
		return results[0].Annotations
	}
	return make(AnnotationSet)
}

// Known annotation keys for validation and documentation.
var (
	// Procedure annotations
	ProcAnnotations = map[string]string{
		"jit-threshold": "int: Override default JIT threshold",
		"no-jit":        "bool: Disable JIT for this procedure",
		"timeout":       "duration: Execution timeout override",
		"log-params":    "bool: Log parameter values",
		"deprecated":    "bool: Log warning when called",
	}

	// Table annotations
	TableAnnotations = map[string]string{
		"isolated":     "bool: Store in separate SQLite file",
		"journal-mode": "string: SQLite journal mode (WAL, DELETE, etc.)",
		"cache-size":   "int: SQLite cache size in pages",
		"synchronous":  "string: SQLite synchronous setting",
		"read-only":    "bool: Reject writes to this table",
	}
)

// ValidateProcAnnotations checks if all keys in the set are valid procedure annotations.
// Returns a list of unknown keys.
func ValidateProcAnnotations(set AnnotationSet) []string {
	var unknown []string
	for key := range set {
		if _, ok := ProcAnnotations[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	return unknown
}

// ValidateTableAnnotations checks if all keys in the set are valid table annotations.
// Returns a list of unknown keys.
func ValidateTableAnnotations(set AnnotationSet) []string {
	var unknown []string
	for key := range set {
		if _, ok := TableAnnotations[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	return unknown
}
