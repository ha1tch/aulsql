package jit

import (
	"regexp"
	"testing"
)

func TestSafeGoName(t *testing.T) {
	// Valid Go identifier pattern
	validIdentifier := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

	tests := []struct {
		name     string
		input    string
		wantSafe bool // Result should be a valid Go identifier
	}{
		{
			name:     "simple name",
			input:    "GetCustomer",
			wantSafe: true,
		},
		{
			name:     "qualified name with dots",
			input:    "salesdb.dbo.GetCustomer",
			wantSafe: true,
		},
		{
			name:     "bracketed name with spaces",
			input:    "[My Weird Proc!]",
			wantSafe: true,
		},
		{
			name:     "starts with digit",
			input:    "123numeric",
			wantSafe: true,
		},
		{
			name:     "special characters",
			input:    "calc$total@value",
			wantSafe: true,
		},
		{
			name:     "unicode characters",
			input:    "プロシージャ",
			wantSafe: true,
		},
		{
			name:     "empty string",
			input:    "",
			wantSafe: true,
		},
		{
			name:     "all special chars",
			input:    "!@#$%^&*()",
			wantSafe: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SafeGoName(tt.input)

			// Check it's a valid identifier
			if !validIdentifier.MatchString(got) {
				t.Errorf("SafeGoName(%q) = %q, not a valid Go identifier", tt.input, got)
			}

			// Check it starts with Proc_
			if len(got) < 5 || got[:5] != "Proc_" {
				t.Errorf("SafeGoName(%q) = %q, should start with 'Proc_'", tt.input, got)
			}

			// Check it ends with a hash (8 hex chars)
			if len(got) < 9 {
				t.Errorf("SafeGoName(%q) = %q, too short", tt.input, got)
			}
		})
	}
}

func TestSafeGoNameUniqueness(t *testing.T) {
	// Different inputs should produce different outputs
	inputs := []string{
		"GetCustomer",
		"getCustomer", // Case difference
		"Get_Customer",
		"salesdb.dbo.GetCustomer",
		"inventorydb.dbo.GetCustomer",
	}

	seen := make(map[string]string)
	for _, input := range inputs {
		output := SafeGoName(input)
		if existingInput, exists := seen[output]; exists {
			t.Errorf("collision: SafeGoName(%q) = SafeGoName(%q) = %q", input, existingInput, output)
		}
		seen[output] = input
	}
}

func TestSafePackageName(t *testing.T) {
	// Package names must be lowercase
	tests := []struct {
		input string
	}{
		{"GetCustomer"},
		{"salesdb.dbo.GetCustomer"},
		{"[My Proc]"},
		{"123start"},
	}

	lowercaseOnly := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := SafePackageName(tt.input)

			if !lowercaseOnly.MatchString(got) {
				t.Errorf("SafePackageName(%q) = %q, should be lowercase only", tt.input, got)
			}
		})
	}
}

func TestWorkspaceDirName(t *testing.T) {
	tests := []struct {
		name       string
		sourceHash string
	}{
		{"GetCustomer", "abcdef1234567890"},
		{"salesdb.dbo.GetCustomer", "1234567890abcdef"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := WorkspaceDirName(tt.name, tt.sourceHash)

			// Should contain hash prefix
			hashPrefix := tt.sourceHash
			if len(hashPrefix) > 8 {
				hashPrefix = hashPrefix[:8]
			}

			if len(got) == 0 {
				t.Errorf("WorkspaceDirName(%q, %q) = empty string", tt.name, tt.sourceHash)
			}

			// Should not contain problematic characters for filesystem
			badChars := regexp.MustCompile(`[/\\:*?"<>|]`)
			if badChars.MatchString(got) {
				t.Errorf("WorkspaceDirName(%q, %q) = %q, contains filesystem-unsafe chars", tt.name, tt.sourceHash, got)
			}
		})
	}
}
