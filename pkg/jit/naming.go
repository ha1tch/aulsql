package jit

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var (
	// unsafeChars matches characters that are not valid in Go identifiers
	unsafeChars = regexp.MustCompile(`[^a-zA-Z0-9_]`)

	// leadingDigit matches strings that start with a digit
	leadingDigit = regexp.MustCompile(`^[0-9]`)
)

// SafeGoName creates a valid Go identifier from a procedure qualified name.
//
// SQL procedure names can contain characters that are invalid for Go identifiers:
//   - Dots: "salesdb.dbo.GetCustomer"
//   - Brackets: "[My Weird Proc]"
//   - Spaces: "Get Customer Data"
//   - Special chars: "calc$total"
//
// This function:
//  1. Replaces unsafe characters with underscores
//  2. Ensures the name doesn't start with a digit
//  3. Adds a short hash for uniqueness (prevents collisions from sanitisation)
//  4. Prefixes with "Proc_" for clarity
//
// Examples:
//
//	"GetCustomer"                -> "Proc_GetCustomer_a1b2c3d4"
//	"salesdb.dbo.GetCustomer"    -> "Proc_salesdb_dbo_GetCustomer_e5f6g7h8"
//	"[My Weird Proc!]"           -> "Proc__My_Weird_Proc__i9j0k1l2"
//	"123numeric"                 -> "Proc__123numeric_m3n4o5p6"
func SafeGoName(qualifiedName string) string {
	// Replace unsafe characters with underscores
	safe := unsafeChars.ReplaceAllString(qualifiedName, "_")

	// Collapse multiple underscores
	for strings.Contains(safe, "__") {
		safe = strings.ReplaceAll(safe, "__", "_")
	}

	// Trim leading/trailing underscores
	safe = strings.Trim(safe, "_")

	// Ensure doesn't start with digit (add underscore prefix)
	if leadingDigit.MatchString(safe) {
		safe = "_" + safe
	}

	// Handle empty result
	if safe == "" {
		safe = "unnamed"
	}

	// Add short hash for uniqueness
	hash := sha256.Sum256([]byte(qualifiedName))
	shortHash := hex.EncodeToString(hash[:4])

	return "Proc_" + safe + "_" + shortHash
}

// SafePackageName creates a valid Go package name from a procedure name.
// Package names have stricter requirements than identifiers (lowercase only).
func SafePackageName(qualifiedName string) string {
	// Lowercase the qualified name first
	lower := strings.ToLower(qualifiedName)

	// Replace unsafe characters with underscores
	safe := unsafeChars.ReplaceAllString(lower, "_")

	// Collapse multiple underscores
	for strings.Contains(safe, "__") {
		safe = strings.ReplaceAll(safe, "__", "_")
	}

	// Trim leading/trailing underscores
	safe = strings.Trim(safe, "_")

	// Ensure doesn't start with digit
	if leadingDigit.MatchString(safe) {
		safe = "p" + safe
	}

	// Handle empty result
	if safe == "" {
		safe = "proc"
	}

	// Add short hash for uniqueness
	hash := sha256.Sum256([]byte(qualifiedName))
	shortHash := hex.EncodeToString(hash[:4])

	return safe + "_" + shortHash
}

// WorkspaceDirName creates a filesystem-safe directory name for a procedure's
// compilation workspace. Includes source hash to allow multiple versions.
func WorkspaceDirName(qualifiedName string, sourceHash string) string {
	safeName := SafePackageName(qualifiedName)

	// Use first 8 chars of source hash
	hashPrefix := sourceHash
	if len(hashPrefix) > 8 {
		hashPrefix = hashPrefix[:8]
	}

	return safeName + "_" + hashPrefix
}
