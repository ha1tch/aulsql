// Package version provides version information for aul.
//
// The version is read from the VERSION file at the repository root.
// The Makefile copies VERSION to pkg/version/version.txt before building.
package version

import (
	_ "embed"
	"strings"
)

//go:embed version.txt
var versionFile string

// Version is the current version of aul.
// This is embedded from version.txt at compile time.
var Version = strings.TrimSpace(versionFile)

// String returns the version string.
func String() string {
	return Version
}

// Full returns a full version string with the package name.
func Full() string {
	return "aul version " + Version
}
