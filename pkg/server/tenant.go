// Package server provides the aul server implementation.
package server

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// tenantContextKey is the context key for tenant ID.
type tenantContextKey struct{}

// TenantFromContext extracts the tenant ID from context.
func TenantFromContext(ctx context.Context) (string, bool) {
	if tenant, ok := ctx.Value(tenantContextKey{}).(string); ok && tenant != "" {
		return tenant, true
	}
	return "", false
}

// WithTenant returns a context with the tenant ID set.
func WithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantContextKey{}, tenant)
}

// TenantConfig configures multi-tenancy behaviour.
type TenantConfig struct {
	// Enabled controls whether multi-tenancy is active.
	Enabled bool `yaml:"enabled" json:"enabled"`

	// Identification defines how to extract tenant ID from requests.
	Identification TenantIdentificationConfig `yaml:"identification" json:"identification"`

	// Storage configures tenant-specific storage.
	Storage TenantStorageConfig `yaml:"storage" json:"storage"`
}

// TenantIdentificationConfig configures tenant ID extraction.
type TenantIdentificationConfig struct {
	// Sources defines extraction methods in priority order.
	Sources []TenantSource `yaml:"sources" json:"sources"`

	// Default is used when no source matches (empty = reject request).
	Default string `yaml:"default" json:"default"`
}

// TenantSource defines a single tenant extraction method.
type TenantSource struct {
	// Type: "header", "tds_property", "connection_string", "query_param"
	Type string `yaml:"type" json:"type"`

	// Name of the header, property, or parameter.
	Name string `yaml:"name" json:"name"`

	// Pattern is an optional regex to extract tenant from value.
	// Must contain exactly one capture group.
	// If empty, the entire value is used as tenant ID.
	Pattern string `yaml:"pattern" json:"pattern"`

	// compiled regex (populated on first use)
	compiledPattern *regexp.Regexp
}

// TenantStorageConfig configures tenant-specific storage.
type TenantStorageConfig struct {
	// AutoCreate controls whether tenant databases are auto-created.
	AutoCreate bool `yaml:"auto_create" json:"auto_create"`

	// BaseDir is the directory for tenant database files.
	// Tenant databases are stored as {BaseDir}/{tenant}.db
	BaseDir string `yaml:"base_dir" json:"base_dir"`
}

// DefaultTenantConfig returns a TenantConfig with sensible defaults.
func DefaultTenantConfig() TenantConfig {
	return TenantConfig{
		Enabled: false,
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{Type: "header", Name: "X-Tenant-ID"},
			},
			Default: "",
		},
		Storage: TenantStorageConfig{
			AutoCreate: true,
			BaseDir:    "./data/tenants",
		},
	}
}

// TenantIdentifier extracts tenant IDs from various request sources.
type TenantIdentifier struct {
	config TenantConfig
}

// NewTenantIdentifier creates a new tenant identifier.
func NewTenantIdentifier(config TenantConfig) *TenantIdentifier {
	return &TenantIdentifier{config: config}
}

// Identify extracts tenant ID from the provided sources.
// Returns the tenant ID and nil error on success.
// Returns empty string and error if no tenant found and no default configured.
func (ti *TenantIdentifier) Identify(sources TenantSources) (string, error) {
	if !ti.config.Enabled {
		// Multi-tenancy disabled, return empty (single-tenant mode)
		return "", nil
	}

	for i := range ti.config.Identification.Sources {
		src := &ti.config.Identification.Sources[i]
		if tenant := ti.extractFromSource(src, sources); tenant != "" {
			return tenant, nil
		}
	}

	// No source matched, use default
	if ti.config.Identification.Default != "" {
		return ti.config.Identification.Default, nil
	}

	return "", fmt.Errorf("tenant identification failed: no tenant found and no default configured")
}

// extractFromSource attempts to extract tenant from a single source.
func (ti *TenantIdentifier) extractFromSource(src *TenantSource, sources TenantSources) string {
	var value string

	switch src.Type {
	case "header":
		value = sources.Header(src.Name)
	case "tds_property":
		value = sources.TDSProperty(src.Name)
	case "connection_string":
		value = sources.ConnectionString(src.Name)
	case "query_param":
		value = sources.QueryParam(src.Name)
	default:
		return ""
	}

	if value == "" {
		return ""
	}

	// Apply pattern if specified
	if src.Pattern != "" {
		return ti.applyPattern(src, value)
	}

	return value
}

// applyPattern extracts tenant using regex pattern.
func (ti *TenantIdentifier) applyPattern(src *TenantSource, value string) string {
	// Compile pattern on first use
	if src.compiledPattern == nil {
		re, err := regexp.Compile(src.Pattern)
		if err != nil {
			return ""
		}
		src.compiledPattern = re
	}

	matches := src.compiledPattern.FindStringSubmatch(value)
	if len(matches) < 2 {
		return ""
	}

	return matches[1]
}

// IsEnabled returns whether multi-tenancy is enabled.
func (ti *TenantIdentifier) IsEnabled() bool {
	return ti.config.Enabled
}

// TenantSources provides access to various request sources for tenant extraction.
type TenantSources interface {
	// Header returns an HTTP header value.
	Header(name string) string

	// TDSProperty returns a TDS connection property.
	TDSProperty(name string) string

	// ConnectionString returns a connection string parameter.
	ConnectionString(name string) string

	// QueryParam returns a URL query parameter.
	QueryParam(name string) string
}

// MapTenantSources is a simple map-based implementation of TenantSources.
type MapTenantSources struct {
	Headers          map[string]string
	TDSProperties    map[string]string
	ConnStringParams map[string]string
	QueryParams      map[string]string
}

func (m *MapTenantSources) Header(name string) string {
	if m.Headers == nil {
		return ""
	}
	// Case-insensitive header lookup
	for k, v := range m.Headers {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}

func (m *MapTenantSources) TDSProperty(name string) string {
	if m.TDSProperties == nil {
		return ""
	}
	// Case-insensitive property lookup
	for k, v := range m.TDSProperties {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}

func (m *MapTenantSources) ConnectionString(name string) string {
	if m.ConnStringParams == nil {
		return ""
	}
	for k, v := range m.ConnStringParams {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}

func (m *MapTenantSources) QueryParam(name string) string {
	if m.QueryParams == nil {
		return ""
	}
	return m.QueryParams[name]
}

// ValidateTenantID checks if a tenant ID is valid.
// Tenant IDs must be non-empty, alphanumeric with underscores/hyphens, max 64 chars.
func ValidateTenantID(tenant string) error {
	if tenant == "" {
		return fmt.Errorf("tenant ID cannot be empty")
	}
	if len(tenant) > 64 {
		return fmt.Errorf("tenant ID too long (max 64 characters)")
	}

	// Allow alphanumeric, underscore, hyphen
	for _, r := range tenant {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-') {
			return fmt.Errorf("tenant ID contains invalid character: %c", r)
		}
	}

	return nil
}
