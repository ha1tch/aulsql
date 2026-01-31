package server

import (
	"context"
	"testing"
)

func TestTenantFromContext(t *testing.T) {
	ctx := context.Background()

	// No tenant set
	tenant, ok := TenantFromContext(ctx)
	if ok {
		t.Error("expected no tenant in empty context")
	}
	if tenant != "" {
		t.Errorf("expected empty tenant, got %q", tenant)
	}

	// With tenant set
	ctx = WithTenant(ctx, "acme")
	tenant, ok = TenantFromContext(ctx)
	if !ok {
		t.Error("expected tenant in context")
	}
	if tenant != "acme" {
		t.Errorf("expected tenant 'acme', got %q", tenant)
	}
}

func TestTenantIdentifier_FromHeader(t *testing.T) {
	config := TenantConfig{
		Enabled: true,
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{Type: "header", Name: "X-Tenant-ID"},
			},
		},
	}

	identifier := NewTenantIdentifier(config)

	sources := &MapTenantSources{
		Headers: map[string]string{
			"X-Tenant-ID": "acme-corp",
		},
	}

	tenant, err := identifier.Identify(sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant != "acme-corp" {
		t.Errorf("expected 'acme-corp', got %q", tenant)
	}
}

func TestTenantIdentifier_FromHeader_CaseInsensitive(t *testing.T) {
	config := TenantConfig{
		Enabled: true,
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{Type: "header", Name: "X-Tenant-ID"},
			},
		},
	}

	identifier := NewTenantIdentifier(config)

	sources := &MapTenantSources{
		Headers: map[string]string{
			"x-tenant-id": "lowercase-tenant",
		},
	}

	tenant, err := identifier.Identify(sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant != "lowercase-tenant" {
		t.Errorf("expected 'lowercase-tenant', got %q", tenant)
	}
}

func TestTenantIdentifier_FromTDSProperty(t *testing.T) {
	config := TenantConfig{
		Enabled: true,
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{Type: "tds_property", Name: "app_name"},
			},
		},
	}

	identifier := NewTenantIdentifier(config)

	sources := &MapTenantSources{
		TDSProperties: map[string]string{
			"app_name": "tenant-beta",
		},
	}

	tenant, err := identifier.Identify(sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant != "tenant-beta" {
		t.Errorf("expected 'tenant-beta', got %q", tenant)
	}
}

func TestTenantIdentifier_WithPattern(t *testing.T) {
	config := TenantConfig{
		Enabled: true,
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{
					Type:    "tds_property",
					Name:    "app_name",
					Pattern: `tenant:(\w+)`,
				},
			},
		},
	}

	identifier := NewTenantIdentifier(config)

	sources := &MapTenantSources{
		TDSProperties: map[string]string{
			"app_name": "myapp-tenant:acme-v2",
		},
	}

	tenant, err := identifier.Identify(sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant != "acme" {
		t.Errorf("expected 'acme', got %q", tenant)
	}
}

func TestTenantIdentifier_PatternNoMatch(t *testing.T) {
	config := TenantConfig{
		Enabled: true,
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{
					Type:    "header",
					Name:    "X-App-Name",
					Pattern: `tenant:(\w+)`,
				},
			},
			Default: "fallback",
		},
	}

	identifier := NewTenantIdentifier(config)

	sources := &MapTenantSources{
		Headers: map[string]string{
			"X-App-Name": "no-tenant-here",
		},
	}

	tenant, err := identifier.Identify(sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant != "fallback" {
		t.Errorf("expected 'fallback', got %q", tenant)
	}
}

func TestTenantIdentifier_MultipleSources(t *testing.T) {
	config := TenantConfig{
		Enabled: true,
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{Type: "header", Name: "X-Tenant-ID"},
				{Type: "tds_property", Name: "app_name"},
				{Type: "query_param", Name: "tenant"},
			},
		},
	}

	identifier := NewTenantIdentifier(config)

	// Test priority: header takes precedence
	sources := &MapTenantSources{
		Headers: map[string]string{
			"X-Tenant-ID": "from-header",
		},
		TDSProperties: map[string]string{
			"app_name": "from-tds",
		},
	}

	tenant, err := identifier.Identify(sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant != "from-header" {
		t.Errorf("expected 'from-header', got %q", tenant)
	}

	// Test fallback: no header, use TDS
	sources2 := &MapTenantSources{
		TDSProperties: map[string]string{
			"app_name": "from-tds",
		},
		QueryParams: map[string]string{
			"tenant": "from-query",
		},
	}

	tenant, err = identifier.Identify(sources2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant != "from-tds" {
		t.Errorf("expected 'from-tds', got %q", tenant)
	}
}

func TestTenantIdentifier_DefaultTenant(t *testing.T) {
	config := TenantConfig{
		Enabled: true,
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{Type: "header", Name: "X-Tenant-ID"},
			},
			Default: "default-tenant",
		},
	}

	identifier := NewTenantIdentifier(config)

	// No tenant in sources
	sources := &MapTenantSources{}

	tenant, err := identifier.Identify(sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant != "default-tenant" {
		t.Errorf("expected 'default-tenant', got %q", tenant)
	}
}

func TestTenantIdentifier_NoTenantNoDefault(t *testing.T) {
	config := TenantConfig{
		Enabled: true,
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{Type: "header", Name: "X-Tenant-ID"},
			},
			Default: "", // No default
		},
	}

	identifier := NewTenantIdentifier(config)

	sources := &MapTenantSources{}

	_, err := identifier.Identify(sources)
	if err == nil {
		t.Fatal("expected error when no tenant found and no default")
	}
}

func TestTenantIdentifier_Disabled(t *testing.T) {
	config := TenantConfig{
		Enabled: false, // Disabled
		Identification: TenantIdentificationConfig{
			Sources: []TenantSource{
				{Type: "header", Name: "X-Tenant-ID"},
			},
		},
	}

	identifier := NewTenantIdentifier(config)

	sources := &MapTenantSources{
		Headers: map[string]string{
			"X-Tenant-ID": "should-be-ignored",
		},
	}

	tenant, err := identifier.Identify(sources)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant != "" {
		t.Errorf("expected empty tenant when disabled, got %q", tenant)
	}
}

func TestValidateTenantID(t *testing.T) {
	longTenant := ""
	for i := 0; i < 65; i++ {
		longTenant += "a"
	}
	maxTenant := ""
	for i := 0; i < 64; i++ {
		maxTenant += "a"
	}

	tests := []struct {
		tenant  string
		wantErr bool
	}{
		{"acme", false},
		{"acme-corp", false},
		{"acme_corp", false},
		{"Acme123", false},
		{"tenant-1", false},
		{"", true},                    // empty
		{"tenant with spaces", true},  // spaces
		{"tenant/slash", true},        // slash
		{"tenant.dot", true},          // dot
		{longTenant, true},            // too long (65 chars)
		{maxTenant, false},            // max length ok (64 chars)
	}

	for _, tt := range tests {
		t.Run(tt.tenant, func(t *testing.T) {
			err := ValidateTenantID(tt.tenant)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for tenant %q", tt.tenant)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error for tenant %q: %v", tt.tenant, err)
			}
		})
	}
}
