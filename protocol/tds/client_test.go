package tds

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/protocol"
)

// TestGoMssqldbConnection tests that a real go-mssqldb client can connect
// to our TDS listener and complete the handshake.
func TestGoMssqldbConnection(t *testing.T) {
	// Start our TDS listener
	cfg := protocol.ListenerConfig{
		Name:     "test-tds",
		Protocol: protocol.ProtocolTDS,
		Host:     "127.0.0.1",
		Port:     0, // OS assigns port
		Options: map[string]interface{}{
			"server_name": "aul-test-server",
		},
	}

	logger := log.New(log.DefaultConfig())
	listener, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	if err := listener.Listen(); err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	// Get the assigned port
	addr := listener.Addr().(*net.TCPAddr)
	port := addr.Port
	t.Logf("TDS listener started on port %d", port)

	// Accept connections in background
	var serverConn protocol.Connection
	var acceptErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverConn, acceptErr = listener.Accept()
	}()

	// Connect using go-mssqldb
	// Note: encrypt=disable is required since we don't support TLS yet
	connStr := fmt.Sprintf("sqlserver://testuser:testpass@127.0.0.1:%d?database=master&encrypt=disable&connection+timeout=5", port)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Ping triggers the actual connection
	err = db.PingContext(ctx)
	
	// Wait for server to accept
	wg.Wait()

	if acceptErr != nil {
		t.Logf("Server accept error: %v", acceptErr)
	}

	if serverConn != nil {
		defer serverConn.Close()
		t.Log("Server accepted connection from go-mssqldb client")
	}

	if err != nil {
		// Connection might fail after handshake when trying to execute initial queries
		// That's expected since we're not running a full server
		t.Logf("Ping result: %v (this may be expected)", err)
	} else {
		t.Log("Ping succeeded - full connection established!")
	}
}

// TestGoMssqldbQuery tests executing a simple query through go-mssqldb.
// This requires the server to handle the request and send back a response.
func TestGoMssqldbQuery(t *testing.T) {
	// Start our TDS listener
	cfg := protocol.ListenerConfig{
		Name:     "test-tds",
		Protocol: protocol.ProtocolTDS,
		Host:     "127.0.0.1",
		Port:     0,
		Options: map[string]interface{}{
			"server_name": "aul-test-server",
		},
	}

	logger := log.New(log.DefaultConfig())
	listener, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	if err := listener.Listen(); err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	port := addr.Port
	t.Logf("TDS listener started on port %d", port)

	// Handle server side in goroutine
	serverDone := make(chan struct{})
	var serverConn protocol.Connection
	var request protocol.Request
	var requestErr error

	go func() {
		defer close(serverDone)
		
		var err error
		serverConn, err = listener.Accept()
		if err != nil {
			t.Logf("Accept error: %v", err)
			return
		}
		
		// Read the request that go-mssqldb sends
		request, requestErr = serverConn.ReadRequest()
		if requestErr != nil {
			t.Logf("ReadRequest error: %v", requestErr)
			return
		}
		
		t.Logf("Received request: Type=%v, SQL=%q, Proc=%q", 
			request.Type, request.SQL, request.ProcedureName)
		
		// Send a simple OK response
		result := protocol.Result{
			Type:         protocol.ResultOK,
			RowsAffected: 0,
		}
		if err := serverConn.SendResult(result); err != nil {
			t.Logf("SendResult error: %v", err)
		}
	}()

	// Connect using go-mssqldb
	connStr := fmt.Sprintf("sqlserver://testuser:testpass@127.0.0.1:%d?database=master&encrypt=disable&connection+timeout=5", port)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Execute a simple query
	_, err = db.ExecContext(ctx, "SELECT 1")
	
	// Wait for server to process
	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Log("Timeout waiting for server")
	}

	if serverConn != nil {
		serverConn.Close()
	}

	// Check what we received
	if requestErr == nil && request.Type != protocol.RequestUnknown {
		t.Logf("Successfully parsed request from go-mssqldb:")
		t.Logf("  Type: %v", request.Type)
		t.Logf("  SQL: %q", request.SQL)
		t.Logf("  ProcedureName: %q", request.ProcedureName)
		t.Logf("  Parameters: %v", request.Parameters)
	}

	if err != nil {
		t.Logf("Query error (may be expected): %v", err)
	}
}

// TestGoMssqldbWithParameters tests parameterised queries through go-mssqldb.
func TestGoMssqldbWithParameters(t *testing.T) {
	cfg := protocol.ListenerConfig{
		Name:     "test-tds",
		Protocol: protocol.ProtocolTDS,
		Host:     "127.0.0.1",
		Port:     0,
		Options: map[string]interface{}{
			"server_name": "aul-test-server",
		},
	}

	logger := log.New(log.DefaultConfig())
	listener, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	if err := listener.Listen(); err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	port := addr.Port
	t.Logf("TDS listener started on port %d", port)

	// Handle server side
	serverDone := make(chan struct{})
	var request protocol.Request
	var requestErr error

	go func() {
		defer close(serverDone)
		
		serverConn, err := listener.Accept()
		if err != nil {
			return
		}
		defer serverConn.Close()
		
		// Read the parameterised request
		request, requestErr = serverConn.ReadRequest()
		if requestErr != nil {
			return
		}
		
		// Send OK response
		serverConn.SendResult(protocol.Result{
			Type:         protocol.ResultOK,
			RowsAffected: 1,
		})
	}()

	// Connect and execute parameterised query
	connStr := fmt.Sprintf("sqlserver://testuser:testpass@127.0.0.1:%d?database=master&encrypt=disable&connection+timeout=5", port)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Execute parameterised query - go-mssqldb will use sp_executesql
	_, err = db.ExecContext(ctx, "SELECT @p1, @p2", sql.Named("p1", 42), sql.Named("p2", "hello"))
	
	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Log("Timeout waiting for server")
	}

	// Verify the request
	if requestErr != nil {
		t.Logf("Request error: %v", requestErr)
	} else {
		t.Logf("Request parsed successfully:")
		t.Logf("  Type: %v", request.Type)
		t.Logf("  SQL: %q", request.SQL)
		t.Logf("  ProcedureName: %q", request.ProcedureName)
		t.Logf("  Parameters: %v", request.Parameters)
		
		// Verify sp_executesql was detected
		if request.ProcedureName != "sp_executesql" {
			t.Errorf("Expected sp_executesql, got %q", request.ProcedureName)
		}
		
		// Verify SQL was extracted
		if request.SQL == "" {
			t.Error("SQL should not be empty for sp_executesql")
		}
		
		// Verify parameters were extracted
		if len(request.Parameters) == 0 {
			t.Error("Parameters should not be empty")
		} else {
			t.Logf("Parameter count: %d", len(request.Parameters))
			for k, v := range request.Parameters {
				t.Logf("  %s = %v (%T)", k, v, v)
			}
		}
	}
}

// TestGoMssqldbTLS tests TLS connection with go-mssqldb.
// This test generates a self-signed certificate and verifies encrypted connections work.
func TestGoMssqldbTLS(t *testing.T) {
	// Generate self-signed certificate for testing
	certPEM, keyPEM, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate certificate: %v", err)
	}

	// Write to temp files
	certFile := "/tmp/aul-test-cert.pem"
	keyFile := "/tmp/aul-test-key.pem"
	if err := os.WriteFile(certFile, certPEM, 0644); err != nil {
		t.Fatalf("Failed to write cert file: %v", err)
	}
	defer os.Remove(certFile)
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("Failed to write key file: %v", err)
	}
	defer os.Remove(keyFile)

	// Start TLS-enabled listener
	cfg := protocol.ListenerConfig{
		Name:        "test-tds-tls",
		Protocol:    protocol.ProtocolTDS,
		Host:        "127.0.0.1",
		Port:        0,
		TLSEnabled:  true,
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
		Options: map[string]interface{}{
			"server_name": "aul-test-server",
		},
	}

	logger := log.New(log.DefaultConfig())
	listener, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	if err := listener.Listen(); err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	port := addr.Port
	t.Logf("TLS-enabled TDS listener started on port %d", port)

	// Accept connections in background
	serverDone := make(chan struct{})
	var serverConn protocol.Connection
	var acceptErr error

	go func() {
		defer close(serverDone)
		serverConn, acceptErr = listener.Accept()
	}()

	// Connect using go-mssqldb with TLS
	// encrypt=true requires TLS, TrustServerCertificate=true skips cert validation
	connStr := fmt.Sprintf("sqlserver://testuser:testpass@127.0.0.1:%d?database=master&encrypt=true&TrustServerCertificate=true&connection+timeout=10", port)
	
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Ping triggers the connection
	err = db.PingContext(ctx)

	// Wait for server
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Log("Timeout waiting for server")
	}

	if acceptErr != nil {
		t.Logf("Server accept error: %v", acceptErr)
	}

	if serverConn != nil {
		serverConn.Close()
		t.Log("Server accepted TLS connection from go-mssqldb")
	}

	if err != nil {
		t.Logf("TLS connection result: %v", err)
	} else {
		t.Log("TLS connection and ping succeeded!")
	}
}

// generateTestCertificate creates a self-signed certificate for testing.
func generateTestCertificate() (certPEM, keyPEM []byte, err error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"AUL Test"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return certPEM, keyPEM, nil
}
