# TDS Implementation Plan for aul

## Overview

This document outlines the plan to complete the TDS (Tabular Data Stream) protocol implementation in aul. The implementation mirrors go-mssqldb's client behaviour to ensure compatibility, rather than attempting full MS-TDS specification compliance.

## Current State (v0.2.2)

### Implemented

| Component | File | Status |
|-----------|------|--------|
| Packet framing | `tds/packet.go` | ✓ Complete |
| Packet header read/write | `tds/packet.go` | ✓ Complete |
| Connection wrapper | `tds/conn.go` | ✓ Complete |
| PRELOGIN parsing | `tds/prelogin.go` | ✓ Complete |
| PRELOGIN response | `tds/prelogin.go` | ✓ Complete |
| LOGIN7 parsing | `tds/login.go` | ✓ Complete |
| Password demangling | `tds/login.go` | ✓ Complete |
| Basic token types | `tds/token.go` | ✓ Partial |
| LOGINACK token | `tds/token.go` | ✓ Complete |
| ENVCHANGE token | `tds/token.go` | ✓ Complete |
| ERROR/INFO tokens | `tds/token.go` | ✓ Complete |
| DONE tokens | `tds/token.go` | ✓ Complete |
| Basic SQL types | `tds/types.go` | ✓ Partial |
| COLMETADATA token | `tds/types.go` | ✓ Complete |
| ROW token | `tds/types.go` | ✓ Partial |
| Protocol listener | `protocol/tds/listener.go` | ✓ Complete |
| Protocol connection | `protocol/tds/connection.go` | ✓ Basic |

### Not Yet Implemented

- TLS encryption
- RPC parameter parsing
- RETURNVALUE token
- NBCRow (null bitmap compressed row)
- MARS (Multiple Active Result Sets)
- Transaction management tokens
- Attention handling
- Bulk copy
- Table-valued parameters (TVP)
- All datetime types
- Decimal/numeric types
- XML type
- Geography/geometry types

---

## Implementation Phases

### Phase 1: Core Query Execution (Priority: Critical) ✓ COMPLETE

**Goal:** Enable go-mssqldb clients to execute basic queries and stored procedures.

#### 1.1 RPC Parameter Parsing ✓
**File:** `tds/rpc.go`

Parse RPC_REQUEST packets to extract:
- Procedure name or ID
- Parameter metadata (name, type, direction)
- Parameter values

```
Tasks:
✓ Create rpc.go with RPCRequest struct
✓ Implement parseRPCRequest() function
✓ Handle system procedure IDs (sp_executesql, sp_prepare, etc.)
✓ Parse parameter type info (TYPE_INFO structure)
✓ Parse parameter values for all basic types
✓ Handle output parameter markers
✓ Add unit tests with captured go-mssqldb packets
```

#### 1.2 Extended Type Support ✓
**File:** `tds/rpc.go`

All basic types supported for reading:

```
Tasks:
✓ DECIMAL/NUMERIC (TypeDecimalN, TypeNumericN)
✓ DATE (TypeDateN) 
✓ TIME (TypeTimeN)
✓ DATETIME2 (TypeDateTime2N)
✓ DATETIMEOFFSET (TypeDateTimeOffsetN)
✓ UNIQUEIDENTIFIER (TypeGUID)
✓ VARBINARY(MAX) / VARCHAR(MAX) / NVARCHAR(MAX)
✓ Type reading functions for all parameter types
✓ Unit tests for each type (13 tests)
```

#### 1.3 RETURNVALUE Token ✓
**File:** `tds/token.go`

Write RETURNVALUE tokens for output parameters:

```
Tasks:
✓ Implement WriteReturnValue() method
✓ Handle parameter name encoding
✓ Support all basic parameter types
✓ Integration with SendResult for output params
```

#### 1.4 sp_executesql Support ✓
**File:** `protocol/tds/connection.go`

Handle the most common RPC call pattern:

```
Tasks:
✓ Detect sp_executesql RPC calls
✓ Extract SQL text from @stmt parameter
✓ Extract parameter definitions from @params
✓ Map actual parameters to query
✓ Route to aul query execution (via protocol.Request)
✓ Integration test verifying end-to-end flow
```

---

### Phase 2: Production Readiness (Priority: High) ✓ COMPLETE

**Goal:** Handle real-world client behaviour and edge cases.

#### 2.1 TLS Encryption ✓ (Infrastructure)
**Files:** `tds/tls.go`, `protocol/tds/listener.go`, `protocol/tds/connection.go`

TLS infrastructure is in place:

```
Tasks:
✓ Create tlsHandshakeConn wrapper (mirrors go-mssqldb)
✓ TLS negotiation in PRELOGIN
✓ Certificate loading in listener config
✓ TLS config propagation to connections
✓ UpgradeToTLS() method
☐ Full TLS integration testing (complex due to TDS-wrapped handshake)
```

Note: TLS infrastructure is complete. The TDS TLS handshake wraps TLS 
messages in PRELOGIN packets, which requires careful coordination. 
Basic unencrypted connections (encrypt=disable) work perfectly with go-mssqldb.

#### 2.2 Attention Handling ✓
**File:** `protocol/tds/connection.go`, `protocol/protocol.go`

Handle query cancellation:

```
Tasks:
✓ Detect ATTENTION packets (type 0x06)
✓ Signal running query to cancel (via RequestCancel)
✓ Send DONE with ATTN flag (ResultCancel type)
✓ DoneAttn flag defined and used
```

#### 2.3 Connection Reset ✓
**File:** `protocol/tds/connection.go`, `tds/conn.go`

Handle connection reset requests:

```
Tasks:
✓ Detect StatusResetConnection in packet header
✓ ReadPacketWithStatus() to expose status byte
✓ resetSession() hook for state reset
✓ IsResetConnection() / IsResetConnectionSkipTran() helpers
```

#### 2.4 Error Handling Improvements ✓
**File:** `tds/errors.go` (new)

Better error reporting:

```
Tasks:
✓ Map aul errors to SQL Server error numbers
✓ Proper severity levels (SeverityInfo through SeveritySystem)
✓ State codes
✓ ErrorInfo struct with fluent builder
✓ Common error constructors (LoginFailed, Syntax, ProcNotFound, etc.)
```

---

### Phase 3: Advanced Features (Priority: Medium) ✓ COMPLETE

**Goal:** Support advanced SQL Server client features.

**Architecture document:** See `docs/PHASE3_ARCHITECTURE.md`

#### 3.1 Transaction Management ✓
**Files:** `tds/transaction.go`, `tds/txn_parser.go`, `protocol/tds/txn_handler.go`

```
Tasks:
✓ TransactionManager interface defined
✓ TransactionDescriptor type (8-byte opaque ID)
✓ IsolationLevel enum
✓ ENVCHANGE token writers (BeginTran, CommitTran, RollbackTran)
✓ NullTransactionManager stub
✓ Transaction SQL parsing (BEGIN/COMMIT/ROLLBACK/SAVE/SET ISOLATION)
✓ Transaction handler with nesting support
✓ Unit tests (27 parser tests)
```

#### 3.2 Prepared Statements ✓
**Files:** `tds/prepared.go`, `protocol/tds/phase3.go`

```
Tasks:
✓ PreparedStatementStore interface defined
✓ PreparedStatement struct with handle, SQL, metadata
✓ HandlePool for handle management
✓ PreparedStatementCache default implementation
✓ sp_prepare handler
✓ sp_execute handler
✓ sp_unprepare handler
✓ Unit tests
```

#### 3.3 NBCRow Support ✓
**File:** `tds/nbcrow.go`

```
Tasks:
✓ BuildNullBitmap() function
✓ IsNullInBitmap() helper
✓ ShouldUseNBCRow() heuristic
✓ WriteNBCRow() method on ResultSetWriter
✓ EnableNBCRow() configuration
✓ WriteRowAuto() automatic format selection
```

#### 3.4 Cursor Support ✓
**Files:** `tds/cursor.go`, `protocol/tds/phase3.go`

```
Tasks:
✓ CursorManager interface defined
✓ Cursor struct with handle, position, options
✓ CursorFetchType, CursorScrollOpt, CursorConcurrencyOpt enums
✓ CursorCache default implementation
✓ sp_cursoropen handler
✓ sp_cursorfetch handler
✓ sp_cursorclose handler
✓ sp_cursoroption handler
✓ Unit tests
```

#### 3.5 Integration Points ✓
**File:** `protocol/tds/phase3.go`

```
Tasks:
✓ Phase3Handlers grouping
✓ ConnectionPhase3State per-connection state
✓ Phase 3 RPC classifier
✓ handlePhase3Request dispatcher
✓ handleTransactionSQL dispatcher
✓ Default null stubs for graceful degradation
✓ Unit tests
```

---

### Phase 4: Enterprise Features (Priority: Low)

**Goal:** Full feature parity for enterprise scenarios.

#### 4.1 MARS (Multiple Active Result Sets)
**File:** `tds/mars.go` (new)

```
Tasks:
☐ Session multiplexing
☐ Request/response interleaving
☐ SMP (Session Multiplex Protocol) header handling
```

**Estimated effort:** 8-12 hours

#### 4.2 Bulk Copy
**File:** `tds/bulkcopy.go` (new)

```
Tasks:
☐ Parse BULK_LOAD packets
☐ BCP metadata parsing
☐ Row data streaming
☐ Commit batching
```

**Estimated effort:** 6-8 hours

#### 4.3 Table-Valued Parameters
**File:** `tds/tvp.go` (new)

```
Tasks:
☐ TVP type metadata
☐ Row streaming for TVP
☐ Integration with RPC parameters
```

**Estimated effort:** 4-6 hours

#### 4.4 Always Encrypted
**Complexity:** Very High - likely out of scope

---

## Testing Strategy

### Unit Tests

Each component should have unit tests using captured packet data:

```
tds/
  packet_test.go      - Packet framing tests
  prelogin_test.go    - PRELOGIN round-trip tests
  login_test.go       - LOGIN7 parsing tests
  rpc_test.go         - RPC parameter parsing tests
  types_test.go       - Type encoding/decoding tests
  token_test.go       - Token generation tests
```

### Integration Tests

Test against actual go-mssqldb client:

```
tests/
  integration/
    connect_test.go     - Connection establishment
    query_test.go       - Basic queries
    procedure_test.go   - Stored procedure calls
    params_test.go      - Parameter handling
    transaction_test.go - Transaction management
```

### Compatibility Tests

Capture and replay go-mssqldb sessions:

```
testdata/
  captures/
    login_success.bin
    simple_query.bin
    sp_executesql.bin
    output_params.bin
```

---

## File Structure (Proposed)

```
aul/
├── tds/                      # Low-level TDS protocol (decoupled)
│   ├── packet.go             # Packet framing
│   ├── conn.go               # Connection management
│   ├── prelogin.go           # PRELOGIN negotiation
│   ├── login.go              # LOGIN7 handling
│   ├── token.go              # Token stream writing
│   ├── types.go              # SQL type encoding
│   ├── rpc.go                # RPC request parsing [NEW]
│   ├── tls.go                # TLS handshake [NEW]
│   ├── transaction.go        # Transaction tokens [NEW]
│   ├── prepared.go           # Prepared statements [NEW]
│   ├── cursor.go             # Cursor support [NEW]
│   └── *_test.go             # Unit tests
│
├── protocol/
│   └── tds/                  # aul protocol integration
│       ├── init.go           # Package registration
│       ├── listener.go       # TDS listener
│       ├── connection.go     # Connection handling
│       ├── handler.go        # Request dispatch [NEW]
│       ├── executor.go       # Query execution bridge [NEW]
│       └── *_test.go         # Integration tests
```

---

## Dependencies

The TDS package (`aul/tds`) should remain decoupled with zero dependencies on aul internals:

**Allowed imports in `tds/`:**
- Standard library only

**Integration point (`protocol/tds/`):**
- Imports `tds/` for protocol handling
- Imports `aul/runtime` for query execution
- Imports `aul/procedure` for procedure registry

---

## Milestones

| Milestone | Components | Target |
|-----------|------------|--------|
| M1: Basic Queries | Phase 1 complete | Week 1-2 |
| M2: Production Ready | Phase 2 complete | Week 3-4 |
| M3: Full Featured | Phase 3 complete | Week 5-6 |
| M4: Enterprise | Phase 4 (selective) | Week 7+ |

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| TLS complexity | High | Mirror go-mssqldb exactly |
| Type edge cases | Medium | Extensive test captures |
| MARS complexity | High | Defer to Phase 4 |
| Undocumented behaviour | Medium | Test against real clients |

---

## Notes

1. **Mirror, don't spec:** The TDS specification is enormous and ambiguous. Our approach is to mirror go-mssqldb's client behaviour exactly, ensuring compatibility with the most widely-used Go TDS client.

2. **Packet captures:** Consider using Wireshark or a TDS proxy to capture real client/server exchanges for test data.

3. **Version targeting:** Focus on TDS 7.4 (SQL Server 2012+). Older versions can be added later if needed.

4. **Feature flags:** Consider making advanced features (MARS, cursors) configurable to reduce attack surface in production.
