# Dual-Socket Binary Protocol (DSBP)

**Version:** 0.6.4  
**Status:** Design specification  
**Last updated:** February 2026

---

## Overview

DSBP defines the communication protocol between the aul server and out-of-process worker binaries produced by `tgpiler --recursive`. It extends the execution model described in [001-STORED_PROCEDURE_ARCHITECTURE](001-STORED_PROCEDURE_ARCHITECTURE.md) with a fourth execution path: compiled binaries running in a separate process, communicating over Unix Domain Sockets.

### Execution Priority (Updated)

```
func (r *Runtime) Execute(ctx, proc, execCtx):
    1. Delegated   → procedure pushed to backend (PostgreSQL/MySQL native)
    2. Out-of-Proc → tgpiler-compiled binary via DSBP
    3. JIT         → in-process compiled Go plugin
    4. Interpreted  → AST walk (always available)
```

Out-of-process execution occupies a distinct niche: procedures too complex for delegation (which requires the backend to support the full logic natively) but where the overhead of in-process JIT compilation or the isolation guarantees of a separate address space are desirable.

---

## 1. Dual-Channel Architecture

Communication between the aul server and a worker binary is split across two Unix Domain Sockets to avoid head-of-line blocking. Control signals (metadata, errors, cancellation) must never compete with bulk data for socket buffer space.

```
                        aul server
                    ┌───────────────────┐
                    │   Connection      │
                    │   Handler         │
                    │                   │
                    │  ┌─────────────┐  │
    Client ◄────────┤  │ TDS / PG /  │  │
    (DBeaver,       │  │ MySQL wire  │  │
     SSMS, etc.)    │  └──────┬──────┘  │
                    │         │         │
                    │  ┌──────▼──────┐  │
                    │  │  DSBP       │  │
                    │  │  Dispatcher │  │
                    │  └──┬──────┬───┘  │
                    └─────┼──────┼──────┘
                          │      │
             Control UDS  │      │  Data UDS
           (bidirectional)│      │  (worker → server)
                          │      │
                    ┌─────▼──────▼──────┐
                    │  Worker Binary    │
                    │  (tgpiler output) │
                    │                   │
                    │  Proc A           │
                    │    └─ Proc B      │
                    │         └─ Proc C │
                    └───────────────────┘
```

### 1.1 The Control Channel (Bidirectional)

Carries session handshake, procedure invocation, result set metadata, output parameters, errors, heartbeats, and cancellation signals. Low volume, high importance. Messages are synchronous during initialisation (handshake, metadata barrier) and asynchronous during execution (heartbeats, cancellation).

### 1.2 The Data Channel (Unidirectional: Worker → Server)

Carries tabular row data exclusively. Designed as a continuous binary stream of CBOR-encoded frames optimised for zero-copy forwarding. The server can use `splice(2)` and `tee(2)` to move data from the UDS file descriptor directly to the client's TCP socket without copying through user-space memory.

**Invariant:** The data channel is strictly unidirectional (worker → server) in DSBP v1. The server must never write to the data socket, even if future flag extensions add new capabilities. Server-to-worker communication uses the control channel exclusively. This constraint is what makes `splice(2)` safe: the server can treat the data socket as a read-only file descriptor with no risk of interleaved writes corrupting the stream. Implementations should treat any write attempt to the data socket as a fatal bug (assertion failure), not a recoverable error.

### 1.3 Why Two Channels

A single multiplexed channel requires the server to parse every byte of the stream to distinguish control frames from data frames. With the dual-channel model:

- The server can block-read the data channel with `splice(2)` — no parsing, no user-space copy.
- Cancellation signals on the control channel are delivered immediately, regardless of how much row data is buffered.
- Backpressure on the data channel (slow client) does not block error delivery.

---

## 2. Channel Synchronisation and the Metadata Barrier

A result set's column schema must reach the server before any rows for that set. Without a synchronisation barrier, the data channel could deliver rows that the server cannot yet parse or forward. The protocol enforces this with a three-step handshake per result set:

```
    Worker                              Server
      │                                    │
      │──── RS_META (Control) ────────────►│  "Here are the columns"
      │                                    │
      │                                    │  Server prepares upstream
      │                                    │  buffers (e.g. TDS COLMETADATA)
      │                                    │
      │◄──── RS_READY (Control) ──────────│  "Ready to receive rows"
      │                                    │
      │════ RS_ROW (Data) ═══════════════►│  Row data flows
      │════ RS_ROW (Data) ═══════════════►│
      │════ RS_ROW (Data) ═══════════════►│
      │                                    │
      │════ RS_DONE (Data) ══════════════►│  "End of this result set"
      │                                    │
```

The worker must not write any `RS_ROW` frames to the data channel until it has received `RS_READY` on the control channel. This guarantees that the server's `splice(2)` loop is configured with the correct column dimensions before the data firehose opens.

For procedures returning multiple result sets, this sequence repeats for each set. Between sets, the server emits the appropriate upstream tokens (e.g., TDS `DONEINPROC`) before acknowledging readiness for the next set.

---

## 3. CBOR Data Representation

The protocol uses CBOR (RFC 8949) for row serialisation. CBOR is chosen over JSON for compactness, native binary support (no base64 encoding of BLOBs), and support for indefinite-length sequences which enable streaming without knowing row counts in advance.

### 3.1 Metadata Frame (Control Channel)

Sent as the payload of `RS_META`:

```cbor
{
  "columns": [
    {"name": "ID",    "type": "INT",     "nullable": false},
    {"name": "Name",  "type": "NVARCHAR", "len": 255, "nullable": true},
    {"name": "Price", "type": "DECIMAL",  "precision": 10, "scale": 2}
  ]
}
```

Type identifiers follow the T-SQL type system. The server maps these to the appropriate wire format for the client's protocol (TDS types, PostgreSQL OIDs, MySQL field types).

### 3.2 Row Frames (Data Channel)

Each `RS_ROW` frame payload begins with a 4-byte batch row count (uint32, big-endian), followed by a CBOR array of arrays. The batch row count is the number of top-level elements in the CBOR array — i.e., the number of rows in this batch, not fields or bytes. If the CBOR payload is decoded for any reason (e.g., non-splice forwarding, debugging, or validation), the decoded array length must match the batch count; a mismatch is a protocol error. The batch count allows the server to track row totals without decoding CBOR (see section 11.1). Rows are batched adaptively (see section 4) rather than sent individually:

```
[4 bytes: batch row count = 3]
[CBOR payload:]
[
  [1, "Widget", 29.99],
  [2, "Gadget", 49.99],
  [3, null, 0.00]
]
```

NULL values are represented as CBOR null (`0xf6`). Type coercion follows the column metadata: the server interprets the CBOR value according to the declared SQL type, not the CBOR major type.

### 3.3 Multiple Result Sets

T-SQL procedures routinely return multiple result sets. DSBP handles this through repeated metadata-barrier-stream cycles:

```
RS_META  (columns for set 1)     Control
RS_READY (ack)                   Control
RS_ROW   (rows for set 1)       Data
RS_DONE  (end of set 1)         Data
RS_META  (columns for set 2)     Control
RS_READY (ack)                   Control
RS_ROW   (rows for set 2)       Data
RS_DONE  (end of set 2)         Data
OUT_PARAM (output variables)     Control
STATUS    (return code)          Control
```

The server maps each cycle to the upstream protocol's multi-result-set mechanism (TDS `COLMETADATA` + `ROW` + `DONEINPROC`; PostgreSQL `RowDescription` + `DataRow` + `CommandComplete`).

On normal completion, the server must observe `RS_DONE` for the current result set on the data channel before sending `RS_READY` for the next set on the control channel. This serialises the transitions and prevents the worker from interleaving rows from different result sets.

---

## 4. Adaptive Data Plane Batching

Fixed row counts per frame are suboptimal: a table with two INT columns produces ~8 bytes per row, while a table with NVARCHAR(MAX) columns may produce kilobytes per row. DSBP uses an adaptive buffer strategy.

### 4.1 Flush Rules

The worker maintains a local serialisation buffer and flushes an `RS_ROW` frame to the data channel when any of these conditions is met:

| Condition | Rationale |
|-----------|-----------|
| Buffer exceeds 64 KB | Aligns with OS page sizes and typical UDS buffer capacity |
| 10ms since last flush | Ensures low-latency visibility for slow-producing queries |
| Result set ends | Final partial batch must not be held |

### 4.2 Frame Size Rationale

64 KB is chosen as the target frame size because:

- It matches the default `SO_SNDBUF` size on most Linux configurations.
- It is large enough to amortise the 8-byte TLV header overhead.
- It is small enough to avoid excessive buffering latency.
- It aligns well with `splice(2)` pipe buffer sizes (typically 64 KB per pipe page).

Implementations may tune this value. The protocol does not mandate a maximum frame size, but receivers must handle frames up to 4 GB (the TLV length field limit).

---

## 5. Frame Specification (TLV)

Every message on both channels is prefixed with an 8-byte header:

```
┌──────────┬──────────┬──────────┬──────────────────────────────┬──────────┐
│ Byte 0   │ Byte 1   │ Byte 2   │ Bytes 3-6                    │ Byte 7   │
│ Version  │ Type     │ Flags    │ Length (uint32, big-endian)   │ Reserved │
├──────────┼──────────┼──────────┼──────────────────────────────┼──────────┤
│ 1 byte   │ 1 byte   │ 1 byte   │ 4 bytes                      │ 1 byte   │
└──────────┴──────────┴──────────┴──────────────────────────────┴──────────┘
```

**Version** (byte 0): Protocol version. Current value: `0x01`. A receiver must reject frames with an unrecognised version and close the session with an `ERROR` frame. This allows future changes to the serialisation format or header layout without breaking existing worker binaries.

**Type** (byte 1): Frame type identifier (see section 5.1).

**Flags** (byte 2): Reserved bitfield for per-frame options. Senders in v1 must set this to `0x00`. Receivers must ignore unknown flag bits and must not reject a frame solely because flags are non-zero. This permits forward compatibility with future senders. Anticipated uses include payload compression (bit 0), alternate serialisation formats (bit 1), and continuation frames for payloads exceeding 4 GB (bit 2).

**Length** (bytes 3-6): Number of payload bytes following the header, as a 32-bit unsigned big-endian integer. A length of 0 is valid (used by `RS_READY`, `RS_ABORT`, and `CMD_CANCEL`).

**Reserved** (byte 7): Must be `0x00`. Provides 8-byte alignment and reserves space for future use.

### 5.1 Frame Types

| Type | Name | Channel | Direction | Payload |
|:-----|:-----|:--------|:----------|:--------|
| `0x01` | `EXEC_CMD` | Control | S → W | Procedure name, serialised parameters, session context |
| `0x02` | `RS_META` | Control | W → S | CBOR column metadata |
| `0x03` | `RS_ROW` | Data | W → S | 4-byte batch row count (uint32 BE) + CBOR row batch |
| `0x04` | `OUT_PARAM` | Control | W → S | CBOR map of output variable names to final values |
| `0x05` | `STATUS` | Control | W → S | 4-byte return code (int32, big-endian) |
| `0x06` | `ERROR` | Control | W → S | CBOR error: `{code, message, severity, state}` |
| `0x07` | `HEARTBEAT` | Control | W → S | 8-byte Unix timestamp (nanoseconds, big-endian) |
| `0x08` | `RS_READY` | Control | S → W | Empty payload (acknowledgement) |
| `0x09` | `RS_DONE` | Data | W → S | 8-byte total row count for this result set (uint64, big-endian) |
| `0x0A` | `RS_ABORT` | Data | W → S | Empty payload (stream terminated) |
| `0x0B` | `CMD_CANCEL` | Control | S → W | Empty payload (attention signal) |
| `0x0C` | `AUTH_CHALLENGE` | Control | S → W | 32 bytes: random challenge |
| `0x0D` | `AUTH_RESPONSE` | Control | W → S | 32 bytes: HMAC-SHA-256(nonce, challenge) |

### 5.2 Payload Size Constraints

The 4-byte length field supports payloads up to 4,294,967,295 bytes (4 GB). In practice:

- `RS_ROW` frames target 64 KB but may be larger for wide rows. The first 4 bytes of every `RS_ROW` payload are a batch row count (uint32, big-endian), followed by the CBOR-encoded row data. This allows the server to track row counts without decoding CBOR, which is essential for the splice loop (section 11.1): the server reads the TLV header (8 bytes) and the batch count (4 bytes) in user space, then splices the remaining `length - 4` bytes directly to the client.
- `RS_META` frames are typically under 4 KB.
- `ERROR` frames are typically under 1 KB.
- `RS_DONE` always carries exactly 8 bytes (the total row count for the completed result set).
- `HEARTBEAT` always carries exactly 8 bytes (Unix timestamp in nanoseconds).
- `STATUS` always carries exactly 4 bytes (return code).
- `RS_READY`, `RS_ABORT`, and `CMD_CANCEL` carry empty payloads (length 0).

### 5.3 RS_DONE and Data Integrity

The `RS_DONE` frame carries the total number of rows the worker produced for the result set. The server must track the number of rows received across all `RS_ROW` batches and compare against this total. A mismatch indicates data loss (e.g., truncated UDS buffer, premature socket closure) and the server must treat the result set as incomplete, logging the discrepancy and returning an error to the client rather than delivering partial data silently. This cross-check is the primary defence against silent data truncation and should be treated as a critical-path assertion in the server implementation — it must never be downgraded to a warning or skipped for performance reasons.

**Invariants:**

- `RS_DONE` and `RS_ABORT` are mutually exclusive. Every result set stream terminates with exactly one of them, never both.
- `RS_DONE` must never follow `RS_ABORT` for the same result set.
- If an error occurs mid-batch (after some `RS_ROW` frames have been sent but before the set is complete), the worker sends `RS_ABORT`, not `RS_DONE`. The row count in a partially-streamed set is undefined and must not be trusted by the server.
- If the server has already forwarded rows upstream before receiving `RS_ABORT`, it must signal an error to the client using the upstream protocol's error mechanism (e.g., TDS `ERROR` token followed by `DONE` with `ERROR` status, PostgreSQL `ErrorResponse`). The server must not silently discard the abort and let the client believe the partial result set was complete.
- `RS_ABORT` terminates the active result set and implies session failure. The worker must not send `RS_META` for subsequent result sets after an abort. The session proceeds directly to `ERROR` and `STATUS` on the control channel.

---

## 6. Session Lifecycle

A complete DSBP session follows this sequence:

```
    Worker starts ──► opens Control UDS
                  ──► opens Data UDS

    Server ─── EXEC_CMD ───► Worker          (invoke procedure)

    Worker ─── RS_META ────► Server          (result set 1 columns)
    Server ─── RS_READY ───► Worker          (ready)
    Worker ═══ RS_ROW ═════► Server          (rows, adaptive batches)
    Worker ═══ RS_DONE ════► Server          (end of result set 1)

    Worker ─── RS_META ────► Server          (result set 2 columns)
    Server ─── RS_READY ───► Worker          (ready)
    Worker ═══ RS_ROW ═════► Server          (rows)
    Worker ═══ RS_DONE ════► Server          (end of result set 2)

    Worker ─── OUT_PARAM ──► Server          (output variables)
    Worker ─── STATUS ─────► Server          (return code; session ends)

    Worker exits ──► closes both sockets
    Server reaps worker process
```

Dashed arrows (───) denote control channel. Double arrows (═══) denote data channel.

**Ordering rule:** The worker must not send `OUT_PARAM` or `STATUS` until it has sent the final terminator (`RS_DONE` or `RS_ABORT`) for the last result set. If output parameters are present, `OUT_PARAM` must precede `STATUS`. `STATUS` is always the last frame of the session.

---

## 7. Heartbeat and Timeout

### 7.1 Worker Heartbeat

If the worker has not sent a frame on either channel for 15 seconds, it must emit a `HEARTBEAT` frame on the control channel. This covers scenarios where the worker is executing a long-running backend query and has no rows to stream.

### 7.2 Server Timeout

If the server receives no frame (data or control) and no heartbeat for 30 seconds (or the configured `--exec-timeout`), it assumes the worker is in a zombie state. The server:

1. Sends `CMD_CANCEL` on the control channel (best-effort).
2. Waits 5 seconds for a graceful `STATUS` or `ERROR` response.
3. If no response, issues `os.Process.Kill()`.
4. Drains and discards any remaining data channel bytes.
5. Returns an error to the upstream client.

### 7.3 Timing Invariants

- A `HEARTBEAT` frame resets the server's timeout clock, even if the data channel is idle. The server treats any frame on either channel as evidence of liveness.
- The server's timeout clock measures time since the last **complete TLV frame** was parsed, not time since the last byte was received. A half-read frame on a stalled socket (e.g., header received but payload blocked) does not constitute liveness.
- Heartbeats must not be coalesced with other control frames. A heartbeat is its own TLV frame, not a field within another message.
- `HEARTBEAT` length must be exactly 8 bytes. Any other length is a protocol error and the server should treat it as a malformed frame.
- The worker's 15-second heartbeat interval and the server's 30-second timeout provide a margin of one missed heartbeat before the server acts. Implementations should not reduce the timeout below twice the heartbeat interval.

---

## 8. Error Handling and Cancellation

### 8.1 Worker-Initiated Error

When the worker encounters an error during execution (e.g., constraint violation, division by zero):

1. If currently streaming rows: send `RS_ABORT` on the data channel.
2. Send `ERROR` on the control channel with structured error details.
3. Send `STATUS` with a non-zero return code.
4. Exit.

### 8.2 Client-Initiated Cancellation (Attention Signal)

When the end client sends an attention signal (TDS Attention, PostgreSQL CancelRequest):

```
    Client ──► Server ─── CMD_CANCEL ───► Worker (Control)
                                                │
                                    Worker stops query
                                                │
              Server ◄═══ RS_ABORT ═════ Worker (Data)
              Server ◄─── ERROR ────── Worker (Control)
              Server ◄─── STATUS ───── Worker (Control)
```

The worker receives `CMD_CANCEL` via a dedicated goroutine monitoring the control channel. This goroutine signals the main execution path (e.g., via `context.Cancel()`). The worker then:

1. Aborts the backend query.
2. Sends `RS_ABORT` on the data channel to terminate any active row stream.
3. Sends `ERROR` with code 3621 (T-SQL "The statement has been terminated").
4. Sends `STATUS` with return code -1.

This out-of-band signalling is a key advantage of the dual-channel model. In a single-channel protocol, delivering a cancel signal requires either byte-level scanning of the entire data stream or waiting for the current frame to complete — neither of which is acceptable when the data channel is saturated.

### 8.3 Server Drain Protocol

When the server receives `RS_ABORT`, it must drain the data channel of any bytes that were already in-flight (buffered in the UDS kernel buffer) before the worker wrote the abort sentinel. Drain means parsing TLV headers and skipping `length` bytes per frame; it MUST NOT be implemented as an unframed `io.Copy(io.Discard, sock)` loop, because the server must recognise the `RS_ABORT` sentinel frame to terminate. The drain loop reads and discards frames until `RS_ABORT` is encountered or the socket reaches EOF:

```go
for {
    frameType, payload, err := readFrame(dataSocket)
    if err == io.EOF {
        // Fatal: EOF before RS_ABORT is an incomplete stream (protocol error),
        // even if ERROR/STATUS were received on the control channel.
        return protocolError("data channel EOF before RS_ABORT sentinel")
    }
    if frameType == RS_ABORT {
        break
    }
    // discard payload
}
```

The server may read the control channel concurrently during the drain, but must not declare the session complete until both channels have reached their terminal state (data channel drained, control channel `STATUS` received).

---

## 9. Recursive Packing and Internal Execution

`tgpiler --recursive` resolves a procedure's dependency tree at transpilation time and packs all reachable procedures into a single Go binary.

### 9.1 Internal Call Dispatch

When Procedure A calls Procedure B (`EXEC B`) and both are packed in the same binary:

- The call resolves as a direct Go function call. No socket communication, no serialisation, no DSBP overhead.
- Both procedures share the same `abi.StorageBackend` and `*sql.Tx` pointers passed during the initial `EXEC_CMD` handshake.
- Procedure B writes to the same data channel initialised by Procedure A. The server sees a seamless continuation of the row stream.

### 9.2 Root Control

Only the root procedure (the one named in `EXEC_CMD`) may send the terminal `STATUS` frame. Internally called procedures return their results through Go return values. This prevents nested procedures from prematurely closing the DSBP session.

### 9.3 Nesting Limits

The transpiler verifies the dependency graph is acyclic during compilation. The generated code includes an atomic nesting counter:

```go
var nestingLevel atomic.Int32

func execProcB(ctx context.Context, backend abi.StorageBackend, ...) error {
    if nestingLevel.Add(1) > 32 {
        nestingLevel.Add(-1)
        return fmt.Errorf("nesting limit exceeded (32)")
    }
    defer nestingLevel.Add(-1)
    // ... procedure body ...
}
```

If an internal call exceeds the nesting limit (default 32), the binary initiates the abort sequence.

### 9.4 Isolation Properties

The unit of process isolation is the dependency tree, not the individual procedure. This has important implications:

- **Version consistency:** All procedures in the binary were compiled together. There is no risk of version skew between caller and callee.
- **Failure boundary:** If any procedure in the tree panics, the entire binary aborts cleanly via the abort sequence. The aul server receives a structured error and returns the session to a clean state.
- **Memory footprint:** Packing increases the binary's resident set size but eliminates the overhead of multiple processes for a single call chain.

---

## 10. EXEC_CMD Payload

The `EXEC_CMD` frame carries everything the worker needs to begin execution:

```cbor
{
  "procedure": "GetCustomerOrders",
  "params": {
    "@CustomerID": 42,
    "@Status": "paid",
    "@PageSize": 10,
    "@PageNumber": 1
  },
  "output_params": ["@TotalCount"],
  "session": {
    "database": "master",
    "user": "sa",
    "tenant": "acme",
    "transaction_id": null,
    "isolation_level": "READ_COMMITTED",
    "options": {
      "ANSI_NULLS": true,
      "QUOTED_IDENTIFIER": true,
      "NOCOUNT": true
    }
  },
  "backend": {
    "driver": "sqlite",
    "dsn": "/data/acme/ecommerce.db"
  }
}
```

The `backend` section provides the worker with the information it needs to open its own database connection.

### 10.1 Transaction Visibility Boundary

Because the worker opens an independent database connection, it operates in a separate transaction context from the aul server. This has an important semantic consequence: the worker cannot see uncommitted data from the server's session, and vice versa.

In practice this means:

- If the client has begun a transaction and inserted rows but not yet committed, a procedure executed out-of-process will not see those rows.
- The worker's own writes are invisible to the server until the worker commits.
- This is consistent with `READ COMMITTED` isolation but diverges from in-process execution (interpreted or JIT), where the procedure shares the server's connection and transaction.

This is an acceptable boundary for DSBP v1. Full transaction sharing requires passing the database connection's file descriptor to the worker via `SCM_RIGHTS` (see Appendix B), which eliminates the independent-connection limitation at the cost of additional implementation complexity.

**Operational note:** This means that the same procedure can return different results depending on its execution path. If a client begins a transaction, inserts rows, and then calls a procedure, the in-process (interpreted or JIT) path will see the uncommitted rows but the out-of-process (DSBP) path will not. Operators should be aware of this when configuring execution priority, particularly in environments where procedures are called within open transactions.

---

## 11. Trust Boundaries

The aul server operates as an I/O multiplexer. It trusts the worker binary to respect TLV framing but remains immune to the worker's internal state:

| Concern | Server Responsibility | Worker Responsibility |
|---------|----------------------|----------------------|
| Frame validity | Validate TLV headers, reject oversized frames | Produce well-formed CBOR payloads |
| Authentication | Issue challenge, verify HMAC, reject impostors | Prove nonce possession via HMAC |
| Timeout | Kill unresponsive workers | Emit heartbeats |
| Cancellation | Deliver `CMD_CANCEL` | Honour cancellation promptly |
| Stream termination | Drain until `RS_DONE`/`RS_ABORT` | Always send terminal sentinel |
| Process lifecycle | Fork, wait, reap | Exit cleanly after `STATUS` |
| Resource limits | Enforce memory/CPU cgroup constraints | Operate within limits |
| Binary staleness | Track tree hashes, degrade to interpreter | N/A (server-side concern) |

The server never interprets CBOR row data during `splice(2)` forwarding. It only parses CBOR on the control channel (metadata, errors, parameters) where payloads are small and well-bounded. However, the server must always parse TLV frame headers on the data channel so it can detect `RS_DONE` and `RS_ABORT` boundaries. The data channel is not fully opaque at the frame level — only `RS_ROW` payloads may be spliced without content inspection.

### 11.1 The Splice Loop

The optimal server-side hot path for the data channel is a "read header, splice payload" loop:

```
loop:
    read 8 bytes from data UDS into user-space   (TLV header)
    parse type and length

    if type == RS_ROW:
        read 4 bytes from data UDS into user-space   (batch row count, uint32 BE)
        row_counter += batch_row_count
        splice(data_uds_fd, NULL, client_tcp_fd, NULL, length - 4, ...)
    else if type == RS_DONE:
        read 8-byte payload (total row count)
        verify row_counter == total_row_count
        break
    else if type == RS_ABORT:
        drain remaining frames
        break
```

Per frame, only 12 bytes enter user-space (8-byte header + 4-byte batch count). The CBOR row payload — which constitutes the vast majority of bytes — moves directly from the UDS kernel buffer to the client TCP socket via the kernel's pipe infrastructure. This is the fastest path available on Linux for proxying bulk data between file descriptors.

On platforms without `splice(2)` (macOS, FreeBSD), the server falls back to a user-space `read`/`write` loop for the `RS_ROW` payload. The protocol semantics are identical — only throughput is affected. The TLV framing, batch count prefix, and row count verification work the same way regardless of the forwarding mechanism.

---

## 12. Dependency Invalidation and Graceful Degradation

### 12.1 The Problem

When `tgpiler --recursive` packs procedures A, B, and C into a single binary, the binary is valid only as long as all three source files remain unchanged. If procedure B (a helper called by both A and C) is modified on disc, every binary containing B is stale: its compiled logic no longer matches the source of truth.

This is distinct from the in-process JIT case, where invalidation is cheap (discard the compiled function, fall back to the interpreter on the next call). An out-of-process binary is a file on disc, potentially running, and potentially referenced by multiple root procedures.

### 12.2 Tree Hash

Each compiled binary is identified by a **tree hash**: the hash of the concatenation of the source hashes of every procedure in its dependency tree, in topological order. `tgpiler` records this in the binary's metadata at build time.

```
tree_hash = SHA-256(
    hash(procA.sql) || hash(procB.sql) || hash(procC.sql)
)
```

The server's binary registry stores a mapping:

```
binary path  →  { root procedure, tree hash, [member procedures] }
```

### 12.3 Invalidation Trigger

The existing hot-reload mechanism (file watcher on the procedures directory) already detects source changes and recomputes `SourceHash` per procedure. When a procedure's hash changes, the server must additionally:

1. Scan the binary registry for every binary whose member list includes the changed procedure.
2. Mark each matching binary as **stale**.
3. For each stale binary that is currently running: allow it to complete its current invocation (do not kill mid-execution), but do not dispatch new invocations to it.

### 12.4 Graceful Degradation

A stale binary is not immediately deleted. The server follows this sequence:

```
Source change detected (proc B)
        │
        ▼
Mark all binaries containing B as stale
        │
        ├──► Running invocations: finish normally
        │
        ├──► New invocations of procs in stale binaries:
        │        fall back to interpreted execution
        │
        └──► Background: enqueue recompilation of each stale binary
                │
                ▼
        On successful recompilation:
            replace binary, update tree hash,
            resume out-of-proc dispatch
```

The key property is that the system never enters a state where execution is unavailable. The interpreter is always the fallback. The user experiences a temporary performance regression (interpreted is slower than compiled), not an outage.

### 12.5 Recompilation Ordering

If multiple binaries are stale, the server should recompile them in order of invocation frequency (hottest first). Recompilation is asynchronous and must not block request handling.

### 12.6 Concurrent Invocations During Transition

A binary may be mid-execution when it is marked stale. The server maintains a reference count per binary. A stale binary is eligible for deletion (or replacement) only when its reference count reaches zero. This prevents removing a binary whose worker process is still running.

---

## 13. Process Authentication

### 13.1 Threat Model

In a shared-host environment, the UDS sockets used by DSBP are filesystem objects. Without authentication, two risks arise:

- **Misdirected commands:** An aul server instance connects to a worker spawned by a different aul instance, issuing `EXEC_CMD` to a process that holds a different tenant's database connection.
- **Adversarial injection:** A compromised process on the same host connects to a worker's control socket and issues commands, potentially exfiltrating data through the data channel.

Standard Unix file permissions (socket owned by the aul user, mode `0600`) mitigate the second risk in non-compromised environments, but are insufficient when multiple aul instances run under the same user or when defence-in-depth is required.

### 13.2 Authentication Mechanism

DSBP uses a per-session cryptographic nonce to bind a worker process to the specific aul server that spawned it.

**At fork time**, the server:

1. Generates a 256-bit cryptographically random nonce using `crypto/rand`.
2. Passes the nonce to the worker via a file descriptor (not an environment variable — see section 13.3) using a pipe that is closed immediately after the worker reads it.
3. Records the nonce in its session table, keyed by worker PID.

**At handshake time**, before the server sends `EXEC_CMD`:

1. The server sends an `AUTH_CHALLENGE` frame on the control channel containing a second random 256-bit value (the challenge).
2. The worker computes `HMAC-SHA-256(nonce, challenge)` and returns it in an `AUTH_RESPONSE` frame.
3. The server independently computes the same HMAC and compares. If the values match, the session proceeds. If not, the server closes both sockets and kills the worker.

This proves the worker possesses the nonce without transmitting the nonce over the socket (preventing replay by an eavesdropper on the UDS).

### 13.3 Why Not Environment Variables

Environment variables are readable by any process with access to `/proc/<pid>/environ` on Linux (same-user processes, root, or any process with `CAP_SYS_PTRACE`). A pipe file descriptor is visible only to the parent and child processes and is destroyed on read. This is a meaningful improvement in the threat model.

### 13.4 Frame Extensions for Authentication

Two additional frame types are used during the handshake phase only:

| Type | Name | Channel | Direction | Payload |
|:-----|:-----|:--------|:----------|:--------|
| `0x0C` | `AUTH_CHALLENGE` | Control | S → W | 32 bytes: random challenge |
| `0x0D` | `AUTH_RESPONSE` | Control | W → S | 32 bytes: HMAC-SHA-256(nonce, challenge) |

The handshake occurs once per session, immediately after socket establishment and before `EXEC_CMD`. A worker that receives `EXEC_CMD` before `AUTH_CHALLENGE` must reject the session and exit.

### 13.5 Session Lifecycle (Updated)

```
    Worker starts ──► reads nonce from pipe FD
                  ──► opens Control UDS
                  ──► opens Data UDS

    Server ─── AUTH_CHALLENGE ──► Worker      (random challenge)
    Worker ─── AUTH_RESPONSE ───► Server      (HMAC proof)

    Server ─── EXEC_CMD ───► Worker           (invoke procedure)
    ...                                        (normal DSBP session)
```

### 13.6 Scope and Limitations

Process authentication is optional and controlled by a server configuration flag (`--worker-auth`). When disabled, the handshake frames are omitted and `EXEC_CMD` is sent immediately after socket establishment. This is appropriate for development environments and single-tenant deployments where the UDS file permissions provide sufficient isolation.

Authentication does not provide encryption. The nonce proves identity; it does not protect data in transit. For encrypted channels, see the TLS consideration in Appendix B.

---

## 14. Relationship to Existing Execution Paths

DSBP does not replace in-process execution. It adds a new tier:

```
Execution Path     │ Where Runs        │ Use Case
───────────────────┼───────────────────┼──────────────────────────────
Delegated          │ Backend DB        │ Simple procs, backend supports
Out-of-Proc (DSBP) │ Separate process  │ Complex procs, isolation needed
JIT (in-process)   │ aul process       │ Complex procs, low latency
Interpreted        │ aul process       │ Fallback, always available
```

The decision of which path to use follows the existing automatic optimisation model described in [002-PROCEDURE_STORAGE_AND_TRANSLATION](002-PROCEDURE_STORAGE_AND_TRANSLATION.md). The `considerOptimisation()` function evaluates whether a frequently-executed procedure would benefit from out-of-process compilation based on its complexity profile, isolation requirements, and the availability of a cached binary.

---

## Appendix A: Socket Naming Convention

Worker sockets are created in a per-session subdirectory of the platform's runtime directory:

```
{runtime_dir}/aul/workers/{session_id}/control.sock
{runtime_dir}/aul/workers/{session_id}/data.sock
```

### A.1 Runtime Directory Resolution

The server resolves `{runtime_dir}` using the following precedence:

1. `$AUL_RUNTIME_DIR` — explicit override (honours containerised and non-standard layouts).
2. `$XDG_RUNTIME_DIR` — per-user runtime directory, typically `/run/user/<uid>` on systemd-based systems. Preferred when aul runs as a non-root user.
3. `/run` — FHS 3.0 canonical location for system-level runtime data (Linux). Used when aul runs as a system service.
4. `/var/run` — legacy equivalent. On modern Linux this is usually a symlink to `/run`, but on older systems and some BSDs it may be the only option. On macOS (Darwin), `/var` is a symlink to `/private/var`, so this resolves to `/private/var/run`.
5. `$TMPDIR/aul-run` — Darwin-specific fallback. macOS sets `$TMPDIR` to a per-user temporary directory (e.g., `/var/folders/xx/.../T/`), which is always writable and requires no privilege escalation.
6. `$AUL_DATA_DIR/run` — final fallback when none of the above exist (e.g., unprivileged installs with no access to system runtime directories).

Each candidate is tested in order: the server uses the first directory that exists and is writable. If none of the standard locations are available, the server falls back to creating `$AUL_DATA_DIR/run`.

### A.2 Platform Notes

**Linux:** `/run` and `$XDG_RUNTIME_DIR` are available on all systemd-based distributions. Older sysvinit systems may only have `/var/run`. The `splice(2)` fast path (section 11.1) is Linux-specific.

**macOS (Darwin):** There is no `/run` directory. `/var/run` exists (via `/private/var/run`) but is typically root-owned. For non-root users, `$TMPDIR` is the most reliable writable location. Note that macOS has no `splice(2)` syscall; the data channel falls back to user-space `read`/`write` forwarding. This does not affect protocol correctness, only throughput.

**FreeBSD / other BSDs:** `/var/run` is the standard runtime directory. `splice(2)` is not available; use `sendfile(2)` where applicable or fall back to user-space forwarding.

### A.3 Socket Path Length

Unix domain socket paths are subject to a platform-defined maximum (typically 108 bytes on Linux, approximately 104 bytes on macOS; consult `sizeof(((struct sockaddr_un *)0)->sun_path)` for the exact value on the target platform). Implementations MUST validate the total path length against the platform limit before calling `bind(2)` and fail with a clear error if it would be exceeded. Session IDs should be kept short — hex-encoded 8-byte values (16 characters) are recommended. With a typical runtime directory of `/var/run` (8 bytes) and the fixed structure `/aul/workers/` (13 bytes) plus `/control.sock` (13 bytes), this leaves ample headroom on all supported platforms.

### A.4 Permissions and Cleanup

The server must create the `aul/workers/{session_id}/` subtree with permissions `0700` and verify ownership before binding sockets. Socket files are removed when the worker process exits or is reaped by the server. The session directory itself is removed after cleanup.

## Appendix B: Future Considerations

- **Connection sharing:** Workers currently open their own database connections. A future optimisation could pass an existing connection file descriptor via `SCM_RIGHTS` on the control channel, eliminating connection setup overhead.
- **Worker pooling:** Rather than forking a new worker per invocation, long-lived worker pools could accept multiple `EXEC_CMD` frames sequentially, amortising process startup cost. Worker pooling will require a protocol v2 (version byte `0x02`) because several v1 invariants assume a single session per socket pair:
  - `STATUS` is terminal and closes the session
  - Authentication nonce is per-invocation
  - Socket lifecycle is tied to process lifecycle
  - No session-reset semantics exist

  A pooled model would need session framing, re-authentication between invocations, and explicit reset/drain semantics to recycle a socket pair safely.
- **TLS on sockets:** For deployments where the worker runs on a different host (TCP instead of UDS), the protocol should support optional TLS on both channels.

## Appendix C: Protocol Invariants (Test Checklist)

The following invariants are mechanically checkable and should be implemented as assertions, test cases, or fuzzing targets. "MUST" and "MUST NOT" are used in the RFC 2119 sense.

**General error disposition:** Any violation of a MUST/MUST NOT rule below is a fatal protocol error. The detecting side MUST close both sockets. If the server detects the violation, it SHOULD send `ERROR` on the control channel if it is still writable (best-effort; do not attempt to resurrect a closed channel) and kill the worker process. If the worker detects the violation, it MUST send `ERROR` and `STATUS` on the control channel (best-effort) and exit.

**Framing:**

- Every frame begins with an 8-byte header: version (1 byte), type (1 byte), flags (1 byte), length (4 bytes, uint32 big-endian), reserved (1 byte).
- Version byte MUST be `0x01`. On version mismatch: close sockets, send `ERROR` on control channel if possible.
- Senders in v1 MUST set flags to `0x00`. Receivers MUST ignore unknown flag bits and MUST NOT reject a frame solely because flags are non-zero. This permits forward compatibility with future senders.
- Reserved byte MUST be `0x00`.
- Payload length MUST match the declared length field exactly.

**Channel discipline:**

- Server MUST NOT write to the data socket. Implementations should wrap the data FD in a type that only exposes read/splice operations. Any write attempt is a fatal bug (assertion failure).
- Server MUST parse TLV headers on the data channel even when splicing row payloads, so it can detect `RS_DONE`/`RS_ABORT` boundaries. The data channel is not fully opaque; frame headers are always parsed, only `RS_ROW` payloads may be spliced without inspection.
- Worker MUST NOT write control frame types (`RS_META`, `OUT_PARAM`, `STATUS`, `ERROR`, `HEARTBEAT`, `AUTH_RESPONSE`) to the data socket.
- Worker MUST NOT write data frame types (`RS_ROW`, `RS_DONE`, `RS_ABORT`) to the control socket.

**Metadata barrier:**

- Worker MUST NOT send `RS_ROW` before receiving `RS_READY` for the current result set.
- Server MUST NOT send `RS_READY` before receiving `RS_META` for the current result set.
- Server MUST observe `RS_DONE` on the data channel before sending `RS_READY` for the next result set.

**Stream termination:**

- Every result set terminates with exactly one of `RS_DONE` or `RS_ABORT`, never both, never neither.
- `RS_DONE` MUST NOT follow `RS_ABORT` for the same result set.
- `RS_ABORT` terminates the session. No `RS_META` for subsequent result sets may follow.
- `RS_ABORT` MAY appear without any prior `RS_ROW` frames (e.g., error during query execution before any rows were produced). The drain loop must handle this case.
- After `RS_ABORT`, the worker MUST send `ERROR` then `STATUS` on the control channel and exit.
- On `RS_ABORT`, server MUST drain and discard data channel frames until it encounters the `RS_ABORT` sentinel or the data socket reaches EOF (close). EOF before `RS_ABORT` is a fatal protocol error (incomplete stream), even if `ERROR` and `STATUS` have already been received on the control channel. The server MUST NOT declare the session complete until it has both drained the data channel and observed `ERROR`/`STATUS` on the control channel, but it MAY read both channels concurrently.

**Control channel ordering:**

- Worker MUST NOT send `OUT_PARAM` or `STATUS` until it has sent the final terminator (`RS_DONE` or `RS_ABORT`) for the last result set.
- Server MUST NOT treat the session as complete until it has observed the final terminator on the data channel and then read `STATUS` on the control channel.
- `OUT_PARAM` (if present) MUST precede `STATUS`.
- `STATUS` is the final frame. No frames may follow it on either channel.

**Data integrity:**

- Every `RS_ROW` payload begins with a 4-byte batch row count (uint32, big-endian) followed by CBOR row data.
- `RS_ROW` length MUST be greater than 4. A length of exactly 4 (batch count with no CBOR data) or less than 4 is a protocol error.
- Workers MUST NOT emit empty row batches (batch count of 0).
- Batch row count = number of top-level elements in the CBOR array-of-arrays. If CBOR is decoded, the decoded array length MUST match the batch count; mismatch is a protocol error.
- `RS_DONE` payload is exactly 8 bytes (uint64 row count).
- Server-counted rows = sum of batch row counts from all `RS_ROW` frames in that result set. This count MUST equal the `RS_DONE` row count. Mismatch is a data integrity error. This check MUST NOT be downgraded to a warning or skipped for performance.
- If `RS_ABORT` was received, any buffered rows already forwarded upstream MUST be signalled as incomplete to the client.

**Fixed-size payloads:**

- `HEARTBEAT`: exactly 8 bytes. Any other length is a protocol error.
- `STATUS`: exactly 4 bytes.
- `RS_DONE`: exactly 8 bytes.
- `RS_READY`, `RS_ABORT`, `CMD_CANCEL`: exactly 0 bytes.
- `AUTH_CHALLENGE`: exactly 32 bytes.
- `AUTH_RESPONSE`: exactly 32 bytes.
- Receiver MUST treat any fixed-size mismatch as a malformed frame and fail the session per the general error disposition above.

**Liveness:**

- Worker MUST emit `HEARTBEAT` if no frame sent on either channel for 15 seconds.
- Server timeout clock measures time since last complete TLV frame parsed, not last byte received.
- Server timeout (default 30s) MUST be at least twice the heartbeat interval.
- Any complete frame on either channel resets the server's timeout clock.

**Authentication (when `--worker-auth` is enabled):**

- Worker reads nonce from pipe FD before opening sockets.
- `AUTH_CHALLENGE` MUST precede `EXEC_CMD`.
- Worker MUST reject `EXEC_CMD` received before `AUTH_CHALLENGE`.
- `AUTH_RESPONSE` = HMAC-SHA-256(nonce, challenge). Mismatch terminates session.

**Authentication (when `--worker-auth` is disabled):**

- Server MUST NOT send `AUTH_CHALLENGE`.
- Worker MUST accept `EXEC_CMD` as the first frame on the control channel.

**Session lifecycle:**

- Worker MUST exit after sending `STATUS`.
- One `EXEC_CMD` per session. A second `EXEC_CMD` on the same socket pair is a protocol error.

**Recursive packing:**

- Only the root procedure (named in `EXEC_CMD`) may send `STATUS`.
- Nesting depth MUST NOT exceed the configured limit (default 32).
- Exceeding the nesting limit triggers the abort sequence.
