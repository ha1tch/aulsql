# aul Benchmark Results

**Version:** 0.6.1  
**Date:** January 2026  
**Platform:** Linux, Intel Xeon Platinum 8581C @ 2.10GHz  
**Backend:** SQLite (in-memory)

---

## Summary

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| SimpleSelect | 230,000 | 149 KB | 1,406 |
| SelectWithParams | 249,000 | 154 KB | 1,475 |
| Arithmetic | 241,000 | 155 KB | 1,490 |
| ControlFlow (IF/ELSE) | 249,000 | 156 KB | 1,518 |
| WhileLoop (10 iter) | 248,000 | 154 KB | 1,527 |
| WhileLoop (100 iter) | 331,000 | 162 KB | 1,980 |
| WhileLoop (1000 iter) | 1,232,000 | 242 KB | 6,481 |
| TableInsert | 285,000 | 154 KB | 1,471 |
| TableSelect (100 rows) | 463,000 | 234 KB | 2,540 |
| TableSelectAggregate (10K rows) | 3,115,000 | 176 KB | 1,638 |
| TempTable (100 rows) | 285,000 | 161 KB | 1,646 |
| NestedExec (1 level) | 525,000 | 302 KB | 2,858 |
| NestedExec (3 levels) | 1,047,000 | 602 KB | 5,703 |
| NestedExec (5 levels) | 1,641,000 | 905 KB | 8,545 |
| Transaction | 884,000 | 395 KB | 3,730 |
| ComplexProcedure | 1,259,000 | 534 KB | 5,023 |
| MultipleResultSets | 742,000 | 397 KB | 3,703 |
| StringOperations | 291,000 | 160 KB | 1,549 |
| CaseExpression | 458,000 | 167 KB | 1,525 |

---

## Analysis

### Throughput

| Operation Type | ops/sec (approx) |
|----------------|------------------|
| Simple SELECT | ~4,300 |
| SELECT with params | ~4,000 |
| Table INSERT | ~3,500 |
| Table SELECT (100 rows) | ~2,200 |
| Complex procedure | ~800 |
| Transaction (INSERT+UPDATE+SELECT) | ~1,100 |

### Scaling

**WHILE loop scaling:**
- 10 iterations: 248 µs
- 100 iterations: 331 µs (+33%)
- 1000 iterations: 1,232 µs (+372%)

Per-iteration overhead: ~1 µs

**Nested EXEC scaling:**
- 1 level: 525 µs
- 3 levels: 1,047 µs (~350 µs per additional level)
- 5 levels: 1,641 µs (~280 µs per additional level)

### Memory

Base overhead per procedure call: ~150 KB, ~1,400 allocations

This is dominated by:
- AST parsing and allocation
- Interpreter context setup
- Result set construction

---

## SQL Server Performance Reference Data

The following data was gathered from Microsoft documentation, HammerDB benchmarks, and industry sources to provide context for comparing aul performance.

### SQL Server Execution Characteristics

**Timing resolution:** SQL Server's `sys.dm_exec_procedure_stats` reports elapsed time in microseconds, but values under 1 millisecond are reported as 0. This means SQL Server's instrumentation cannot measure sub-millisecond procedure executions with precision.

**Plan caching advantage:** SQL Server compiles procedures once and caches the execution plan. On subsequent executions, compilation time is zero. This is a significant advantage over aul, which currently parses the AST on every execution.

**Typical OLTP latencies:**
- Simple cached procedure (plan in cache): Sub-millisecond to low single-digit ms
- First execution (compilation required): 10-50+ ms depending on complexity
- Natively compiled stored procedures (In-Memory OLTP): Can achieve 1/100th of interpreted execution time

### In-Memory OLTP Benchmarks (from Microsoft)

Microsoft's In-Memory OLTP documentation claims:
- 5-20x faster than disk-based tables for typical workloads
- Up to 99x faster for computation-heavy natively compiled stored procedures
- Sub-millisecond latency achievable for memory-optimized tables

These numbers represent the best-case scenario with memory-optimized tables and natively compiled procedures, which compile T-SQL to native machine code DLLs.

### HammerDB TPROC-C Reference Points

HammerDB is the industry-standard open-source database benchmark. Key reference points:

| Configuration | NOPM (New Orders/Min) | Notes |
|---------------|----------------------:|-------|
| Desktop (24 cores, 2024) | ~1,000,000 | SQL Server, performance cores only |
| AWS RDS SQL Server | Varies by instance | Typical tests show thousands to tens of thousands |
| Enterprise systems | 10,000,000+ | High-end hardware, expert tuning |

**Response time expectations (HammerDB guidance):**
- "Good" performance: Low single-digit to tens of milliseconds per transaction
- Problem indicator: Hundreds or thousands of milliseconds

### Triangulated Comparison

| Metric | aul v0.6.0 | SQL Server (interpreted) | SQL Server (In-Memory OLTP) |
|--------|------------|--------------------------|----------------------------|
| Simple SELECT | ~230 µs | Sub-ms to 1-2 ms (cached) | Sub-100 µs |
| Complex procedure | ~1.3 ms | 1-10 ms (typical) | Sub-ms |
| INSERT (single row) | ~285 µs | Sub-ms (cached) | Sub-100 µs |
| Parse overhead | Every call | First call only | Compiled to DLL |
| Plan caching | None | Yes | N/A (native code) |

### Key Differences Affecting Comparison

1. **Network round-trip:** aul benchmarks are in-process (no network). SQL Server typically adds 0.1-5ms network latency per call depending on topology.

2. **Connection pooling:** SQL Server clients use connection pools; establishing a new connection costs 10-100+ ms. aul benchmarks don't include connection overhead.

3. **Compilation:** SQL Server compiles once, executes many times. aul parses the AST on every execution. This is the primary performance gap for repeated procedure calls.

4. **Storage backend:** aul uses SQLite (in-memory). SQL Server uses a sophisticated buffer pool, log-based recovery, and MVCC. SQLite is simpler but lacks advanced optimisation.

5. **Concurrency:** aul benchmarks are single-threaded. SQL Server excels at concurrent workloads with its latch-free in-memory engine and parallel query execution.

### Interpretation

**Where aul is competitive:**
- Single-call latency for simple procedures is in the same order of magnitude as SQL Server
- In-process execution eliminates network latency
- SQLite backend is fast for small to medium datasets

**Where SQL Server wins:**
- Plan caching means repeated executions are much faster
- In-Memory OLTP with native compilation is 10-100x faster for hot paths
- Concurrent workloads scale better with SQL Server's threading model
- Large datasets benefit from SQL Server's query optimiser

**Bottom line:**
aul at ~230 µs for a simple SELECT is within 10x of a cached SQL Server procedure call, and competitive with a first-execution (uncached) call. For workloads where:
- Network latency dominates (remote SQL Server)
- Procedures are not called repeatedly (no plan cache benefit)
- Dataset fits in memory
- Licensing cost matters

...aul can be a viable alternative.

---

## Running Benchmarks

```bash
cd /path/to/aul
make bench
# or
CGO_ENABLED=1 CGO_LDFLAGS=-lm go test -bench=. -benchmem ./runtime/...
```

For specific benchmarks:
```bash
CGO_ENABLED=1 CGO_LDFLAGS=-lm go test -bench=Complex -benchmem ./runtime/...
```

With CPU profiling:
```bash
CGO_ENABLED=1 CGO_LDFLAGS=-lm go test -bench=Complex -cpuprofile=cpu.prof ./runtime/...
go tool pprof cpu.prof
```

---

## In-Memory vs Disk (WAL) Comparison

Benchmarks were also run with SQLite configured for filesystem storage with WAL journaling.

### Configuration

| Setting | Value |
|---------|-------|
| Journal mode | WAL |
| Synchronous | NORMAL |
| Cache size | 64 MB |
| Storage | Temp directory file |

### Results

| Benchmark | In-Memory (µs) | Disk/WAL (µs) | Overhead |
|-----------|---------------:|---------------:|---------:|
| SimpleSelect | 241 | 293 | +22% |
| SelectWithParams | 290 | 315 | +9% |
| WhileLoop (100) | 405 | 350 | -14% |
| TableInsert | 286 | 858 | **+200%** |
| TableSelect (100 rows) | 497 | 713 | +43% |
| TableSelectAggregate (10K) | 3,456 | 3,299 | -5% |
| TempTable | 345 | 323 | -6% |
| Transaction | 1,170 | 1,445 | +24% |
| ComplexProcedure | 1,386 | 1,679 | +21% |

### Analysis

**Write-heavy operations see the biggest impact:**
- TableInsert: 3x slower (286 µs → 858 µs) due to WAL writes and fsync
- Transaction: ~24% slower due to commit overhead

**Read-heavy and compute-heavy operations are similar:**
- TableSelectAggregate: Nearly identical (data fits in cache)
- WhileLoop: Nearly identical (no I/O involved)
- TempTable: Nearly identical (temp tables are memory-based)

**Simple queries have modest overhead:**
- SimpleSelect: ~22% slower (connection/file overhead)
- SelectWithParams: ~9% slower

**Implications:**
- For read-heavy workloads, disk storage adds minimal overhead once data is cached
- For write-heavy workloads, consider batching or async writes
- WAL mode with NORMAL synchronous provides a good balance of durability and performance

---

## Future Optimisations

The following could significantly improve aul performance:

1. **AST caching:** Parse procedures once, cache the AST. This would eliminate ~50% of per-call overhead.

2. **Prepared statement caching:** Cache SQLite prepared statements for repeated queries within procedures.

3. **JIT compilation:** The existing JIT infrastructure (Phase 4) could compile hot procedures to Go plugins, approaching native speed.

4. **Delegation:** Phase 3 delegation would push suitable procedures to native PostgreSQL/MySQL execution.

5. **Reduce allocations:** The ~1,400 allocations per call suggest opportunities for object pooling and arena allocation.
