# aul Documentation Index

**Version:** 0.6.1  
**Last updated:** January 2026

---

## Documents

| # | Document | Description | Status |
|---|----------|-------------|--------|
| 001 | [Stored Procedure Architecture](001-STORED_PROCEDURE_ARCHITECTURE.md) | Execution architecture, code paths, dialect handling | Current |
| 002 | [Storage and Translation](002-PROCEDURE_STORAGE_AND_TRANSLATION.md) | Storage hierarchy, translation, delegation, tenancy, ACL | Current |
| 003 | [Development Plan](003-STORED_PROCEDURE_DEVELOPMENT_PLAN.md) | Phased roadmap: Core → Tenancy → Annotations → Delegation → JIT → ACL | Active |
| 004 | [JIT Architecture](004-JIT_ARCHITECTURE.md) | JIT pipeline design and implementation fixes | Implemented |
| 005 | [TDS Implementation](005-TDS_IMPLEMENTATION.md) | TDS protocol implementation details | Reference |
| 006 | [Dialect Inventory](006-DIALECT_INVENTORY.md) | T-SQL function/feature translation by dialect | Reference |
| 007 | [T-SQL Compatibility](007-TSQL_COMPATIBILITY.md) | Test results for T-SQL compatibility | v0.4.8 |
| 008 | [Phase 3 Architecture](008-PHASE3_ARCHITECTURE.md) | Historical: Phase 3 design (storage, protocols) | Archive |
| 009 | [Annotations](009-ANNOTATIONS.md) | Annotation system (`-- @aul:`), isolated table storage | Current |
| 010 | [Benchmarks](010-BENCHMARKS.md) | Performance benchmarks and comparison methodology | Current |
| 011 | [System Catalog](011-SYSTEM_CATALOG.md) | SQL Server-compatible system views (sys.tables, etc.) | Current |

---

## Document Relationships

```
003-STORED_PROCEDURE_DEVELOPMENT_PLAN  (roadmap)
            │
            ├── Phase 1:   001-STORED_PROCEDURE_ARCHITECTURE (execution)
            │              002-PROCEDURE_STORAGE_AND_TRANSLATION (storage)
            │              *** COMPLETE in v0.5.0 ***
            │
            ├── Phase 2:   002-PROCEDURE_STORAGE_AND_TRANSLATION (tenancy)
            │              *** COMPLETE in v0.5.0 ***
            │
            ├── Phase 2.5: 009-ANNOTATIONS (annotation system, isolated tables)
            │              *** COMPLETE in v0.6.0 ***
            │
            ├── Phase 3:   002-PROCEDURE_STORAGE_AND_TRANSLATION (delegation)
            │
            ├── Phase 4:   004-JIT_ARCHITECTURE (JIT implementation)
            │
            └── Phase 5:   002-PROCEDURE_STORAGE_AND_TRANSLATION (ACL hooks)

Supporting:
    006-DIALECT_INVENTORY ──► Translation layer reference
    007-TSQL_COMPATIBILITY ──► Test coverage tracking
    005-TDS_IMPLEMENTATION ──► Protocol details
```

---

## Current Implementation Status

| Feature | Status | Document |
|---------|--------|----------|
| T-SQL parsing | ✓ Complete | — |
| SQLite backend | ✓ Complete | 001 |
| TDS protocol | ✓ Complete | 005 |
| Dialect rewriting | ✓ Complete | 006 |
| Simple procedure execution | ✓ Complete | 001 |
| JIT pipeline | ✓ Complete | 004 |
| Hierarchical storage | ✓ Complete | 002 |
| Schema validation | ✓ Complete | 003 |
| Nested EXEC support | ✓ Complete | 003 |
| Output parameters | ✓ Complete | 003 |
| Hot reload | ✓ Complete | 003 |
| Multi-tenancy | ✓ Complete | 002, 003 |
| Annotation system | ✓ Complete | 003, 009 |
| Isolated tables | ✓ Complete | 003, 009 |
| Query routing | ✓ Complete | 003 |
| SELECT @var = col | ✓ Complete | — |
| Transactions (COMMIT/ROLLBACK) | ✓ Complete | — |
| Complex stored procedures (DDL/DML) | ✓ Complete | — |
| Delegation optimisation | Planned | 002 |
| PostgreSQL backend | Planned | 003 |
| ACL hooks | Planned | 002 |

---

## Key Design Decisions

1. **Source of truth:** Procedures stored as files, not in database catalogs
2. **Execution priority:** Delegated > JIT > Interpreted
3. **Delegation:** Automatic, invisible optimisation owned by aul
4. **Tenancy:** Hierarchical directories with `_tenant/` overrides
5. **JIT ABI:** Shared `jit/abi` package for plugin type compatibility

---

## Updating Documentation

When making changes:

1. Update the relevant document
2. Update status in this index
3. For new documents, assign next number (e.g., 012-*)
4. Keep version numbers synchronised with aul version
