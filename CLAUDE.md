# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ladybug (formerly Kuzu) is an **embedded graph database** written in C++20 with multi-language bindings. It uses a **columnar disk-based storage** engine with CSR adjacency/join indices, a vectorized/factorized query processor, Cypher query language support, native full-text search, vector indices, and serializable ACID transactions.

## Build System

CMake with a `Makefile` frontend. **Prefer Ninja** as the generator. The C++20 standard is required.

```bash
# Fastest: release build
make release

# Recommended for development: optimized + debug symbols
make relwithdebinfo

# Debug build (assertions, no optimization)
make debug

# Build everything (all language bindings, extensions, tests)
make all

# Language-specific builds
make python          # Python bindings
make java            # Java bindings
make nodejs          # Node.js bindings
make shell           # CLI shell

# Extensions
make extension-build              # Build all extensions
make extension-release            # Release build with extensions
make extension-debug              # Debug build with extensions
```

Sanitizer-enabled builds: `make debug ASAN=1`, `make debug TSAN=1`, `make debug UBSAN=1`.

Runtime checks (extra asserts): `make debug RUNTIME_CHECKS=1`.

Key cmake options: `PAGE_SIZE_LOG2` (default 12), `VECTOR_CAPACITY_LOG2` (default 11), `NODE_GROUP_SIZE_LOG2` (default 17), `SINGLE_THREADED` for single-threaded mode.

## Testing

```bash
# Build and run all C++ tests (RelWithDebInfo, 10 parallel jobs)
make test

# Build tests only
make test-build

# Run a single e2e test with gtest filter
make test-build
E2E_TEST_FILES_DIRECTORY=test/test_files build/relwithdebinfo/test/runner/e2e_test \
  --gtest_filter="*merge_tinysnb.Merge*"

# Language binding tests
make pytest         # Python tests
make javatest       # Java tests
make nodejstest     # Node.js tests (from tools/nodejs_api: `npm test`)
make rusttest       # Rust tests (from tools/rust_api: `cargo test --release`)
make wasmtest       # WASM tests

# Extension tests
make extension-test
```

Test categories in `test/`: `runner/` (e2e), `storage/`, `transaction/`, `api/`, `c_api/`, `binder/`, `planner/`, `optimizer/`, `common/`, `copy/`.

E2E tests are in `test/test_files/` — each subdirectory contains Cypher `.cypher` statements and expected `.txt` outputs.

## Code Style

- **clang-format-18** required for formatting: `python3 scripts/run-clang-format.py --clang-format-executable /usr/bin/clang-format-18 -r <dirs>`
- **clang-tidy** for linting: `make tidy` or `make tidy-analyzer`
- Naming: `PascalCase` classes, `camelCase` functions/variables, `UPPER_SNAKE_CASE` macros, no `m_` prefix for members
- Namespaces: `lbug` as root, then module name (`lbug::main`, `lbug::storage`, etc.)
- Assertions: `DASSERT` (debug), `ASSERT` (always), `UNREACHABLE_CODE`, `UNUSED(expr)`
- Headers under `src/include/<module>/`, sources under `src/<module>/`
- Include order enforced by clang-format: C system → C++ system → third-party `<>` → project `"src/` → local `"`

## High-Level Architecture

The query execution follows a classic **parse → bind → plan → optimize → execute** pipeline:

### Core Pipeline

1. **Parser** (`src/parser/`): ANTLR4-based Cypher parser. Lexer/grammar files in `third_party/antlr4_cypher/`. Generates a parse tree.

2. **Binder** (`src/binder/`): Resolves names against the catalog (tables, columns, functions, types). Produces bound statements with resolved types and catalog references. Entry: `src/binder/bind/`.

3. **Planner** (`src/planner/`): Converts bound statements into a logical plan tree (`src/planner/operator/`, `src/planner/plan/`). Handles join order enumeration in `src/planner/join_order/`.

4. **Optimizer** (`src/optimizer/`): Rewrites logical plans — filter push-down, projection push-down, factorization rewriting, correlated subquery unnesting, limit/top-k push-down, unnecessary join removal, etc.

5. **Processor** (`src/processor/`): Compiles logical plans into a physical operator DAG. Operators live in `src/processor/operator/`. Executes via a push-based task system with morsel-driven parallelism (`src/common/task_system/`).

### Storage Engine (`src/storage/`)

- **Columnar disk storage**: Tables stored as column chunks. Pages managed by `buffer_manager/` and `page_manager/`.
- **CSR indices**: Sparse columnar adjacency lists for fast relationship traversal.
- **WAL**: Write-ahead log for recovery (`src/storage/wal/`).
- **Compression**: Multiple codec support in `src/storage/compression/`.
- **Undo buffer**: MVCC-style transaction isolation via `src/storage/undo_buffer.h`.
- **Checkpointer**: Checkpoint management in `src/storage/checkpointer.h`.

### Catalog (`src/catalog/`)

Tracks all database metadata: tables, columns, relationships, functions, sequences, extensions. Entries defined in `src/catalog/catalog_entry/`.

### Transaction (`src/transaction/`)

Serializable ACID transactions with MVCC. Read/write transaction types. Automatic rollback on failure.

### Extensions

Extension system in `extension/` for DuckDB, PostgreSQL, SQLite, HTTP filesystem, FTS, Delta Lake, Iceberg, JSON, Azure, Unity Catalog, vector search, Neo4j, algorithms, LLM integration. Built as loadable shared libraries.

### Language Bindings (`tools/`)

- **C API** (`src/c_api/`): Stable C ABI used by all language bindings
- **Python** (`tools/python_api/`): pybind11-based, lives inside `src/` as a compiled target
- **Node.js** (`tools/nodejs_api/`): N-API bindings
- **Java** (`tools/java_api/`): JNI bindings via Gradle
- **Rust** (`tools/rust_api/`): FFI bindings
- **WASM** (`tools/wasm/`): Emscripten-compiled for browser

### Key Public API (one header)

`src/include/main/lbug.h` — the single include that exports all public types: `Database`, `Connection`, `PreparedStatement`, `QueryResult`, `QuerySummary`, and all value types (`Value`, `Node`, `Rel`, etc.).

## Important Directories

| Directory | Purpose |
|---|---|
| `third_party/` | Vendored dependencies (ANTLR4, brotli, zstd, re2, mbedtls, utf8proc, etc.) |
| `scripts/` | Build/CI scripts, grammar generation |
| `benchmark/` | Performance benchmarks |
| `examples/` | C and C++ usage examples |
| `docs/` | Developer documentation, incident reports |
| `.github/` | CI workflows |
