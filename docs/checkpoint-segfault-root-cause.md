# Checkpoint Segfault Root Cause Analysis

## Symptom

Opening a database with uncommitted WAL records (e.g., `ALTER TABLE ADD COLUMN`) and
executing `checkpoint` leads to a segfault (SIGSEGV, exit code 139). The crash occurs
in `Database::~Database` when `forceCheckpointOnClose` triggers a second checkpoint.

Reproducer:
```
printf "checkpoint;\n" | lbug /path/to/db_with_pending_wal.lbug
# Exit code 139 (SIGSEGV)
```

## Crash Location

```
EXC_BAD_ACCESS (code=1, address=0x20)

frame #0: LogicalType::LogicalType(other=0x20)           // copying from invalid pointer
frame #3: NodeGroup::scanAllInsertedAndVersions()         // node_group.cpp:663
frame #4: NodeGroup::checkpointInMemAndOnDisk()           // node_group.cpp:467
frame #5: NodeGroup::checkpoint()                         // node_group.cpp:414
frame #6: NodeGroupCollection::checkpoint()               // node_group_collection.cpp:207
frame #7: NodeTable::checkpoint()                         // node_table.cpp:675
  ...
frame #13: Database::~Database()                          // database.cpp:147
```

## Root Cause

`NodeTable::checkpoint()` is **not exception-safe**. It performs a destructive column
move **before** the operation that can fail, leaving the table in an inconsistent state
when the failure is caught and retried.

### Detailed Flow

**Step 1: Database opens with pending WAL**

WAL replay executes `addColumn` on a table, setting `hasChanges = true`.
The table now has 18 columns (indices 0-17, with one column being newly added via WAL replay).

**Step 2: First checkpoint executes (user command)**

In `NodeTable::checkpoint()` (node_table.cpp:654-683):

```cpp
if (hasChanges) {
    // (A) DESTRUCTIVE: Move columns based on catalog properties.
    //     Vacuums dropped columns. 18 columns → 17 columns.
    for (auto& property : tableEntry->getProperties()) {
        auto columnID = tableEntry->getColumnID(property.getName());
        checkpointColumns.push_back(std::move(columns[columnID]));
    }
    columns = std::move(checkpointColumns);  // columns is now size 17

    // (B) Build raw pointers from the 17 columns
    std::vector<Column*> checkpointColumnPtrs;
    for (const auto& column : columns) {
        checkpointColumnPtrs.push_back(column.get());
    }

    // (C) THIS FAILS — exception or signal caught by query pipeline
    nodeGroups->checkpoint(*memoryManager, state);

    // (D) NEVER REACHED — columnIDs not vacuumed, hasChanges not reset
    tableEntry->vacuumColumnIDs(0);
    hasChanges = false;                      // ← never executed
}
```

Step (C) fails because of a data integrity issue (nested type column with
uninitialized `childrenStates`). The failure is caught by the query pipeline's
exception handling, converting it to a query error rather than a process crash.

**After the first checkpoint fails:**
- `columns` has been irreversibly moved: now 17 elements (indices 0-16)
- `tableEntry` column IDs are **not** vacuumed: still the original IDs (e.g., 0-17 with gaps)
- `hasChanges` is still `true`

**Step 3: Second checkpoint executes (`Database::~Database`)**

The destructor's `forceCheckpointOnClose` calls checkpoint again. Since `hasChanges`
is still `true`, `NodeTable::checkpoint` enters the `if` block again:

```cpp
auto columnID = tableEntry->getColumnID(property.getName());
// columnID could be 17 (original, non-vacuumed ID)
checkpointColumns.push_back(std::move(columns[columnID]));
// columns[17] is OUT OF BOUNDS on a 17-element vector!
```

This out-of-bounds access reads garbage memory, producing an invalid `unique_ptr`.
Later, `column.get()` returns a garbage pointer (e.g., `0x20`), and
`column->getDataType().copy()` dereferences it → **SIGSEGV**.

## Why New Databases Don't Crash

New databases have no pending WAL records with `ALTER TABLE ADD COLUMN`,
so no table has `hasChanges = true` at open time. Checkpoint has nothing to vacuum,
so the destructive move path is never entered.

## Fix

Defer the destructive column move until **after** `nodeGroups->checkpoint()` succeeds.
The raw Column pointers needed for checkpoint are the same regardless of whether the
`unique_ptr` ownership has been transferred. This makes `NodeTable::checkpoint()`
exception-safe: if `nodeGroups->checkpoint()` fails, `columns` and `hasChanges` remain
in their original state, and a retry (or the destructor's checkpoint) will operate
on consistent data.

```cpp
// Before: move columns first, then checkpoint (not exception-safe)
// After:  checkpoint first with raw pointers, then move columns
```

## Related Observations

- The DASSERT `childIdx < childrenStates.size()` (column_chunk_data.h:71) indicates
  a separate data integrity issue with nested type columns after WAL replay. This is
  a pre-existing issue in the specific database and should be investigated separately.
- The `return` fix in `TransactionManager::Get()` (adding the missing `return` for
  `getAttachedDatabase()->getTransactionManager()`) is correct but unrelated to this crash.
