# Checkpoint Bug: INTEGER_BITPACKING is not implemented for type DOUBLE

## Symptom

Executing `CHECKPOINT;` throws:

```
Error: INTEGER_BITPACKING is not implemented for type DOUBLE
```

This occurs on databases where at least one table has had a property dropped
(`ALTER TABLE ... DROP ...`) without an intervening checkpoint.

## Root Cause

### Background: `state.columns` uses position-based indexing

`NodeTable::checkpoint` builds `state.columns` (a `std::vector<Column*>`) and
`state.columnIDs` (a `std::vector<column_id_t>`) together, in lock-step, by
iterating `tableEntry->getProperties()`:

```cpp
// node_table.cpp
for (auto& property : tableEntry->getProperties()) {
    auto columnID = tableEntry->getColumnID(property.getName());
    columnIDs.push_back(columnID);
    checkpointColumnPtrs.push_back(columns[columnID].get());
}
```

The invariant is: `state.columns[i]` is the `Column*` for `state.columnIDs[i]`.
`state.columns` is a **dense, position-based** vector (indices 0..N-1), not a
sparse vector indexed by column ID.

### The bug: wrong index in `checkpointInMemAndOnDisk`

`NodeGroup::checkpointInMemAndOnDisk` iterates over `state.columnIDs` with a
position variable `i` and a column ID variable `columnID = state.columnIDs[i]`.
These two values are equal only when column IDs are contiguous (no gaps).

After a `DROP` without a checkpoint, column IDs have gaps. For example, after
dropping the property with `columnID=2` from a 5-column table, the remaining
column IDs are `[0, 1, 3, 4]`. Then for `i=2`, `columnID=3`, and these diverge.

The buggy call used `columnID` as the index into `state.columns`:

```cpp
// node_group.cpp — BEFORE FIX
if (columnHasUpdates) {
    scanCommittedUpdatesForColumn(chunkCheckpointStates, memoryManager, lock, columnID,
        state.columns[columnID], txn);  // ← wrong: columnID=3, but state.columns[3]
                                        //   is the Column for colID=4, not colID=3
}
```

A few lines later, the same loop correctly uses `i`:

```cpp
firstGroup->getColumnChunk(columnID).checkpoint(*state.columns[i], ...);
//                                                             ^^^^ correct
```

### Why this produces the specific error

`scanCommittedUpdatesForColumn` receives the wrong `Column*` — the one for the
**next** property (e.g., a DOUBLE column) instead of the current property (e.g.,
an INT64 column). It creates a `LazySegmentScanner` with
`column->getDataType()` (DOUBLE), then attempts to write scanned values back
to the on-disk page using `WriteCompressedValuesToPage`:

```
physicalType = DOUBLE          (from the wrong Column object)
metadata.compression = INTEGER_BITPACKING  (from the actual INT64 chunk on disk)
```

`WriteCompressedValuesToPage::operator()` reaches the `INTEGER_BITPACKING`
branch and calls `TypeUtils::visit` with `physicalType=DOUBLE`. Since DOUBLE
does not satisfy `numeric_utils::IsIntegral`, the `else` branch throws:

```cpp
throw NotImplementedException(
    "INTEGER_BITPACKING is not implemented for type " +
    PhysicalTypeUtils::toString(physicalType));  // → "DOUBLE"
```

## Trigger Conditions

1. A table has had `ALTER TABLE ... DROP PROPERTY ...` executed on it.
2. No `CHECKPOINT` was run after the drop (which would have called
   `vacuumColumnIDs(0)` and collapsed the column ID gaps).
3. The property immediately following the dropped column has on-disk committed
   updates at checkpoint time (`columnHasUpdates == true`).

## Fix

**File:** `src/storage/table/node_group.cpp`  
**Function:** `NodeGroup::checkpointInMemAndOnDisk`

Change `state.columns[columnID]` to `state.columns[i]` so both usages in the
loop are consistently position-indexed:

```cpp
// BEFORE
if (columnHasUpdates) {
    scanCommittedUpdatesForColumn(chunkCheckpointStates, memoryManager, lock, columnID,
        state.columns[columnID], txn);
}

// AFTER
if (columnHasUpdates) {
    scanCommittedUpdatesForColumn(chunkCheckpointStates, memoryManager, lock, columnID,
        state.columns[i], txn);
}
```

## Related

- `docs/checkpoint-segfault-root-cause.md` — an earlier crash in the same
  checkpoint path caused by moving `columns` before `nodeGroups->checkpoint()`
  succeeded. That fix deferred the destructive move, which is what allows
  this second bug to become observable: the checkpoint now proceeds further
  and reaches `scanCommittedUpdatesForColumn`.
