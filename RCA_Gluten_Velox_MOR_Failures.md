# RCA: Gluten/Velox MOR Test Failures

Three sub-tests fail when Gluten/Velox native execution is enabled:
`testMORWithMap`, `testMORWithDecimal`, `testMORWithArray`.

Each has a distinct root cause. This document traces execution with raw code and supporting log evidence.

---

## 1. `testMORWithMap` — `sizeInBytes (276) should be a multiple of 8`

### Symptom

```
java.lang.AssertionError: sizeInBytes (276) should be a multiple of 8
  at org.apache.spark.sql.catalyst.expressions.UnsafeRow.setTotalSize(UnsafeRow.java:172)
  at org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter.getRow(UnsafeRowWriter.java:78)
  at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
  at org.apache.spark.sql.catalyst.expressions.codegen.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
  at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$...$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
  at HoodieSparkUtils$.$anonfun$createRdd$2(HoodieSparkUtils.scala:107)
```

### Trigger point

`HoodieSparkUtils.createRdd` (`HoodieSparkUtils.scala:107`) calls `rows.isEmpty` on the iterator
returned by `df.queryExecution.toRdd.mapPartitions(...)`.  When Gluten is active, `rows` is a
`WholeStageCodegenPartitionEvaluator`, so `isEmpty` → `hasNext` → `processNext` in WSCG-generated
code, which tries to build a new UnsafeRow from the input.

**Log evidence — plan dump:**
```
[main] WARN  org.apache.hudi.HoodieSparkUtils [] - createRdd executedPlan:
*(1) Scan ExistingRDD[_hoodie_commit_time#206,_hoodie_commit_seqno#207,_hoodie_record_key#208,
     _hoodie_partition_path#209,_hoodie_file_name#210,id#211,col_str#212,col_map#213,ts#214L]
```

The `*(1)` prefix means this scan is inside a `WholeStageCodegenExec` stage. The `ExistingRDD`
contains Gluten/Velox-produced UnsafeRows (from a prior Velox native execution of the Hudi UPDATE
read path, passed back through the Arrow C data interface).

### Why WSCG re-serializes the rows

`RDDScanExec` extends `InputRDDCodegen` with `createUnsafeProjection = true`:

```scala
// ExistingRDD.scala:290-291
// Input can be InternalRow, has to be turned into UnsafeRows.
override protected val createUnsafeProjection: Boolean = true
```

`InputRDDCodegen.doProduce` generates per-column field access expressions (`outputVars`) and calls
`consume(ctx, outputVars, null)` (no row variable):

```scala
// WholeStageCodegenExec.scala:464-485
val outputVars = if (createUnsafeProjection) {
  ctx.INPUT_ROW = row
  ctx.currentVars = null
  output.zipWithIndex.map { case (a, i) =>
    BoundReference(i, a.dataType, a.nullable).genCode(ctx)  // per-column access
  }
}
consume(ctx, outputVars, if (createUnsafeProjection) null else row)
```

`consume()` then calls `prepareRowVar(ctx, null /*row*/, outputVars)` which, because `row == null`
and there are columns, calls:

```scala
// WholeStageCodegenExec.scala:124-138
val ev = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
// ↑ generates UnsafeRowWriter code that writes ALL fields into a new UnsafeRow
```

This generated code reads each field from the input Velox UnsafeRow and writes it into a NEW
UnsafeRow through `UnsafeRowWriter`. This is the re-serialization step.

### Layer 1: Velox produces a 60-byte MAP blob (not 8-aligned)

**Log evidence — Velox HudiMOR-DS output type:**
```
[HudiMOR-DS] next: rowsScanned=4 outputSize=4 outputType=
  ROW<...,col_str:VARCHAR,col_map:MAP<VARCHAR,INTEGER>,ts:BIGINT>
```

Velox serializes `MAP<VARCHAR,INTEGER>` with 1 entry `{"k1": 1}` using `UnsafeRowFast::serializeMap`
(`velox/row/UnsafeRowFast.cpp:272`):

```cpp
// UnsafeRowFast.cpp:267-269  — mapRowSize
return kFieldWidth +                                   //  8 B  (key-array-size header)
    arrayRowSize(children_[0], offset, size, false) +  // 32 B  (key array, STRING)
    arrayRowSize(children_[1], offset, size, true);    // 20 B  (value array, INTEGER)
```

The value array size for a fixed-width type is computed in `arrayRowSize`:

```cpp
// UnsafeRowFast.cpp:305-315
int32_t UnsafeRowFast::arrayRowSize(
    const UnsafeRowFast& elements, vector_size_t offset,
    vector_size_t size, bool fixedWidth) const {
  int32_t nullBytes = alignBits(size);    // alignBits(1) = 8 B
  int32_t rowSize = kFieldWidth + nullBytes;  // 8 + 8 = 16 B
  if (fixedWidth) {
    return rowSize + size * elements.valueBytes();  // 16 + 1*4 = 20 B  ← NOT 8-aligned!
  }
  ...
}
```

Velox's INTEGER is 32-bit (4 bytes). `16 + 1*4 = 20`, which is not a multiple of 8.

Compare with Spark's `UnsafeArrayWriter.initialize` for the same data:
```java
// UnsafeArrayWriter.java (Spark)
long fixedPartInBytesLong =
  ByteArrayMethods.roundNumberOfBytesToNearestWord((long) elementSize * numElements);
// INT(4B), 1 element: roundUp(4) = 8 B → total = 16 + 8 = 24 B  ✓ 8-aligned
```

**MAP byte layout (Velox vs Spark):**

| Part | Velox | Spark |
|------|-------|-------|
| MAP header (key-array size) | 8 B | 8 B |
| Key array (`"k1"`, STRING, 1 element) | 32 B | 32 B |
| Value array (INT=1, 1 element) | **20 B** | **24 B** |
| **Total MAP** | **60 B** | **64 B** |
| 60 % 8 | **4** (bad) | — |

The outer UnsafeRow that Velox produces IS correctly aligned because `serializeRow` uses `alignBytes`:
```cpp
// UnsafeRowFast.cpp:423  — advance outer row's variable-width cursor
variableWidthOffset += alignBytes(size);   // alignBytes(60) = 64 → outer row OK
```
So the outer row passes its own `setTotalSize` assertion; the bad alignment is inside the MAP blob.

### Layer 2: Spark WSCG copies 60 bytes without rounding

`GenerateUnsafeProjection.writeMapToBuffer` (`GenerateUnsafeProjection.scala:227-228`) generates:

```java
// Generated code for col_map (MAP<STRING,INT>)
final MapData tmpInput_7 = col_map_value;
if (tmpInput_7 instanceof UnsafeMapData) {
    rowWriter.write(7, (UnsafeMapData) tmpInput_7);   // ← writeAlignedBytes(60)
} else { ... }
```

`UnsafeWriter.write(int ordinal, UnsafeMapData)` (`UnsafeWriter.java:156-158`):
```java
public final void write(int ordinal, UnsafeMapData map) {
    writeAlignedBytes(ordinal, map.getBaseObject(), map.getBaseOffset(), map.getSizeInBytes());
    //                                                                         ^^^^ = 60
}
```

`writeAlignedBytes` (`UnsafeWriter.java:175-184`) trusts the blob is already padded — it is NOT:
```java
private void writeAlignedBytes(int ordinal, Object baseObject, long baseOffset, int numBytes) {
    grow(numBytes);
    Platform.copyMemory(baseObject, baseOffset, getBuffer(), cursor(), numBytes);
    setOffsetAndSize(ordinal, numBytes);
    increaseCursor(numBytes);   // ← advances by 60 (not 64)!
}
```

Contrast with `writeUnalignedBytes` used for STRING/BINARY, which DOES round:
```java
private void writeUnalignedBytes(..., int numBytes) {
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    ...
    increaseCursor(roundedSize);   // rounded ✓
}
```

### Layer 3: Row total = 276 bytes (4 mod 8) → assertion fails

The UPDATE writes to a 9-field schema: 5 Hudi meta STRING columns + `id` INT + `col_str` STRING +
`col_map` MAP + `ts` LONG. The new UnsafeRow layout:

```
Fixed part:
  null bitmap (9 fields):      8 B
  9 slots × 8 B each:         72 B
                             ──────
                              80 B  (always 8-aligned)

Variable part (written via UnsafeRowWriter cursor):
  _hoodie_commit_time  (17 chars):   roundUp(17) =  24 B  ← writeUnalignedBytes
  _hoodie_commit_seqno (21 chars):   roundUp(21) =  24 B
  _hoodie_record_key   ( 1 char):    roundUp( 1) =   8 B
  _hoodie_partition_path(0 chars):   roundUp( 0) =   0 B
  _hoodie_file_name    (65 chars):   roundUp(65) =  72 B
  col_str  ("s1", 2 chars):          roundUp( 2) =   8 B
  col_map  (Velox MAP blob):         NO rounding = **60 B** ← writeAlignedBytes
                                                  ──────────
                                                   196 B

Grand total = 80 + 196 = 276.   276 % 8 = 4  →  assertion fires.
```

**Log evidence — the assertion:**
```
[Executor task launch worker for task 0.0 in stage 27.0 (TID 45)]
WARN org.apache.spark.storage.BlockManager - Putting block rdd_92_0 failed due to exception
java.lang.AssertionError: sizeInBytes (276) should be a multiple of 8
```

### Root cause summary

| Layer | Location | What happens |
|-------|----------|-------------|
| Velox | `UnsafeRowFast.cpp:314` | `return rowSize + size * elements.valueBytes()` — value-array for INTEGER(4B) with 1 element = **20 B** (not rounded to 8) |
| Velox | `UnsafeRowFast.cpp:267-269` | MAP total = 8 + 32 + **20** = **60 B** (not 8-aligned) |
| Velox | `UnsafeRowFast.cpp:423` | Outer row uses `alignBytes(60)=64` → outer UnsafeRow IS aligned |
| Spark WSCG | `GenerateUnsafeProjection.scala:228` | Generated code: `rowWriter.write(7, (UnsafeMapData) tmpInput)` |
| Spark | `UnsafeWriter.java:157` | `write(UnsafeMapData)` → `writeAlignedBytes(60)` → `increaseCursor(60)` (no rounding) |
| Spark | `UnsafeRow.java:172` | `setTotalSize(276)` → `assert 276 % 8 == 0` **FAILS** |

**Mismatch**: Velox writes INTEGER elements in 4-byte slots without padding. Spark's
`UnsafeArrayWriter` rounds the fixed-width element region to 8 bytes. When Spark's WSCG re-copies
the 60-byte Velox MAP blob via `writeAlignedBytes`, it trusts the blob is already internally
aligned — it is not.

**Fix location**: `UnsafeRowFast.cpp`, `arrayRowSize` and `serializeAsArray` for fixed-width
elements. The data region must be padded to the next 8-byte boundary, matching Spark's
`UnsafeArrayWriter`:
```cpp
// Before (UnsafeRowFast.cpp:314):
return rowSize + size * elements.valueBytes();

// After:
return rowSize + bits::roundUp((int64_t)size * elements.valueBytes(), 8);
```

---

## 2. `testMORWithDecimal` — `type Decimal128(10, 2) not supported`

### Symptom

```
org.apache.gluten.exception.GlutenException: Exception: VeloxRuntimeError
Error Source: RUNTIME
Error Code: INVALID_STATE
Reason: Operator::getOutput failed for [operator: TableScan, plan node ID: 0]:
  Failed to read file slice: Schema error: type Decimal128(10, 2) not supported
```

Stack (abbreviated):
```
Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeHasNext
↑
gluten::ResultIterator::hasNext
↑
gluten::WholeStageResultIterator::next
↑
facebook::velox::exec::Task::next
↑
facebook::velox::exec::Driver::runInternal   [velox/exec/Driver.cpp:666]
```

### Failure point

The failure occurs at test line 448:
```scala
// testMORWithDecimal:448
assertEquals(1, hudiRows("id = 1 AND col_decimal = cast(-99.99 AS DECIMAL(10,2))").length)
```

This is a **read** from the Hudi MOR table immediately after an UPDATE that wrote a log file.
The read goes through Gluten's native Velox execution (TableScan → VeloxHudiMOR reader).

### Execution path

1. `spark.read.format("hudi").load(tablePath).filter(cond).collect()` →
   Gluten plans this as a Velox native scan.
2. Velox's `HudiSplitReader.prepareSplit` calls into **hudi-rs** (Rust):
   ```cpp
   // HudiSplitReader.cpp:107
   arrowStream_ = (*fileGroupReader_)->read_file_slice(**fileSlice_);
   ```
3. hudi-rs reads the base Parquet file + AVRO delta log file, applies MOR merge,
   and returns an Arrow C Data Interface stream.
4. Inside the MOR merge, hudi-rs encounters the `DECIMAL(10, 2)` column. While Spark
   represents it internally as a 64-bit `Decimal` (precision ≤ 18 fits in a `long`),
   the Arrow representation used by hudi-rs is **Decimal128** (Arrow's only decimal type).
5. hudi-rs throws: `Schema error: type Decimal128(10, 2) not supported` — this Decimal128
   Arrow type is not handled in this version of the Velox Hudi connector's schema mapping code.
6. The error propagates through the Arrow stream `get_next` call back to Velox's
   `Driver::runInternal`, which wraps it as a `VeloxRuntimeError`, re-thrown as `GlutenException`.

### Root cause

Velox's Hudi connector does not map Arrow's `Decimal128` type to a Velox type when importing the
Arrow batch from hudi-rs. The error message `Schema error: type Decimal128(10, 2) not supported`
is generated in Arrow's schema compatibility layer when Velox calls `importFromArrowAsOwner`:

```cpp
// HudiSplitReader.cpp:179
auto fullRowVector = importFromArrowAsOwner(arrowSchema, arrowArray, pool_);
```

The import fails because the Arrow schema contains `Decimal128(10, 2)` and the Velox type registry
for the Hudi connector does not include a mapping for this type.

**Fix location**: Add Decimal128→DECIMAL(p,s) type mapping in Velox's Arrow import path for
the Hudi connector, or ensure hudi-rs exports decimal columns using a representation compatible
with Velox's supported types (e.g., mapping precision ≤ 18 to `Decimal64` / `ShortDecimal`).

---

## 3. `testMORWithArray` — Arrow array concatenation schema mismatch

### Symptom

```
org.apache.gluten.exception.GlutenException: Exception: VeloxRuntimeError
Error Source: RUNTIME
Error Code: INVALID_STATE
Reason: Operator::getOutput failed for [operator: TableScan, plan node ID: 0]:
  Failed to read file slice: Invalid argument error: It is not possible to concatenate arrays
  of different data types (List(non-null Utf8, field: 'array'), List(Utf8, field: 'element')).
```

### Failure point

The failure occurs at test line 510:
```scala
// testMORWithArray:510
assertEquals(1, hudiRows("id = 1 AND col_array[0] = 'x' AND col_array[1] = 'y'").length)
```

This reads the MOR table after an UPDATE (`SET col_array = array('x', 'y', 'z')`). The table now
has a base Parquet file (original data) and a delta log file (the UPDATE's log). Reading this MOR
slice requires merging base + log.

### Root cause: two different Arrow List schemas

The error message names two Arrow schemas for the `col_array` column:
- **Base Parquet file**: `List(non-null Utf8, field: 'array')` — Parquet **legacy** list encoding.
  When Spark writes `ArrayType(StringType, containsNull=true)` to Parquet without the modern
  3-level list encoding, the element field is written as:
  ```
  required group array {
    required binary array (UTF8);   ← element name = "array", non-null
  }
  ```
- **AVRO log file**: `List(Utf8, field: 'element')` — Hudi's AVRO log writer uses the standard
  AVRO array convention where the element field is named `"element"` and is nullable.

When hudi-rs performs the MOR merge, it attempts to `concat` the Arrow arrays from the base file
and the matching log records. Arrow's `Concatenate` validates that all input arrays have identical
schemas, and it rejects the combination because:
1. Element nullability differs: `non-null Utf8` (base) vs `Utf8` (log)
2. Element field name differs: `'array'` (legacy Parquet) vs `'element'` (AVRO convention)

**Fix location**: hudi-rs's MOR merge must normalise the element field name and nullability of
list columns before concatenation, or the Parquet/AVRO schema should be written consistently.
Alternatively, Velox's Arrow import can coerce mismatched list schemas during merge.

---

## Summary table

| Test | Error | Failure point | Root cause |
|------|-------|---------------|------------|
| `testMORWithMap` | `sizeInBytes (276) should be a multiple of 8` | `HoodieSparkUtils.createRdd:107` — WSCG `processNext` | Velox `UnsafeRowFast::arrayRowSize` returns 20 B for INTEGER[1] (not rounded to 24 B); Spark WSCG copies the 60-byte MAP blob via `writeAlignedBytes` without rounding |
| `testMORWithDecimal` | `type Decimal128(10, 2) not supported` | Velox `TableScan` operator during MOR read | Velox Hudi connector cannot import Arrow `Decimal128` type from hudi-rs |
| `testMORWithArray` | `Cannot concatenate arrays of different types` | Velox `TableScan` operator during MOR read | hudi-rs base Parquet uses legacy list schema (`field:'array'`, non-null) while AVRO log uses modern schema (`field:'element'`, nullable); Arrow rejects concatenation |
