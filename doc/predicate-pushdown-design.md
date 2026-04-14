# Expression and Predicate Pushdown Design

## Overview

Several format readers and writers in EngineeredWood have an overlapping need for
expressions: trees of typed values, column references, comparisons, and boolean
combinators. The current and planned uses are:

| Use case | Format | Purpose |
|---|---|---|
| Manifest evaluation | Iceberg | Skip data files using min/max stats per field ID |
| Row group pruning | Parquet | Skip row groups using min/max stats and bloom filters |
| Stripe pruning | ORC | Skip stripes using stripe statistics (future) |
| Partition pruning | Delta Lake | Skip files whose partition values can't satisfy a filter |
| Stats-based file pruning | Delta Lake | Skip files using `AddFile.stats` min/max |
| CHECK constraints | Delta Lake | Validate every row satisfies a boolean expression on write |
| Generated columns | Delta Lake | Compute a column's value from an expression on write |
| Post-filter | Any reader | Drop rows that survived coarse pruning but don't match |

Today, only Iceberg has an expression tree (mature, schema-bound, with three-valued
statistics evaluation). Parquet has a design but no implementation. ORC, Delta, and
the row-level evaluator have nothing.

This document covers the architecture for unifying these needs across formats and
the implementation phases to get there.

## Architecture

Two evaluation models share a single expression tree:

- **Statistics-based pruning** is coarse and three-valued (`MightMatch` /
  `CannotMatch` / `Unknown`). It runs against aggregated stats — min/max, null
  count, distinct count — for a file, row group, or stripe. No row data is read.
- **Row-level evaluation** is exact and two-valued. It runs against an Arrow
  `RecordBatch` and produces a `BooleanArray`, one bit per row. Used by CHECK
  constraints, generated columns, and post-filtering.

Both consume the same `Expression` tree. The split falls on a clean dependency
boundary: stats evaluation only needs typed value comparison; row evaluation
needs Arrow.

### Project layout

```
EngineeredWood.Expressions              (new — no Arrow, no format deps)
    │
    ├── Expression tree types
    ├── LiteralValue (typed scalar with cross-type promotion)
    ├── IStatisticsEvaluator<TStats> + generic three-valued logic
    └── ExpressionBinder (resolve unbound name refs against a schema)
        ↑
        ├── EngineeredWood.Iceberg            (already has this; migrates to consume)
        ├── EngineeredWood.Parquet            (predicate pushdown — this document's primary deliverable)
        ├── EngineeredWood.Orc                (stripe pruning — future)
        └── EngineeredWood.DeltaLake          (partition + stats pruning)

EngineeredWood.Expressions.Arrow        (new — depends on Expressions + Apache.Arrow)
    │
    └── IRowEvaluator + ArrowRowEvaluator
        ↑
        ├── EngineeredWood.DeltaLake.Table    (CHECK constraints, generated columns, post-filter)
        └── EngineeredWood.Parquet            (post-pruning row filtering — future)

EngineeredWood.SparkSql                 (new — ANTLR-based, optional)
    │
    └── Parses Spark SQL expression strings into Expression trees
        ↑
        └── consumed only by Delta Lake clients that need CHECK / generated columns
```

ANTLR sits at the leaves. Format libraries depend on the core expression library;
they never see the parser. Delta Lake takes an optional
`IExpressionParser` (or `Func<string, Expression>`) on table open. Consumers who
don't need CHECK constraints or generated columns never reference the Spark
parser package and never pull in ANTLR.

### Why not one library per concern?

- **Stats evaluation without Arrow** is genuinely useful (metadata-only tools,
  query planners deciding which files to distribute to workers).
- **Row evaluation without stats** is the natural Delta Lake CHECK path.
- Forcing an Arrow dependency on stats consumers, or a stats dependency on row
  consumers, creates needless coupling.
- Two small libraries with a clear upstream/downstream relationship is simpler
  than one library with conditional Arrow references or feature flags.

## `EngineeredWood.Expressions` — Core Library

### Expression tree

Modeled on Iceberg's existing implementation (which is already proven and shaped
correctly). The tree distinguishes value-producing expressions from
boolean-producing predicates, mirroring delta-kernel-rs:

```csharp
public abstract record Expression;

// Leaf expressions
public sealed record UnboundReference(string Name) : Expression;
public sealed record BoundReference(int FieldId, string Name) : Expression;
public sealed record LiteralExpression(LiteralValue Value) : Expression;
public sealed record FunctionCall(string Name, IReadOnlyList<Expression> Args) : Expression;

// Boolean predicates
public abstract record Predicate : Expression;
public sealed record TruePredicate : Predicate;
public sealed record FalsePredicate : Predicate;
public sealed record AndPredicate(IReadOnlyList<Predicate> Children) : Predicate;
public sealed record OrPredicate(IReadOnlyList<Predicate> Children) : Predicate;
public sealed record NotPredicate(Predicate Child) : Predicate;
public sealed record ComparisonPredicate(Expression Left, ComparisonOperator Op, Expression Right) : Predicate;
public sealed record UnaryPredicate(Expression Operand, UnaryOperator Op) : Predicate;
public sealed record SetPredicate(Expression Operand, IReadOnlyList<LiteralValue> Values, SetOperator Op) : Predicate;
```

Operators are enums:
- `ComparisonOperator`: `Equal`, `NotEqual`, `LessThan`, `LessThanOrEqual`,
  `GreaterThan`, `GreaterThanOrEqual`, `StartsWith`, `NotStartsWith`,
  `NullSafeEqual` (Spark's `<=>`, used by generated column validation)
- `UnaryOperator`: `IsNull`, `IsNotNull`, `IsNaN`, `IsNotNaN`
- `SetOperator`: `In`, `NotIn`

Convenience factories live in a static `Expressions` class:

```csharp
public static class Expressions
{
    public static Predicate Equal(string column, LiteralValue value);
    public static Predicate GreaterThan(string column, LiteralValue value);
    public static Predicate IsNull(string column);
    public static Predicate And(params Predicate[] children);
    // etc.
}
```

### `LiteralValue` — typed scalar

A value type that wraps any comparable scalar without boxing. Supports cross-type
numeric promotion (int vs long, float vs double) via `IComparable<LiteralValue>`.

```csharp
public readonly struct LiteralValue : IComparable<LiteralValue>, IEquatable<LiteralValue>
{
    internal enum Kind : byte
    {
        Null,
        Boolean, Int32, Int64, UInt32, UInt64,
        Float, Double, Half,
        String, Binary,
        Decimal, HighPrecisionDecimal,
        DateOnly, TimeOnly, DateTimeOffset,
        Guid,
    }

    public Kind Type { get; }

    public static implicit operator LiteralValue(bool value);
    public static implicit operator LiteralValue(int value);
    public static implicit operator LiteralValue(long value);
    public static implicit operator LiteralValue(uint value);
    public static implicit operator LiteralValue(ulong value);
    public static implicit operator LiteralValue(float value);
    public static implicit operator LiteralValue(double value);
    public static implicit operator LiteralValue(decimal value);
    public static implicit operator LiteralValue(string value);
    public static implicit operator LiteralValue(byte[] value);
    public static implicit operator LiteralValue(DateTimeOffset value);
    public static implicit operator LiteralValue(Guid value);
#if NET6_0_OR_GREATER
    public static implicit operator LiteralValue(Half value);
    public static implicit operator LiteralValue(DateOnly value);
    public static implicit operator LiteralValue(TimeOnly value);
#endif

    public static LiteralValue Null { get; }

    /// High-precision decimal for Decimal128/256 columns whose precision exceeds
    /// System.decimal's 28-29 digit limit.
    public static LiteralValue Decimal(BigInteger unscaledValue, int scale);

    public int CompareTo(LiteralValue other);
}
```

The implicit conversions mean call sites look like
`Expressions.Equal("name", "Alice")` — the `LiteralValue` type is invisible in
common cases.

**Why not Iceberg's existing `LiteralValue`?** Iceberg's version is a class
(boxes), doesn't support high-precision decimal, and lives in the wrong
assembly. The new struct subsumes it; Iceberg migrates to use the shared one.

### Schema binding

`UnboundReference("name")` carries a column name; `BoundReference(fieldId, name)`
carries an Iceberg-style field ID after binding. `ExpressionBinder` walks an
expression tree against a schema and replaces unbound references with bound
ones, also validating that referenced columns exist and that types make sense.

For non-Iceberg formats (Parquet, Delta, ORC), binding is optional — name-based
references work directly. Iceberg requires binding before evaluation.

### Statistics evaluator

Three-valued logic generic over the statistics carrier:

```csharp
public enum FilterResult
{
    /// All rows in this unit satisfy the predicate.
    AlwaysTrue,
    /// No rows in this unit can satisfy the predicate.
    AlwaysFalse,
    /// Some rows may satisfy the predicate; must read to determine.
    Unknown,
}

public interface IStatisticsAccessor<TStats>
{
    LiteralValue? GetMinValue(TStats stats, string column);
    LiteralValue? GetMaxValue(TStats stats, string column);
    long? GetNullCount(TStats stats, string column);
    long? GetValueCount(TStats stats, string column);
    bool IsMinExact(TStats stats, string column);
    bool IsMaxExact(TStats stats, string column);
}

public static class StatisticsEvaluator
{
    public static FilterResult Evaluate<TStats>(
        Predicate predicate,
        TStats stats,
        IStatisticsAccessor<TStats> accessor);
}
```

Each format implements `IStatisticsAccessor<TStats>` for its own stats carrier:

- Iceberg: `IStatisticsAccessor<DataFileStats>`
- Parquet: `IStatisticsAccessor<RowGroup>` (decodes physical bytes into `LiteralValue` on demand)
- Delta: `IStatisticsAccessor<ColumnStats>` (parses `JsonElement` into `LiteralValue`)
- ORC: `IStatisticsAccessor<StripeFooter>` (future)

### Three-valued evaluation rules

| Predicate | AlwaysTrue when | AlwaysFalse when |
|-----------|----------------|-----------------|
| `Equal(col, v)` | min == max == v | v < min or v > max |
| `NotEqual(col, v)` | v < min or v > max | min == max == v |
| `GreaterThan(col, v)` | min > v | max <= v |
| `GreaterThanOrEqual(col, v)` | min >= v | max < v |
| `LessThan(col, v)` | max < v | min >= v |
| `LessThanOrEqual(col, v)` | max <= v | min > v |
| `NullSafeEqual(col, v)` | min == max == v | v < min or v > max |
| `IsNull(col)` | NullCount == ValueCount | NullCount == 0 |
| `IsNotNull(col)` | NullCount == 0 | NullCount == ValueCount |
| `In(col, vs)` | (defers to bloom filter) | all values outside [min, max] |
| `And(a, b, ...)` | all AlwaysTrue | any AlwaysFalse |
| `Or(a, b, ...)` | any AlwaysTrue | all AlwaysFalse |
| `Not(a)` | a is AlwaysFalse | a is AlwaysTrue |

When statistics are missing or the accessor returns null, the result is
`Unknown`. When `IsMinExact` or `IsMaxExact` is false (truncated statistics),
range comparisons at the boundary conservatively return `Unknown`.

## `EngineeredWood.Expressions.Arrow` — Row Evaluator

Walks an `Expression` tree against a `RecordBatch`, producing typed Arrow arrays
for value expressions and `BooleanArray` for predicates.

```csharp
public interface IRowEvaluator
{
    BooleanArray Evaluate(Predicate predicate, RecordBatch batch);
    IArrowArray Evaluate(Expression expression, RecordBatch batch);
}

public sealed class ArrowRowEvaluator : IRowEvaluator
{
    public ArrowRowEvaluator(IFunctionRegistry? functions = null);
}

public interface IFunctionRegistry
{
    IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount);
    bool IsRegistered(string name);
}
```

The default `ArrowRowEvaluator` handles all built-in predicates and operators
(comparisons, AND/OR/NOT, IS NULL, IN, arithmetic, CAST). Format-specific
function libraries are registered via `IFunctionRegistry` — Spark SQL functions
like `YEAR`, `SUBSTRING`, `DATE_FORMAT` come from the `EngineeredWood.SparkSql`
package's function registry.

### Used by

- **Delta Lake CHECK constraints**: evaluate the constraint predicate against
  every batch being written; if any row evaluates to false (or null), abort the
  write.
- **Delta Lake generated columns**: evaluate the generation expression to
  produce the column's values; for validation, compute
  `(materialized <=> generated) IS TRUE` per row.
- **Parquet post-filter** (future): after row group pruning identifies candidate
  groups, the row evaluator filters the materialized batches to drop
  non-matching rows.
- **Any reader**: caller-provided post-filter applied to the output stream.

## Parquet Predicate Pushdown

This is the original scope of this document. Parquet integration becomes
straightforward once the shared library exists.

### `ParquetReadOptions.Filter`

```csharp
public sealed class ParquetReadOptions
{
    // ... existing fields ...

    /// Filter predicate for row group pruning. Row groups whose statistics
    /// prove no rows can match are skipped entirely.
    public Predicate? Filter { get; init; }

    /// When true and a Filter is set, bloom filters are probed for equality
    /// predicates before falling back to statistics. Requires additional I/O
    /// per candidate row group. Default: false.
    public bool FilterUseBloomFilters { get; init; }
}
```

### `ParquetStatisticsAccessor`

Implements `IStatisticsAccessor<RowGroup>` for Parquet's row group metadata.
Decodes raw `byte[]` min/max from the column's physical type into `LiteralValue`
on demand using the column descriptor.

This is where Parquet's binary statistics encoding (signed vs unsigned int,
big-endian decimal, lexicographic byte arrays, NaN handling on float/double)
lives. The shared `StatisticsEvaluator` consumes typed `LiteralValue` and
doesn't need to know about physical encoding.

**Sort orders** (from the Parquet spec):

| Physical Type | Logical Type | Sort Order |
|---|---|---|
| BOOLEAN | — | Unsigned (false < true) |
| INT32 | — | Signed |
| INT32 | INT(unsigned) | Unsigned |
| INT32 | DATE | Signed |
| INT64 | — | Signed |
| INT64 | INT(unsigned) | Unsigned |
| INT64 | TIMESTAMP | Signed |
| FLOAT | — | Signed (with NaN handling) |
| DOUBLE | — | Signed (with NaN handling) |
| BYTE_ARRAY | STRING | Unsigned lexicographic |
| BYTE_ARRAY | — | Unsigned lexicographic |
| FIXED_LEN_BYTE_ARRAY | — | Unsigned lexicographic |
| FIXED_LEN_BYTE_ARRAY | DECIMAL | Big-endian signed |
| INT96 | — | Undefined (statistics unreliable) |

**Float/Double NaN handling:** Per the Parquet spec, NaN is not a valid
statistics value. If min or max is NaN, statistics are treated as absent.

### Reader integration

```csharp
public async IAsyncEnumerable<RecordBatch> ReadAllAsync(
    IReadOnlyList<string>? columnNames = null,
    [EnumeratorCancellation] CancellationToken ct = default)
{
    var metadata = await ReadMetadataAsync(ct).ConfigureAwait(false);
    var accessor = new ParquetStatisticsAccessor(_schema);

    for (int i = 0; i < metadata.RowGroups.Count; i++)
    {
        if (_options.Filter != null)
        {
            var result = StatisticsEvaluator.Evaluate(
                _options.Filter, metadata.RowGroups[i], accessor);

            if (result == FilterResult.AlwaysFalse)
                continue;

            if (_options.FilterUseBloomFilters
                && result == FilterResult.Unknown)
            {
                var bloomResult = await BloomFilterEvaluator.EvaluateAsync(
                    _options.Filter, _file, metadata.RowGroups[i], _schema, ct)
                    .ConfigureAwait(false);
                if (bloomResult == FilterResult.AlwaysFalse)
                    continue;
            }
        }

        yield return await ReadRowGroupAsync(i, columnNames, ct)
            .ConfigureAwait(false);
    }
}
```

### Bloom filter integration

`ParquetFileReader.GetCandidateRowGroupsAsync` already probes bloom filters for
a column + value list. The new `BloomFilterEvaluator` walks an `Expression` tree
to find `Equal`/`In` predicates and probes them per row group:

1. Walk the predicate to find equality-shaped sub-predicates
2. For each, probe the bloom filter (if present) for the row group/column
3. If the bloom filter says "definitely not present" → `AlwaysFalse`
4. Otherwise, fall through to statistics

Bloom filter probing is only useful for equality and set membership. Range
predicates (`GreaterThan`, `LessThan`, etc.) cannot use bloom filters.

## Delta Lake Integration

### Stats-based file pruning

Implement `IStatisticsAccessor<ColumnStats>` for Delta's `ColumnStats` type
(parsed from `AddFile.stats` JSON). Filter `Snapshot.ActiveFiles` before
reading:

```csharp
public IAsyncEnumerable<RecordBatch> ReadAllAsync(
    Predicate? filter = null,
    IReadOnlyList<string>? columns = null,
    CancellationToken ct = default)
{
    var accessor = new DeltaStatisticsAccessor();

    foreach (var addFile in CurrentSnapshot.ActiveFiles.Values)
    {
        if (filter != null && addFile.Stats != null)
        {
            var stats = ColumnStats.Parse(addFile.Stats);
            var result = StatisticsEvaluator.Evaluate(filter, stats, accessor);
            if (result == FilterResult.AlwaysFalse)
                continue;
        }

        // Also: partition pruning based on addFile.PartitionValues
        // ...

        await foreach (var batch in ReadFileAsync(addFile, columns, ...))
            yield return batch;
    }
}
```

### CHECK constraints

On write, parse `delta.constraints.{name}` into a `Predicate`, evaluate against
each batch, and abort if any row fails:

```csharp
foreach (var batch in batches)
{
    foreach (var (name, expr) in constraints)
    {
        var result = _rowEvaluator.Evaluate(expr, batch);
        if (HasFalseOrNull(result))
            throw new DeltaConstraintViolationException(name);
    }
    // ... write batch ...
}
```

The constraint expressions are stored as Spark SQL strings in
`Metadata.Configuration`. Parsing requires the optional
`EngineeredWood.SparkSql` package via an injected `IExpressionParser`:

```csharp
public sealed record DeltaTableOptions
{
    // ... existing ...

    /// Parser for SQL expressions in CHECK constraints and generated columns.
    /// If null and the table has CHECK constraints or generated columns, write
    /// operations will throw.
    public IExpressionParser? ExpressionParser { get; init; }
}
```

### Generated columns

On write, parse `delta.generationExpression` from each generated column's
metadata. For each batch:

1. If the user provided a value for the generated column: validate
   `(materialized <=> generated) IS TRUE`
2. If the user did not provide a value: compute it via the row evaluator and
   substitute it into the batch

Same parser injection as CHECK constraints.

## `EngineeredWood.SparkSql` — ANTLR-Based Parser

A separate, optional package. Consumers who don't need CHECK constraints or
generated columns never reference it.

### Approach

Apache Spark publishes its SQL grammar as `SqlBaseParser.g4` and
`SqlBaseLexer.g4` under Apache 2.0. The full grammar is ~3,400 lines and covers
DDL, DML, queries, and expressions. The expression subset we need is a small
fraction:

- `expression` → `booleanExpression` → `predicated` → `valueExpression`
- `valueExpression` → arithmetic, comparison
- `primaryExpression` → literals, CAST, CASE, function calls, column refs
- Supporting rules: `comparisonOperator`, `predicate`, `dataType`,
  `identifier`, `qualifiedName`, `whenClause`, `constant`

Total: roughly 200 lines of parser rules + 100 lines of lexer tokens.

### Subsetting tool

A small program parses the upstream `SqlBaseParser.g4` grammar, walks rule
references starting from `expression`, and emits a self-contained subset
grammar. Rerun the tool against each new Spark release to pick up additions.

This avoids:
- Hand-maintaining a fork (drift over time)
- Depending on a third-party SQL parser whose Spark-dialect coverage is
  approximate (SqlParser-cs, etc.)
- Pulling ANTLR into the bottom of the dependency stack — it lives only in this
  optional leaf package.

### Translation layer

A visitor walks ANTLR's CST and produces `EngineeredWood.Expressions`
`Expression` nodes. Function calls become `FunctionCall(name, args)`; the
`SparkFunctionRegistry` provides Arrow-backed implementations for the standard
Spark functions used in Delta expressions (`CAST`, `YEAR`, `MONTH`, `DAY`,
`HOUR`, `SUBSTRING`, `DATE_FORMAT`, `CONCAT`, `COALESCE`, `IF`, etc.).

### Function set

Minimum viable set for CHECK constraints and generated columns:

- Type casting: `CAST`, `TRY_CAST`
- Date/time: `YEAR`, `MONTH`, `DAY`, `HOUR`, `DATE_FORMAT`, `CURRENT_DATE`,
  `CURRENT_TIMESTAMP`
- String: `SUBSTRING`, `CONCAT`, `LENGTH`, `TRIM`, `UPPER`, `LOWER`
- Null handling: `COALESCE`, `IFNULL`, `NULLIF`
- Conditional: `CASE/WHEN/THEN/ELSE/END`, `IF`
- Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`, `<=>` (null-safe), `BETWEEN`,
  `IN`, `LIKE`
- Logical: `AND`, `OR`, `NOT`
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- IS predicates: `IS NULL`, `IS NOT NULL`, `IS TRUE`, `IS FALSE`

## Future: Page-Level Pushdown via Column/Offset Index

| Index | Content | Enables |
|---|---|---|
| **Column Index** | Min/max values and null counts per page within a column chunk | Page-level statistics evaluation — skip pages that can't match |
| **Offset Index** | Byte offset and row range of each page within a column chunk | Seeking directly to matching pages without scanning headers |

Composes naturally with row group pruning: pruning eliminates row groups, the
column index identifies pages within surviving groups, the offset index
provides byte ranges to read only matching pages. The same `Predicate` tree is
evaluated per-page with the same `StatisticsEvaluator`.

This is a separate body of work — requires parsing column/offset index Thrift
structures, a page-skipping read path in `ColumnChunkReader`, and writing the
indexes in `ColumnChunkWriter`.

## Implementation Phases

| Phase | Scope | Project | Depends On |
|---|---|---|---|
| **Phase 1** | `EngineeredWood.Expressions` core: tree, `LiteralValue`, factories | new project | nothing |
| **Phase 2** | `IStatisticsAccessor<TStats>`, `StatisticsEvaluator` | Expressions | Phase 1 |
| **Phase 3** | `ExpressionBinder`, schema binding | Expressions | Phase 1 |
| **Phase 4** | Migrate Iceberg expressions to consume the shared library | Iceberg | Phases 1-3 |
| **Phase 5** | `ParquetStatisticsAccessor`; `ParquetReadOptions.Filter`; row group pruning | Parquet | Phase 2 |
| **Phase 6** | Bloom filter probing in Parquet | Parquet | Phase 5 |
| **Phase 7** | Delta Lake stats-based file pruning + partition pruning | DeltaLake.Table | Phase 2 |
| **Phase 8** | `EngineeredWood.Expressions.Arrow`: `ArrowRowEvaluator`, `IFunctionRegistry` | new project | Phase 1 |
| **Phase 9** | `EngineeredWood.SparkSql`: subsetting tool, ANTLR grammar, parser, function registry | new project | Phases 1, 8 |
| **Phase 10** | Wire CHECK constraints and generated columns into Delta Lake writes | DeltaLake.Table | Phases 8, 9 |
| **Phase 11** | Column/offset index parsing (Parquet read) | Parquet | Phase 2 |
| **Phase 12** | Page-level pushdown using column index | Parquet | Phases 5, 11 |
| **Phase 13** | Column/offset index writing | Parquet | Phase 11 |
| **Phase 14** | ORC stripe pruning | Orc | Phase 2 |

### Ordering rationale

Phases 1-7 are the natural progression of "build the core, prove it on the two
formats with concrete needs (Parquet pruning, Delta pruning), and migrate
Iceberg to consume it." Each phase is independently testable and shippable.

Phases 8-10 extend the architecture to row-level evaluation, unblocking CHECK
constraints and generated columns. The Spark parser is pure leaf work and can
proceed in parallel with anything earlier.

Phases 11-14 are advanced optimization (page-level pruning) and additional
format support (ORC). Defer until the simpler wins land.

### Testing strategy

- **Unit tests:** `LiteralValue` comparison across all type pairs and edge
  cases (NaN, decimal precision, string ordering)
- **Unit tests:** `StatisticsEvaluator` against synthetic stats with known
  min/max/null_count for each predicate variant
- **Unit tests:** `ParquetStatisticsAccessor` decoding of physical bytes for
  every (physical_type, logical_type, sort_order) combination
- **Round-trip tests:** Write Parquet files with known data distributions, read
  with filters, assert correct row group pruning
- **Cross-validation:** Compare pruning decisions against PyArrow's
  `read_table(filters=...)` or DuckDB's predicate pushdown on the same files
- **Edge cases:** Empty statistics, all-null columns, single-row row groups,
  NaN in float columns, truncated min/max, Decimal256 statistics
- **Iceberg parity:** After migration, all existing Iceberg expression tests
  must still pass

## Open Questions

1. **Where does `EngineeredWood.Expressions` live in the dependency graph?** It
   has no Arrow dependency, so it could sit alongside `EngineeredWood.Core`. It
   does not need to depend on Core itself unless we want to share types like
   `IMemoryOwner<byte>` (we don't, for the expression layer). Recommendation:
   sibling of Core, no mutual dependency.

2. **Should `Predicate` and `Expression` be separate hierarchies, or should
   `Predicate` be a tag interface on `Expression`?** delta-kernel-rs separates
   them; Iceberg merges them (`AndExpression` extends `Expression`). Separation
   is more type-safe (you can't accidentally use a value expression where a
   predicate is required) but creates two parallel hierarchies. Recommendation:
   separate, with `Predicate` as a subclass of `Expression` for cases that
   need to mix them (function arguments, CASE branches).

3. **Should the Iceberg migration happen before or after Parquet pushdown?**
   Migrating Iceberg first proves the API shape against an existing user.
   Doing Parquet first delivers user-visible value sooner. Recommendation:
   migrate Iceberg first (Phase 4 before Phase 5) — the API shape risk is real,
   and Iceberg has the test coverage to catch regressions.

4. **Bloom filter probing automatic or opt-in?** Requires I/O even when stats
   alone prove `AlwaysFalse`. Recommendation: opt-in via
   `FilterUseBloomFilters`, as proposed.

5. **Should `ReadRowGroupAsync` also accept a filter?** Currently only
   `ReadAllAsync` would use it. Defer until page-level pushdown exists.
