using EngineeredWood.Expressions;

namespace EngineeredWood.Parquet;

/// <summary>
/// Controls the Arrow output type for BYTE_ARRAY (string/binary) columns.
/// </summary>
public enum ByteArrayOutputKind
{
    /// <summary>
    /// Default: UTF8-annotated columns produce <c>StringType</c>; all others produce <c>BinaryType</c>.
    /// Uses 32-bit offsets (max 2 GB of string data per column per row group).
    /// </summary>
    Default,

    /// <summary>
    /// Produces <c>StringViewType</c> or <c>BinaryViewType</c>.
    /// Values ≤12 bytes are stored inline in the 16-byte view entry (no overflow copy).
    /// Longer values share a single overflow buffer. Best for short-string or prefix-scan workloads.
    /// </summary>
    ViewType,

    /// <summary>
    /// Produces <c>LargeStringType</c> or <c>LargeBinaryType</c> with 64-bit offsets.
    /// Removes the 2 GB per-column limit. Decode path is otherwise identical to <see cref="Default"/>.
    /// </summary>
    LargeOffsets,
}

/// <summary>
/// Options that control how Parquet data is read and mapped to Apache Arrow types.
/// </summary>
public sealed class ParquetReadOptions
{
    /// <summary>Default options: all features disabled, producing standard Arrow types.</summary>
    public static readonly ParquetReadOptions Default = new();

    /// <summary>
    /// Controls the Arrow output type for BYTE_ARRAY (string/binary) columns.
    /// </summary>
    public ByteArrayOutputKind ByteArrayOutput { get; init; } = ByteArrayOutputKind.Default;

    /// <summary>
    /// Maximum number of rows per <see cref="Apache.Arrow.RecordBatch"/>. When set, row groups
    /// larger than this limit are split across multiple batches. When <see langword="null"/>
    /// (the default), each row group produces exactly one batch.
    /// </summary>
    public int? BatchSize { get; init; }

    /// <summary>
    /// Approximate maximum uncompressed size (in bytes) of a single <see cref="Apache.Arrow.RecordBatch"/>.
    /// The budget is measured as the sum of uncompressed Parquet page sizes across all columns;
    /// the actual Arrow representation may be somewhat larger due to validity bitmaps, offset
    /// arrays, and alignment padding. When both <see cref="BatchSize"/> and
    /// <see cref="MaxBatchByteSize"/> are set, the more restrictive limit wins.
    /// When <see langword="null"/> (the default), no size limit is applied.
    /// </summary>
    public long? MaxBatchByteSize { get; init; }

    /// <summary>
    /// Optional row group filter predicate. When set, the reader evaluates the
    /// predicate against each row group's column statistics; row groups that
    /// can be proven empty of matching rows (per <see cref="StatisticsEvaluator"/>)
    /// are skipped without reading data pages.
    /// </summary>
    /// <remarks>
    /// Predicates that statistics can't evaluate (function calls, two-column
    /// comparisons, missing stats) are conservatively kept. The reader does not
    /// re-apply the predicate to rows; callers wanting exact row-level
    /// filtering must do that on the returned batches themselves.
    /// </remarks>
    public Predicate? Filter { get; init; }

    /// <summary>
    /// When <see langword="true"/> and <see cref="Filter"/> is set, the reader
    /// also probes Bloom filters for equality and IN predicates that the
    /// statistics evaluator could not decide. Requires extra I/O per candidate
    /// row group (one read per column with a Bloom filter), so this is opt-in.
    /// Default: <see langword="false"/>.
    /// </summary>
    public bool FilterUseBloomFilters { get; init; }

    /// <summary>
    /// Whether to validate CRC-32C checksums when present in page headers.
    /// When enabled and a page header contains a <c>crc</c> field, the compressed
    /// page data is verified before decompression. Mismatches throw
    /// <see cref="ParquetFormatException"/>. Default is <see langword="false"/>.
    /// </summary>
    public bool PageChecksumValidation { get; init; }

    /// <summary>
    /// Shorthand for <c>ByteArrayOutput == ByteArrayOutputKind.ViewType</c>.
    /// When set to <see langword="true"/>, sets <see cref="ByteArrayOutput"/> to
    /// <see cref="ByteArrayOutputKind.ViewType"/>; setting to <see langword="false"/>
    /// reverts to <see cref="ByteArrayOutputKind.Default"/>.
    /// </summary>
    public bool UseViewTypes
    {
        get => ByteArrayOutput == ByteArrayOutputKind.ViewType;
        init => ByteArrayOutput = value ? ByteArrayOutputKind.ViewType : ByteArrayOutputKind.Default;
    }
}
