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
