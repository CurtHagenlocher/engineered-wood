namespace EngineeredWood.Parquet;

/// <summary>
/// A decoded Parquet column in a flat format analogous to <c>Parquet.Data.DataColumn</c>.
/// <para>
/// <see cref="Data"/> contains the non-null values densely packed — i.e. nulls are not
/// represented in the array.  For nullable columns, <see cref="DefinitionLevels"/> carries
/// a def-level per row that indicates which rows are null (defLevel &lt; maxDefLevel).
/// For required columns <see cref="DefinitionLevels"/> is <see langword="null"/> and
/// <see cref="Data"/> has exactly <see cref="RowCount"/> entries.
/// </para>
/// </summary>
public readonly struct ParquetColumn
{
    /// <summary>
    /// The dense (non-null) values.  The runtime element type depends on the Parquet physical type:
    /// <list type="bullet">
    ///   <item><c>INT32</c> → <c>int[]</c></item>
    ///   <item><c>INT64</c> → <c>long[]</c></item>
    ///   <item><c>FLOAT</c> → <c>float[]</c></item>
    ///   <item><c>DOUBLE</c> → <c>double[]</c></item>
    ///   <item><c>BOOLEAN</c> → <c>bool[]</c></item>
    ///   <item><c>BYTE_ARRAY</c> → <c>byte[][]</c></item>
    ///   <item><c>FIXED_LEN_BYTE_ARRAY</c> → <c>byte[][]</c></item>
    /// </list>
    /// </summary>
    public System.Array Data { get; }

    /// <summary>
    /// Per-row definition levels, or <see langword="null"/> for required (non-nullable) columns.
    /// A row is null when its definition level is less than <see cref="MaxDefinitionLevel"/>.
    /// </summary>
    public int[]? DefinitionLevels { get; }

    /// <summary>Maximum definition level for this column (1 for a flat optional column).</summary>
    public int MaxDefinitionLevel { get; }

    /// <summary>Total number of rows in the row group.</summary>
    public int RowCount { get; }

    /// <summary>Number of non-null values in <see cref="Data"/>.</summary>
    public int ValueCount { get; }

    internal ParquetColumn(
        System.Array data,
        int[]? definitionLevels,
        int maxDefinitionLevel,
        int rowCount,
        int valueCount)
    {
        Data = data;
        DefinitionLevels = definitionLevels;
        MaxDefinitionLevel = maxDefinitionLevel;
        RowCount = rowCount;
        ValueCount = valueCount;
    }
}
