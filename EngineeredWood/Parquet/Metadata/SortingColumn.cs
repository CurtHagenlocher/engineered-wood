namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Describes a sorting column within a row group.
/// </summary>
public readonly record struct SortingColumn(int ColumnIndex, bool Descending, bool NullsFirst);
