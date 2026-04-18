// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Evaluates a <see cref="Predicate"/> against a Delta <see cref="AddFile"/>
/// using both partition values and per-file column statistics. Skips files
/// that the evaluator proves cannot contain matching rows.
/// </summary>
internal sealed class DeltaFilePruner
{
    private readonly DeltaFileStatsAccessor _accessor;

    public DeltaFilePruner(StructType schema, IReadOnlyList<string> partitionColumns)
    {
        var typeMap = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var field in schema.Fields)
        {
            if (field.Type is PrimitiveType pt)
                typeMap[field.Name] = pt.TypeName;
        }

        var partitionSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
        _accessor = new DeltaFileStatsAccessor(typeMap, partitionSet);
    }

    /// <summary>
    /// Returns true if the file might contain rows matching the predicate.
    /// Returns false only when statistics or partition values prove no rows
    /// can match.
    /// </summary>
    public bool ShouldInclude(AddFile addFile, Predicate filter)
    {
        if (filter is TruePredicate)
            return true;

        var stats = new DeltaFileStats(addFile, ColumnStats.Parse(addFile.Stats));
        return StatisticsEvaluator.Evaluate(filter, stats, _accessor)
            != FilterResult.AlwaysFalse;
    }
}

/// <summary>
/// Bundles a Delta file's partition values and parsed column statistics for
/// evaluation. Both views are needed because the predicate may reference
/// partition columns (constants per file) and data columns (typed via stats).
/// </summary>
internal sealed class DeltaFileStats
{
    public DeltaFileStats(AddFile addFile, ColumnStats? columnStats)
    {
        AddFile = addFile;
        ColumnStats = columnStats;
    }

    public AddFile AddFile { get; }
    public ColumnStats? ColumnStats { get; }
}

/// <summary>
/// Adapts <see cref="DeltaFileStats"/> for the shared
/// <see cref="StatisticsEvaluator"/>. Partition columns return their
/// constant value as both min and max (with null-count = 0); data columns
/// look up min/max from the parsed stats and decode the JSON element using
/// the column's Delta primitive type.
/// </summary>
internal sealed class DeltaFileStatsAccessor : IStatisticsAccessor<DeltaFileStats>
{
    private readonly IReadOnlyDictionary<string, string> _columnTypes;
    private readonly HashSet<string> _partitionColumns;

    public DeltaFileStatsAccessor(
        IReadOnlyDictionary<string, string> columnTypes,
        HashSet<string> partitionColumns)
    {
        _columnTypes = columnTypes;
        _partitionColumns = partitionColumns;
    }

    public LiteralValue? GetMinValue(DeltaFileStats stats, string column) =>
        GetBound(stats, column, isMin: true);

    public LiteralValue? GetMaxValue(DeltaFileStats stats, string column) =>
        GetBound(stats, column, isMin: false);

    public long? GetNullCount(DeltaFileStats stats, string column)
    {
        if (_partitionColumns.Contains(column))
        {
            // Partition value is constant per file. If the stored value is
            // null (the dictionary holds a null string), every row is null;
            // otherwise no row is null in this column.
            if (stats.AddFile.PartitionValues.TryGetValue(column, out var v))
                return v is null ? stats.AddFile.PartitionValues.Count > 0 ? GetValueCount(stats, column) : null : 0;
            return null;
        }

        return stats.ColumnStats?.NullCount?.TryGetValue(column, out long n) == true
            ? (long?)n : null;
    }

    public long? GetValueCount(DeltaFileStats stats, string column) =>
        stats.ColumnStats?.NumRecords > 0 ? stats.ColumnStats.NumRecords : null;

    public bool IsMinExact(DeltaFileStats stats, string column) => true;
    public bool IsMaxExact(DeltaFileStats stats, string column) => true;

    private LiteralValue? GetBound(DeltaFileStats stats, string column, bool isMin)
    {
        if (!_columnTypes.TryGetValue(column, out string? typeName))
            return null;

        if (_partitionColumns.Contains(column))
        {
            if (!stats.AddFile.PartitionValues.TryGetValue(column, out var partVal))
                return null;
            return DeltaLiteralDecoder.FromPartitionString(partVal, typeName);
        }

        var bounds = isMin ? stats.ColumnStats?.MinValues : stats.ColumnStats?.MaxValues;
        if (bounds is null || !bounds.TryGetValue(column, out var element))
            return null;

        return DeltaLiteralDecoder.FromJson(element, typeName);
    }
}
