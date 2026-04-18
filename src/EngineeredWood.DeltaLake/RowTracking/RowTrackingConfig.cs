// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.RowTracking;

/// <summary>
/// Constants and utilities for Delta Lake row tracking.
/// Row tracking assigns stable, unique row IDs that persist across compaction.
/// </summary>
public static class RowTrackingConfig
{
    /// <summary>Table property to enable row tracking.</summary>
    public const string EnableKey = "delta.enableRowTracking";

    /// <summary>
    /// The column name for materialized row IDs in data files.
    /// This is a hidden metadata column, not part of the user-visible schema.
    /// </summary>
    public const string RowIdColumnName = "_metadata.row_id";

    /// <summary>Virtual column name exposed to readers for the row ID.</summary>
    public const string VirtualRowIdColumn = "_metadata.row_id";

    /// <summary>Virtual column name exposed to readers for the commit version.</summary>
    public const string VirtualRowCommitVersionColumn = "_metadata.row_commit_version";

    /// <summary>
    /// Returns true if row tracking is enabled for the table.
    /// </summary>
    public static bool IsEnabled(IReadOnlyDictionary<string, string>? configuration) =>
        configuration is not null &&
        configuration.TryGetValue(EnableKey, out string? value) &&
        string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Computes the row ID high water mark from the active file set.
    /// The next available row ID is <c>highWaterMark</c>.
    /// </summary>
    public static long ComputeHighWaterMark(
        IReadOnlyDictionary<string, Actions.AddFile> activeFiles)
    {
        long highWaterMark = 0;

        foreach (var addFile in activeFiles.Values)
        {
            if (addFile.BaseRowId.HasValue)
            {
                // Estimate row count from stats if available
                long numRows = EstimateRowCount(addFile);
                long fileEnd = addFile.BaseRowId.Value + numRows;
                if (fileEnd > highWaterMark)
                    highWaterMark = fileEnd;
            }
        }

        return highWaterMark;
    }

    /// <summary>
    /// Estimates the row count for a file from its stats JSON.
    /// Falls back to 0 if stats are not available.
    /// </summary>
    private static long EstimateRowCount(Actions.AddFile addFile)
    {
        if (addFile.Stats is null)
            return 0;

        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(addFile.Stats);
            if (doc.RootElement.TryGetProperty("numRecords", out var nr))
                return nr.GetInt64();
        }
        catch
        {
            // Stats are optional; if they can't be parsed, use 0
        }

        return 0;
    }
}
