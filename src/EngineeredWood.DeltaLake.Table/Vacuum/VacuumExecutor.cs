// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using DeltaSnapshot = EngineeredWood.DeltaLake.Snapshot.Snapshot;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Table.Vacuum;

/// <summary>
/// Identifies and deletes unreferenced data files that are older than
/// the retention period.
/// </summary>
internal static class VacuumExecutor
{
    /// <summary>
    /// Finds unreferenced files and optionally deletes them.
    /// </summary>
    public static async ValueTask<VacuumResult> ExecuteAsync(
        ITableFileSystem fs,
        DeltaSnapshot snapshot,
        TimeSpan retentionPeriod,
        bool dryRun,
        CancellationToken cancellationToken)
    {
        // Collect all referenced file paths from the current snapshot
        var referencedPaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var addFile in snapshot.ActiveFiles.Values)
            referencedPaths.Add(addFile.Path);

        // List all data files in the table directory
        // Data files are Parquet files not in _delta_log/
        var allFiles = new List<TableFileInfo>();
        await foreach (var file in fs.ListAsync("", cancellationToken).ConfigureAwait(false))
        {
            // Skip log files and non-parquet files
            if (file.Path.StartsWith("_delta_log/", StringComparison.Ordinal) ||
                file.Path.StartsWith("_delta_log\\", StringComparison.Ordinal))
                continue;

            if (!file.Path.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
                continue;

            allFiles.Add(file);
        }

        // Find unreferenced files older than the retention period
        var cutoff = DateTimeOffset.UtcNow - retentionPeriod;
        var filesToDelete = new List<string>();

        foreach (var file in allFiles)
        {
            if (!referencedPaths.Contains(file.Path) && file.LastModified < cutoff)
                filesToDelete.Add(file.Path);
        }

        int deleted = 0;
        if (!dryRun)
        {
            foreach (string path in filesToDelete)
            {
                await fs.DeleteAsync(path, cancellationToken).ConfigureAwait(false);
                deleted++;
            }
        }

        return new VacuumResult
        {
            FilesToDelete = filesToDelete,
            FilesDeleted = deleted,
        };
    }
}
