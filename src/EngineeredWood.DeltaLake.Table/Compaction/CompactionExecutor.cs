// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using DeltaSnapshot = EngineeredWood.DeltaLake.Snapshot.Snapshot;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Compaction;

/// <summary>
/// Executes file compaction: reads small files, rewrites them as larger files,
/// and commits the add/remove actions.
/// </summary>
internal static class CompactionExecutor
{
    /// <summary>
    /// Selects files eligible for compaction and rewrites them.
    /// Returns the new version number, or null if no files were compacted.
    /// </summary>
    public static async ValueTask<long?> ExecuteAsync(
        ITableFileSystem fs,
        TransactionLog log,
        DeltaSnapshot snapshot,
        CompactionOptions options,
        ParquetWriteOptions parquetOptions,
        ParquetReadOptions parquetReadOptions,
        CancellationToken cancellationToken)
    {
        // Select small files as compaction candidates
        var candidates = snapshot.ActiveFiles.Values
            .Where(f => f.Size < options.MinFileSize)
            .OrderBy(f => f.Size)
            .Take(options.MaxFilesPerCommit)
            .ToList();

        if (candidates.Count < 2)
            return null; // Not worth compacting a single file

        // Build target schema for type widening during compaction
        var targetSchema = SchemaConverter.ToArrowSchema(
            DeltaSchemaSerializer.Parse(snapshot.Metadata.SchemaString));

        // Read all data from candidate files, widening types if needed
        var allBatches = new List<RecordBatch>();
        foreach (var addFile in candidates)
        {
            await using var file = await fs.OpenReadAsync(addFile.Path, cancellationToken)
                .ConfigureAwait(false);
            using var reader = new ParquetFileReader(file, ownsFile: false, parquetReadOptions);

            await foreach (var batch in reader.ReadAllAsync(
                cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                // Widen values from old files to match current schema
                allBatches.Add(TypeWidening.ValueWidener.WidenBatch(batch, targetSchema));
            }
        }

        if (allBatches.Count == 0)
            return null;

        // Write compacted data into new files
        var actions = new List<DeltaAction>();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Remove old files (with dataChange: false since this is rearrangement)
        foreach (var oldFile in candidates)
        {
            actions.Add(new RemoveFile
            {
                Path = oldFile.Path,
                DeletionTimestamp = now,
                DataChange = false,
                ExtendedFileMetadata = true,
                PartitionValues = oldFile.PartitionValues,
                Size = oldFile.Size,
            });
        }

        // Row tracking state
        bool rowTrackingEnabled = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;

        // Earliest defaultRowCommitVersion from source files (preserved through compaction)
        long? earliestCommitVersion = candidates
            .Where(c => c.DefaultRowCommitVersion.HasValue)
            .Select(c => c.DefaultRowCommitVersion!.Value)
            .DefaultIfEmpty(-1)
            .Min();
        if (earliestCommitVersion == -1) earliestCommitVersion = null;

        // Write new compacted file(s)
        // Group batches to target file size (approximate by row count)
        long totalRows = allBatches.Sum(b => (long)b.Length);
        long totalBytes = candidates.Sum(f => f.Size);
        double bytesPerRow = totalRows > 0 ? (double)totalBytes / totalRows : 0;
        long rowsPerFile = bytesPerRow > 0
            ? Math.Max(1, (long)(options.TargetFileSize / bytesPerRow))
            : totalRows;

        int batchIdx = 0;
        long currentRowCount = 0;
        var currentBatches = new List<RecordBatch>();

        while (batchIdx < allBatches.Count)
        {
            currentBatches.Add(allBatches[batchIdx]);
            currentRowCount += allBatches[batchIdx].Length;
            batchIdx++;

            if (currentRowCount >= rowsPerFile || batchIdx == allBatches.Count)
            {
                string fileName = $"{Guid.NewGuid():N}.parquet";
                long fileSize;
                long fileBaseRowId = nextRowId;

                await using (var outFile = await fs.CreateAsync(
                    fileName, cancellationToken: cancellationToken).ConfigureAwait(false))
                {
                    await using var writer = new ParquetFileWriter(
                        outFile, ownsFile: false, parquetOptions);
                    foreach (var batch in currentBatches)
                    {
                        await writer.WriteRowGroupAsync(batch, cancellationToken)
                            .ConfigureAwait(false);
                    }
                    await writer.DisposeAsync().ConfigureAwait(false);
                    fileSize = outFile.Position;
                }

                if (rowTrackingEnabled)
                    nextRowId += currentRowCount;

                string? stats = Stats.StatsCollector.Collect(currentBatches);

                actions.Add(new AddFile
                {
                    Path = fileName,
                    PartitionValues = candidates[0].PartitionValues,
                    Size = fileSize,
                    ModificationTime = now,
                    DataChange = false,
                    Stats = stats,
                    BaseRowId = rowTrackingEnabled ? fileBaseRowId : null,
                    DefaultRowCommitVersion = earliestCommitVersion,
                });

                currentBatches.Clear();
                currentRowCount = 0;
            }
        }

        // Commit
        long newVersion = snapshot.Version + 1;
        await log.WriteCommitAsync(newVersion, actions, cancellationToken)
            .ConfigureAwait(false);

        return newVersion;
    }
}
