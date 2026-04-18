// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class VacuumTests : IDisposable
{
    private readonly string _tempDir;

    public VacuumTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_vacuum_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Vacuum_DryRun_ListsFilesToDelete()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write data then overwrite (leaving orphaned file)
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Vacuum with zero retention (all unreferenced files eligible)
        var result = await table.VacuumAsync(
            retentionPeriod: TimeSpan.Zero,
            dryRun: true);

        // Should find the orphaned file from the first write
        Assert.NotEmpty(result.FilesToDelete);
        Assert.Equal(0, result.FilesDeleted); // Dry run → nothing deleted
    }

    [Fact]
    public async Task Vacuum_DeletesOrphanedFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write then overwrite
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Vacuum with zero retention — actually delete
        var result = await table.VacuumAsync(
            retentionPeriod: TimeSpan.Zero,
            dryRun: false);

        Assert.NotEmpty(result.FilesToDelete);
        Assert.Equal(result.FilesToDelete.Count, result.FilesDeleted);

        // Verify deleted files no longer exist
        foreach (string path in result.FilesToDelete)
        {
            Assert.False(File.Exists(Path.Combine(_tempDir, path)));
        }

        // Table data should still be readable (only the active file remains)
        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(1, totalRows);
    }

    [Fact]
    public async Task Vacuum_RespectsRetention()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Vacuum with 7-day retention — files just created won't qualify
        var result = await table.VacuumAsync(
            retentionPeriod: TimeSpan.FromDays(7),
            dryRun: true);

        // Recently created files should not be eligible for deletion
        Assert.Empty(result.FilesToDelete);
    }
}
