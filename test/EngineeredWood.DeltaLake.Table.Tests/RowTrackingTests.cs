// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class RowTrackingTests : IDisposable
{
    private readonly string _tempDir;

    public RowTrackingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateRowTrackingTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["rowTracking"],
            },
            new MetadataAction
            {
                Id = "rt-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { RowTrackingConfig.EnableKey, "true" },
                },
            },
        });

        return await DeltaTable.OpenAsync(fs);
    }

    [Fact]
    public async Task Write_AssignsBaseRowId()
    {
        await using var table = await CreateRowTrackingTable();
        var schema = table.ArrowSchema;

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var values = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var batch = new RecordBatch(schema, [ids, values], 3);

        await table.WriteAsync([batch]);

        // Check that AddFile has baseRowId set
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        Assert.NotNull(addFile.BaseRowId);
        Assert.Equal(0L, addFile.BaseRowId); // First file starts at 0
        Assert.NotNull(addFile.DefaultRowCommitVersion);
        Assert.Equal(1L, addFile.DefaultRowCommitVersion); // Commit version 1
    }

    [Fact]
    public async Task Write_MultipleFiles_IncrementingRowIds()
    {
        await using var table = await CreateRowTrackingTable();
        var schema = table.ArrowSchema;

        // First write: 3 rows
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Append(3).Build(),
             new StringArray.Builder().Append("a").Append("b").Append("c").Build()], 3);
        await table.WriteAsync([batch1]);

        // Second write: 2 rows
        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(4).Append(5).Build(),
             new StringArray.Builder().Append("d").Append("e").Build()], 2);
        await table.WriteAsync([batch2]);

        var files = table.CurrentSnapshot.ActiveFiles.Values
            .OrderBy(f => f.BaseRowId)
            .ToList();

        Assert.Equal(2, files.Count);
        Assert.Equal(0L, files[0].BaseRowId);   // First file: rows 0-2
        Assert.Equal(3L, files[1].BaseRowId);   // Second file: rows 3-4
    }

    [Fact]
    public async Task Write_MultipleFilesInOneBatch_CorrectRowIds()
    {
        await using var table = await CreateRowTrackingTable();
        var schema = table.ArrowSchema;

        // Write two batches in one commit
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build(),
             new StringArray.Builder().Append("a").Append("b").Build()], 2);
        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(3).Build(),
             new StringArray.Builder().Append("c").Build()], 1);

        await table.WriteAsync([batch1, batch2]);

        var files = table.CurrentSnapshot.ActiveFiles.Values
            .OrderBy(f => f.BaseRowId)
            .ToList();

        Assert.Equal(2, files.Count);
        Assert.Equal(0L, files[0].BaseRowId);
        Assert.Equal(2L, files[1].BaseRowId);
    }

    [Fact]
    public async Task Read_StripsInternalRowIdColumn()
    {
        await using var table = await CreateRowTrackingTable();
        var schema = table.ArrowSchema;

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var values = new StringArray.Builder().Append("a").Append("b").Build();
        var batch = new RecordBatch(schema, [ids, values], 2);

        await table.WriteAsync([batch]);

        // Read back — should NOT have __delta_row_id column
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(2, readBatches[0].ColumnCount); // id + value, no __delta_row_id
        Assert.Equal("id", readBatches[0].Schema.FieldsList[0].Name);
        Assert.Equal("value", readBatches[0].Schema.FieldsList[1].Name);
    }

    [Fact]
    public async Task HighWaterMark_IncreasesAcrossCommits()
    {
        await using var table = await CreateRowTrackingTable();
        var schema = table.ArrowSchema;

        Assert.Equal(0L, table.CurrentSnapshot.RowIdHighWaterMark);

        // Write 3 rows
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Append(3).Build(),
             new StringArray.Builder().Append("a").Append("b").Append("c").Build()], 3);
        await table.WriteAsync([batch1]);

        Assert.Equal(3L, table.CurrentSnapshot.RowIdHighWaterMark);

        // Write 2 more rows
        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(4).Append(5).Build(),
             new StringArray.Builder().Append("d").Append("e").Build()], 2);
        await table.WriteAsync([batch2]);

        Assert.Equal(5L, table.CurrentSnapshot.RowIdHighWaterMark);
    }

    [Fact]
    public async Task Compaction_PreservesRowTracking()
    {
        await using var table = await CreateRowTrackingTable();
        var schema = table.ArrowSchema;

        // Write 3 small files
        for (int i = 0; i < 3; i++)
        {
            var batch = new RecordBatch(schema,
                [new Int64Array.Builder().Append(i * 10 + 1).Build(),
                 new StringArray.Builder().Append($"val_{i}").Build()], 1);
            await table.WriteAsync([batch]);
        }

        Assert.Equal(3, table.CurrentSnapshot.FileCount);
        long hwmBeforeCompact = table.CurrentSnapshot.RowIdHighWaterMark;

        // Compact
        await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue,
        });

        // Compacted files should have baseRowId and defaultRowCommitVersion
        foreach (var addFile in table.CurrentSnapshot.ActiveFiles.Values)
        {
            Assert.NotNull(addFile.BaseRowId);
            Assert.NotNull(addFile.DefaultRowCommitVersion);
        }

        // High water mark should have increased (new baseRowIds assigned)
        Assert.True(table.CurrentSnapshot.RowIdHighWaterMark >= hwmBeforeCompact);

        // Data should still be readable
        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(3, totalRows);
    }

    [Fact]
    public async Task NonRowTracking_NoBaseRowId()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var batch = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch]);

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        Assert.Null(addFile.BaseRowId);
        Assert.Null(addFile.DefaultRowCommitVersion);
    }

    [Fact]
    public async Task ProtocolFeature_RowTracking_Accepted()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["rowTracking"],
            },
            new MetadataAction
            {
                Id = "rt-feat",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(7, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }
}
