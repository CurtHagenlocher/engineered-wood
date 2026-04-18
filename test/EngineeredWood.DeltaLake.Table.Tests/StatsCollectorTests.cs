// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.Stats;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class StatsCollectorTests
{
    [Fact]
    public void Collect_IntegerColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder()
            .Append(10).Append(5).Append(20).Build();
        var batch = new RecordBatch(schema, [ids], 3);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal(3, doc.RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal(5, doc.RootElement.GetProperty("minValues").GetProperty("id").GetInt64());
        Assert.Equal(20, doc.RootElement.GetProperty("maxValues").GetProperty("id").GetInt64());
        Assert.Equal(0, doc.RootElement.GetProperty("nullCount").GetProperty("id").GetInt64());
    }

    [Fact]
    public void Collect_StringColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, true))
            .Build();

        var names = new StringArray.Builder()
            .Append("charlie").Append("alice").AppendNull().Append("bob").Build();
        var batch = new RecordBatch(schema, [names], 4);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal(4, doc.RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal("alice", doc.RootElement.GetProperty("minValues").GetProperty("name").GetString());
        Assert.Equal("charlie", doc.RootElement.GetProperty("maxValues").GetProperty("name").GetString());
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("name").GetInt64());
    }

    [Fact]
    public void Collect_MultipleColumns()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", DoubleType.Default, true))
            .Build();

        var ids = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var values = new DoubleArray.Builder().Append(1.5).AppendNull().Append(3.7).Build();
        var batch = new RecordBatch(schema, [ids, values], 3);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal(1, doc.RootElement.GetProperty("minValues").GetProperty("id").GetInt32());
        Assert.Equal(3, doc.RootElement.GetProperty("maxValues").GetProperty("id").GetInt32());
        Assert.Equal(1.5, doc.RootElement.GetProperty("minValues").GetProperty("value").GetDouble(), 5);
        Assert.Equal(3.7, doc.RootElement.GetProperty("maxValues").GetProperty("value").GetDouble(), 5);
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("value").GetInt64());
    }

    [Fact]
    public void Collect_EmptyBatch_ReturnsNull()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder().Build();
        var batch = new RecordBatch(schema, [ids], 0);

        Assert.Null(StatsCollector.Collect(batch));
    }
}
