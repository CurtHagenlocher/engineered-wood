// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Tests;

public class ColumnMappingTests
{
    [Fact]
    public void GetMode_None_Default()
    {
        Assert.Equal(ColumnMappingMode.None, ColumnMapping.GetMode(null));
        Assert.Equal(ColumnMappingMode.None,
            ColumnMapping.GetMode(new Dictionary<string, string>()));
    }

    [Theory]
    [InlineData("none", ColumnMappingMode.None)]
    [InlineData("id", ColumnMappingMode.Id)]
    [InlineData("name", ColumnMappingMode.Name)]
    public void GetMode_FromConfiguration(string modeStr, ColumnMappingMode expected)
    {
        var config = new Dictionary<string, string>
        {
            { ColumnMapping.ModeKey, modeStr },
        };
        Assert.Equal(expected, ColumnMapping.GetMode(config));
    }

    [Fact]
    public void AssignColumnMapping_AssignsIdsAndPhysicalNames()
    {
        var schema = new StructType
        {
            Fields =
            [
                new StructField
                {
                    Name = "id", Type = new PrimitiveType { TypeName = "long" },
                    Nullable = false,
                },
                new StructField
                {
                    Name = "name", Type = new PrimitiveType { TypeName = "string" },
                    Nullable = true,
                },
            ],
        };

        var (mapped, maxId) = ColumnMapping.AssignColumnMapping(schema);

        Assert.Equal(2, maxId);
        Assert.Equal(2, mapped.Fields.Count);

        // First field
        Assert.Equal("id", mapped.Fields[0].Name);
        Assert.NotNull(mapped.Fields[0].Metadata);
        Assert.True(mapped.Fields[0].Metadata!.ContainsKey(ColumnMapping.FieldIdKey));
        Assert.True(mapped.Fields[0].Metadata!.ContainsKey(ColumnMapping.PhysicalNameKey));
        Assert.Equal("1", mapped.Fields[0].Metadata![ColumnMapping.FieldIdKey]);
        Assert.StartsWith("col-", mapped.Fields[0].Metadata![ColumnMapping.PhysicalNameKey]);

        // Second field
        Assert.Equal("2", mapped.Fields[1].Metadata![ColumnMapping.FieldIdKey]);
        Assert.StartsWith("col-", mapped.Fields[1].Metadata![ColumnMapping.PhysicalNameKey]);

        // Physical names should be different
        Assert.NotEqual(
            mapped.Fields[0].Metadata![ColumnMapping.PhysicalNameKey],
            mapped.Fields[1].Metadata![ColumnMapping.PhysicalNameKey]);
    }

    [Fact]
    public void BuildPhysicalToLogicalMap_NameMode()
    {
        var schema = new StructType
        {
            Fields =
            [
                new StructField
                {
                    Name = "id",
                    Type = new PrimitiveType { TypeName = "long" },
                    Nullable = false,
                    Metadata = new Dictionary<string, string>
                    {
                        { ColumnMapping.FieldIdKey, "1" },
                        { ColumnMapping.PhysicalNameKey, "col-abc123" },
                    },
                },
                new StructField
                {
                    Name = "value",
                    Type = new PrimitiveType { TypeName = "string" },
                    Nullable = true,
                    Metadata = new Dictionary<string, string>
                    {
                        { ColumnMapping.FieldIdKey, "2" },
                        { ColumnMapping.PhysicalNameKey, "col-def456" },
                    },
                },
            ],
        };

        var map = ColumnMapping.BuildPhysicalToLogicalMap(schema, ColumnMappingMode.Name);
        Assert.Equal(2, map.Count);
        Assert.Equal("id", map["col-abc123"]);
        Assert.Equal("value", map["col-def456"]);
    }

    [Fact]
    public void BuildPhysicalToLogicalMap_NoneMode_ReturnsEmpty()
    {
        var schema = new StructType
        {
            Fields =
            [
                new StructField
                {
                    Name = "id",
                    Type = new PrimitiveType { TypeName = "long" },
                    Nullable = false,
                },
            ],
        };

        var map = ColumnMapping.BuildPhysicalToLogicalMap(schema, ColumnMappingMode.None);
        Assert.Empty(map);
    }

    [Fact]
    public void RenameColumns_ReplacesPhysicalWithLogical()
    {
        var physicalSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("col-abc", Apache.Arrow.Types.Int64Type.Default, false))
            .Field(new Apache.Arrow.Field("col-def", Apache.Arrow.Types.StringType.Default, true))
            .Build();

        var ids = new Apache.Arrow.Int64Array.Builder().Append(1).Append(2).Build();
        var vals = new Apache.Arrow.StringArray.Builder().Append("a").Append("b").Build();
        var batch = new Apache.Arrow.RecordBatch(physicalSchema,
            [ids, vals], 2);

        var map = new Dictionary<string, string>
        {
            { "col-abc", "id" },
            { "col-def", "value" },
        };

        var result = ColumnMapping.RenameColumns(batch, map);

        Assert.Equal("id", result.Schema.FieldsList[0].Name);
        Assert.Equal("value", result.Schema.FieldsList[1].Name);
        Assert.Equal(2, result.Length);
    }

    [Fact]
    public void GetMaxColumnId_ReturnsHighest()
    {
        var schema = new StructType
        {
            Fields =
            [
                new StructField
                {
                    Name = "a",
                    Type = new PrimitiveType { TypeName = "long" },
                    Nullable = false,
                    Metadata = new Dictionary<string, string>
                    {
                        { ColumnMapping.FieldIdKey, "5" },
                    },
                },
                new StructField
                {
                    Name = "b",
                    Type = new PrimitiveType { TypeName = "string" },
                    Nullable = true,
                    Metadata = new Dictionary<string, string>
                    {
                        { ColumnMapping.FieldIdKey, "12" },
                    },
                },
            ],
        };

        Assert.Equal(12, ColumnMapping.GetMaxColumnId(schema));
    }
}
