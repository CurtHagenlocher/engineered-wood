// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Tests;

public class SchemaConverterTests
{
    [Fact]
    public void RoundTrip_PrimitiveTypes()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, true))
            .Field(new Field("l", Int64Type.Default, false))
            .Field(new Field("i", Int32Type.Default, true))
            .Field(new Field("sh", Int16Type.Default, true))
            .Field(new Field("b", Int8Type.Default, true))
            .Field(new Field("f", FloatType.Default, true))
            .Field(new Field("d", DoubleType.Default, true))
            .Field(new Field("bool", BooleanType.Default, true))
            .Field(new Field("bin", BinaryType.Default, true))
            .Field(new Field("dt", Date32Type.Default, true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);
        var roundTripped = SchemaConverter.ToArrowSchema(deltaSchema);

        Assert.Equal(arrowSchema.FieldsList.Count, roundTripped.FieldsList.Count);
        for (int i = 0; i < arrowSchema.FieldsList.Count; i++)
        {
            Assert.Equal(arrowSchema.FieldsList[i].Name, roundTripped.FieldsList[i].Name);
            Assert.Equal(arrowSchema.FieldsList[i].IsNullable, roundTripped.FieldsList[i].IsNullable);
        }
    }

    [Fact]
    public void RoundTrip_TimestampTypes()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, (string?)"UTC"), true))
            .Field(new Field("ts_ntz", new TimestampType(TimeUnit.Microsecond, (string?)null), true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);

        Assert.Equal(2, deltaSchema.Fields.Count);
        Assert.Equal("timestamp", ((PrimitiveType)deltaSchema.Fields[0].Type).TypeName);
        Assert.Equal("timestamp_ntz", ((PrimitiveType)deltaSchema.Fields[1].Type).TypeName);
    }

    [Fact]
    public void RoundTrip_DecimalType()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", new Decimal128Type(10, 2), true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);
        Assert.Equal("decimal(10,2)", ((PrimitiveType)deltaSchema.Fields[0].Type).TypeName);

        var roundTripped = SchemaConverter.ToArrowSchema(deltaSchema);
        var decType = (Decimal128Type)roundTripped.FieldsList[0].DataType;
        Assert.Equal(10, decType.Precision);
        Assert.Equal(2, decType.Scale);
    }

    [Fact]
    public void RoundTrip_NestedStruct()
    {
        var innerFields = new List<Field>
        {
            new Field("x", Int32Type.Default, true),
            new Field("y", Int32Type.Default, true),
        };
        var structType = new Apache.Arrow.Types.StructType(innerFields);

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("point", structType, true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);
        Assert.IsType<Schema.StructType>(deltaSchema.Fields[0].Type);

        var innerStruct = (Schema.StructType)deltaSchema.Fields[0].Type;
        Assert.Equal(2, innerStruct.Fields.Count);
        Assert.Equal("x", innerStruct.Fields[0].Name);
        Assert.Equal("integer", ((PrimitiveType)innerStruct.Fields[0].Type).TypeName);
    }

    [Fact]
    public void RoundTrip_ListType()
    {
        var listType = new ListType(new Field("element", Int64Type.Default, true));

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ids", listType, true))
            .Build();

        var deltaSchema = SchemaConverter.FromArrowSchema(arrowSchema);
        Assert.IsType<Schema.ArrayType>(deltaSchema.Fields[0].Type);

        var arrayType = (Schema.ArrayType)deltaSchema.Fields[0].Type;
        Assert.Equal("long", ((PrimitiveType)arrayType.ElementType).TypeName);
        Assert.True(arrayType.ContainsNull);
    }

    [Fact]
    public void DeltaSchemaSerializer_RoundTrip()
    {
        var deltaSchema = new Schema.StructType
        {
            Fields =
            [
                new Schema.StructField
                {
                    Name = "id", Type = new PrimitiveType { TypeName = "long" },
                    Nullable = false,
                },
                new Schema.StructField
                {
                    Name = "name", Type = new PrimitiveType { TypeName = "string" },
                    Nullable = true,
                },
                new Schema.StructField
                {
                    Name = "amount",
                    Type = new PrimitiveType { TypeName = "decimal(10,2)" },
                    Nullable = true,
                },
            ],
        };

        string json = DeltaSchemaSerializer.Serialize(deltaSchema);
        var parsed = DeltaSchemaSerializer.Parse(json);

        Assert.Equal(3, parsed.Fields.Count);
        Assert.Equal("id", parsed.Fields[0].Name);
        Assert.Equal("long", ((PrimitiveType)parsed.Fields[0].Type).TypeName);
        Assert.False(parsed.Fields[0].Nullable);
        Assert.Equal("name", parsed.Fields[1].Name);
        Assert.Equal("string", ((PrimitiveType)parsed.Fields[1].Type).TypeName);
        Assert.True(parsed.Fields[1].Nullable);
        Assert.Equal("decimal(10,2)", ((PrimitiveType)parsed.Fields[2].Type).TypeName);
    }
}
