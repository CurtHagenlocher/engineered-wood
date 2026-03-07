using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Metadata;

public class MetadataEncoderTests
{
    [Fact]
    public void RoundTrip_MinimalFileMetaData()
    {
        var original = new FileMetaData
        {
            Version = 2,
            Schema = new[]
            {
                new SchemaElement { Name = "schema", NumChildren = 1 },
                new SchemaElement
                {
                    Name = "id",
                    Type = PhysicalType.Int32,
                    RepetitionType = FieldRepetitionType.Required,
                },
            },
            NumRows = 100,
            RowGroups = new[]
            {
                new RowGroup
                {
                    Columns = new[]
                    {
                        new ColumnChunk
                        {
                            FileOffset = 4,
                            MetaData = new ColumnMetaData
                            {
                                Type = PhysicalType.Int32,
                                Encodings = new[] { Encoding.Plain },
                                PathInSchema = new[] { "id" },
                                Codec = CompressionCodec.Uncompressed,
                                NumValues = 100,
                                TotalUncompressedSize = 400,
                                TotalCompressedSize = 400,
                                DataPageOffset = 4,
                            },
                        },
                    },
                    TotalByteSize = 400,
                    NumRows = 100,
                },
            },
        };

        var encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        Assert.Equal(original.Version, decoded.Version);
        Assert.Equal(original.NumRows, decoded.NumRows);
        Assert.Equal(original.Schema.Count, decoded.Schema.Count);
        Assert.Equal(original.RowGroups.Count, decoded.RowGroups.Count);
    }

    [Fact]
    public void RoundTrip_SchemaElements()
    {
        var original = CreateRichMetadata();
        var encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        for (int i = 0; i < original.Schema.Count; i++)
        {
            var orig = original.Schema[i];
            var dec = decoded.Schema[i];

            Assert.Equal(orig.Name, dec.Name);
            Assert.Equal(orig.Type, dec.Type);
            Assert.Equal(orig.TypeLength, dec.TypeLength);
            Assert.Equal(orig.RepetitionType, dec.RepetitionType);
            Assert.Equal(orig.NumChildren, dec.NumChildren);
            Assert.Equal(orig.ConvertedType, dec.ConvertedType);
            Assert.Equal(orig.Scale, dec.Scale);
            Assert.Equal(orig.Precision, dec.Precision);
            Assert.Equal(orig.FieldId, dec.FieldId);
        }
    }

    [Fact]
    public void RoundTrip_ColumnMetaData()
    {
        var original = CreateRichMetadata();
        var encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        var origCol = original.RowGroups[0].Columns[0].MetaData!;
        var decCol = decoded.RowGroups[0].Columns[0].MetaData!;

        Assert.Equal(origCol.Type, decCol.Type);
        Assert.Equal(origCol.Encodings, decCol.Encodings);
        Assert.Equal(origCol.PathInSchema, decCol.PathInSchema);
        Assert.Equal(origCol.Codec, decCol.Codec);
        Assert.Equal(origCol.NumValues, decCol.NumValues);
        Assert.Equal(origCol.TotalUncompressedSize, decCol.TotalUncompressedSize);
        Assert.Equal(origCol.TotalCompressedSize, decCol.TotalCompressedSize);
        Assert.Equal(origCol.DataPageOffset, decCol.DataPageOffset);
        Assert.Equal(origCol.DictionaryPageOffset, decCol.DictionaryPageOffset);
    }

    [Fact]
    public void RoundTrip_Statistics()
    {
        var original = CreateMetadataWithStatistics();
        var encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        var origStats = original.RowGroups[0].Columns[0].MetaData!.Statistics!;
        var decStats = decoded.RowGroups[0].Columns[0].MetaData!.Statistics!;

        Assert.Equal(origStats.Max, decStats.Max);
        Assert.Equal(origStats.Min, decStats.Min);
        Assert.Equal(origStats.NullCount, decStats.NullCount);
        Assert.Equal(origStats.DistinctCount, decStats.DistinctCount);
        Assert.Equal(origStats.MaxValue, decStats.MaxValue);
        Assert.Equal(origStats.MinValue, decStats.MinValue);
        Assert.Equal(origStats.IsMaxValueExact, decStats.IsMaxValueExact);
        Assert.Equal(origStats.IsMinValueExact, decStats.IsMinValueExact);
    }

    [Fact]
    public void RoundTrip_KeyValueMetadata()
    {
        var original = new FileMetaData
        {
            Version = 1,
            Schema = new[] { new SchemaElement { Name = "schema", NumChildren = 0 } },
            NumRows = 0,
            RowGroups = Array.Empty<RowGroup>(),
            KeyValueMetadata = new[]
            {
                new KeyValue("author", "EngineeredWood"),
                new KeyValue("version", "1.0"),
                new KeyValue("empty_value", null),
            },
            CreatedBy = "engineered-wood version 1.0.0",
        };

        var encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        Assert.NotNull(decoded.KeyValueMetadata);
        Assert.Equal(3, decoded.KeyValueMetadata!.Count);
        Assert.Equal("author", decoded.KeyValueMetadata[0].Key);
        Assert.Equal("EngineeredWood", decoded.KeyValueMetadata[0].Value);
        Assert.Equal("version", decoded.KeyValueMetadata[1].Key);
        Assert.Equal("1.0", decoded.KeyValueMetadata[1].Value);
        Assert.Equal("empty_value", decoded.KeyValueMetadata[2].Key);
        Assert.Null(decoded.KeyValueMetadata[2].Value);
        Assert.Equal("engineered-wood version 1.0.0", decoded.CreatedBy);
    }

    [Fact]
    public void RoundTrip_LogicalTypes()
    {
        var schema = new SchemaElement[]
        {
            new() { Name = "schema", NumChildren = 7 },
            new()
            {
                Name = "str_col", Type = PhysicalType.ByteArray,
                RepetitionType = FieldRepetitionType.Optional,
                LogicalType = new LogicalType.StringType(),
                ConvertedType = ConvertedType.Utf8,
            },
            new()
            {
                Name = "date_col", Type = PhysicalType.Int32,
                RepetitionType = FieldRepetitionType.Required,
                LogicalType = new LogicalType.DateType(),
                ConvertedType = ConvertedType.Date,
            },
            new()
            {
                Name = "ts_col", Type = PhysicalType.Int64,
                RepetitionType = FieldRepetitionType.Required,
                LogicalType = new LogicalType.TimestampType(true, TimeUnit.Micros),
                ConvertedType = ConvertedType.TimestampMicros,
            },
            new()
            {
                Name = "time_col", Type = PhysicalType.Int32,
                RepetitionType = FieldRepetitionType.Required,
                LogicalType = new LogicalType.TimeType(true, TimeUnit.Millis),
                ConvertedType = ConvertedType.TimeMillis,
            },
            new()
            {
                Name = "decimal_col", Type = PhysicalType.FixedLenByteArray,
                RepetitionType = FieldRepetitionType.Required,
                TypeLength = 16,
                LogicalType = new LogicalType.DecimalType(2, 18),
                ConvertedType = ConvertedType.Decimal,
                Scale = 2,
                Precision = 18,
            },
            new()
            {
                Name = "int8_col", Type = PhysicalType.Int32,
                RepetitionType = FieldRepetitionType.Required,
                LogicalType = new LogicalType.IntType(8, true),
                ConvertedType = ConvertedType.Int8,
            },
            new()
            {
                Name = "uuid_col", Type = PhysicalType.FixedLenByteArray,
                RepetitionType = FieldRepetitionType.Required,
                TypeLength = 16,
                LogicalType = new LogicalType.UuidType(),
            },
        };

        var original = new FileMetaData
        {
            Version = 2,
            Schema = schema,
            NumRows = 0,
            RowGroups = Array.Empty<RowGroup>(),
        };

        var encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        for (int i = 0; i < original.Schema.Count; i++)
        {
            Assert.Equal(original.Schema[i].LogicalType, decoded.Schema[i].LogicalType);
            Assert.Equal(original.Schema[i].ConvertedType, decoded.Schema[i].ConvertedType);
        }
    }

    [Fact]
    public void RoundTrip_SortingColumns()
    {
        var original = new FileMetaData
        {
            Version = 2,
            Schema = new[]
            {
                new SchemaElement { Name = "schema", NumChildren = 2 },
                new SchemaElement { Name = "a", Type = PhysicalType.Int32, RepetitionType = FieldRepetitionType.Required },
                new SchemaElement { Name = "b", Type = PhysicalType.Int64, RepetitionType = FieldRepetitionType.Required },
            },
            NumRows = 10,
            RowGroups = new[]
            {
                new RowGroup
                {
                    Columns = Array.Empty<ColumnChunk>(),
                    TotalByteSize = 0,
                    NumRows = 10,
                    SortingColumns = new[]
                    {
                        new SortingColumn(0, Descending: false, NullsFirst: true),
                        new SortingColumn(1, Descending: true, NullsFirst: false),
                    },
                    TotalCompressedSize = 0,
                    Ordinal = 0,
                },
            },
        };

        var encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        var rg = decoded.RowGroups[0];
        Assert.NotNull(rg.SortingColumns);
        Assert.Equal(2, rg.SortingColumns!.Count);
        Assert.Equal(0, rg.SortingColumns[0].ColumnIndex);
        Assert.False(rg.SortingColumns[0].Descending);
        Assert.True(rg.SortingColumns[0].NullsFirst);
        Assert.Equal(1, rg.SortingColumns[1].ColumnIndex);
        Assert.True(rg.SortingColumns[1].Descending);
        Assert.False(rg.SortingColumns[1].NullsFirst);
        Assert.Equal((short)0, rg.Ordinal);
        Assert.Equal(0L, rg.TotalCompressedSize);
    }

    [Fact]
    public void RoundTrip_MultipleEncodings()
    {
        var meta = new FileMetaData
        {
            Version = 2,
            Schema = new[]
            {
                new SchemaElement { Name = "schema", NumChildren = 1 },
                new SchemaElement { Name = "col", Type = PhysicalType.Int32, RepetitionType = FieldRepetitionType.Required },
            },
            NumRows = 50,
            RowGroups = new[]
            {
                new RowGroup
                {
                    Columns = new[]
                    {
                        new ColumnChunk
                        {
                            FileOffset = 4,
                            MetaData = new ColumnMetaData
                            {
                                Type = PhysicalType.Int32,
                                Encodings = new[] { Encoding.Plain, Encoding.RleDictionary, Encoding.DeltaBinaryPacked },
                                PathInSchema = new[] { "col" },
                                Codec = CompressionCodec.Zstd,
                                NumValues = 50,
                                TotalUncompressedSize = 200,
                                TotalCompressedSize = 150,
                                DataPageOffset = 100,
                                DictionaryPageOffset = 4,
                            },
                        },
                    },
                    TotalByteSize = 200,
                    NumRows = 50,
                },
            },
        };

        var encoded = MetadataEncoder.EncodeFileMetaData(meta);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        var col = decoded.RowGroups[0].Columns[0].MetaData!;
        Assert.Equal(3, col.Encodings.Count);
        Assert.Equal(Encoding.Plain, col.Encodings[0]);
        Assert.Equal(Encoding.RleDictionary, col.Encodings[1]);
        Assert.Equal(Encoding.DeltaBinaryPacked, col.Encodings[2]);
        Assert.Equal(CompressionCodec.Zstd, col.Codec);
        Assert.Equal(4L, col.DictionaryPageOffset);
        Assert.Equal(100L, col.DataPageOffset);
    }

    [Fact]
    public void RoundTrip_RealFileFooter()
    {
        // Decode a real file's footer, re-encode it, and decode again.
        // This verifies we can faithfully reproduce what we read.
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var original = MetadataDecoder.DecodeFileMetaData(footerBytes);

        var reEncoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(reEncoded);

        Assert.Equal(original.Version, decoded.Version);
        Assert.Equal(original.NumRows, decoded.NumRows);
        Assert.Equal(original.Schema.Count, decoded.Schema.Count);
        Assert.Equal(original.RowGroups.Count, decoded.RowGroups.Count);

        for (int i = 0; i < original.Schema.Count; i++)
        {
            Assert.Equal(original.Schema[i].Name, decoded.Schema[i].Name);
            Assert.Equal(original.Schema[i].Type, decoded.Schema[i].Type);
            Assert.Equal(original.Schema[i].RepetitionType, decoded.Schema[i].RepetitionType);
        }

        for (int rg = 0; rg < original.RowGroups.Count; rg++)
        {
            Assert.Equal(original.RowGroups[rg].NumRows, decoded.RowGroups[rg].NumRows);
            Assert.Equal(original.RowGroups[rg].Columns.Count, decoded.RowGroups[rg].Columns.Count);

            for (int c = 0; c < original.RowGroups[rg].Columns.Count; c++)
            {
                var origMeta = original.RowGroups[rg].Columns[c].MetaData!;
                var decMeta = decoded.RowGroups[rg].Columns[c].MetaData!;

                Assert.Equal(origMeta.Type, decMeta.Type);
                Assert.Equal(origMeta.Codec, decMeta.Codec);
                Assert.Equal(origMeta.NumValues, decMeta.NumValues);
                Assert.Equal(origMeta.DataPageOffset, decMeta.DataPageOffset);
            }
        }
    }

    // --- Helper methods ---

    private static FileMetaData CreateRichMetadata()
    {
        return new FileMetaData
        {
            Version = 2,
            Schema = new[]
            {
                new SchemaElement { Name = "schema", NumChildren = 3 },
                new SchemaElement
                {
                    Name = "id",
                    Type = PhysicalType.Int32,
                    RepetitionType = FieldRepetitionType.Required,
                    FieldId = 1,
                    LogicalType = new LogicalType.IntType(32, true),
                    ConvertedType = ConvertedType.Int32,
                },
                new SchemaElement
                {
                    Name = "amount",
                    Type = PhysicalType.FixedLenByteArray,
                    TypeLength = 16,
                    RepetitionType = FieldRepetitionType.Optional,
                    ConvertedType = ConvertedType.Decimal,
                    Scale = 2,
                    Precision = 38,
                    LogicalType = new LogicalType.DecimalType(2, 38),
                },
                new SchemaElement
                {
                    Name = "name",
                    Type = PhysicalType.ByteArray,
                    RepetitionType = FieldRepetitionType.Optional,
                    ConvertedType = ConvertedType.Utf8,
                    LogicalType = new LogicalType.StringType(),
                },
            },
            NumRows = 1000,
            RowGroups = new[]
            {
                new RowGroup
                {
                    Columns = new[]
                    {
                        new ColumnChunk
                        {
                            FileOffset = 4,
                            MetaData = new ColumnMetaData
                            {
                                Type = PhysicalType.Int32,
                                Encodings = new[] { Encoding.Plain, Encoding.RleDictionary },
                                PathInSchema = new[] { "id" },
                                Codec = CompressionCodec.Snappy,
                                NumValues = 1000,
                                TotalUncompressedSize = 4000,
                                TotalCompressedSize = 3500,
                                DataPageOffset = 100,
                                DictionaryPageOffset = 4,
                            },
                        },
                    },
                    TotalByteSize = 20000,
                    NumRows = 1000,
                    TotalCompressedSize = 18000,
                    Ordinal = 0,
                },
            },
            CreatedBy = "engineered-wood test",
        };
    }

    private static FileMetaData CreateMetadataWithStatistics()
    {
        return new FileMetaData
        {
            Version = 2,
            Schema = new[]
            {
                new SchemaElement { Name = "schema", NumChildren = 1 },
                new SchemaElement
                {
                    Name = "val",
                    Type = PhysicalType.Int32,
                    RepetitionType = FieldRepetitionType.Optional,
                },
            },
            NumRows = 50,
            RowGroups = new[]
            {
                new RowGroup
                {
                    Columns = new[]
                    {
                        new ColumnChunk
                        {
                            FileOffset = 4,
                            MetaData = new ColumnMetaData
                            {
                                Type = PhysicalType.Int32,
                                Encodings = new[] { Encoding.Plain },
                                PathInSchema = new[] { "val" },
                                Codec = CompressionCodec.Uncompressed,
                                NumValues = 50,
                                TotalUncompressedSize = 200,
                                TotalCompressedSize = 200,
                                DataPageOffset = 4,
                                Statistics = new Statistics
                                {
                                    Max = new byte[] { 0xFF, 0x00, 0x00, 0x00 },
                                    Min = new byte[] { 0x01, 0x00, 0x00, 0x00 },
                                    NullCount = 5,
                                    DistinctCount = 42,
                                    MaxValue = new byte[] { 0xFF, 0x00, 0x00, 0x00 },
                                    MinValue = new byte[] { 0x01, 0x00, 0x00, 0x00 },
                                    IsMaxValueExact = true,
                                    IsMinValueExact = true,
                                },
                            },
                        },
                    },
                    TotalByteSize = 200,
                    NumRows = 50,
                },
            },
        };
    }
}
