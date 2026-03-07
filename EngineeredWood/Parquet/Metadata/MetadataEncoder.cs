using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Encodes Parquet file metadata to Thrift Compact Protocol bytes.
/// Mirror of <see cref="MetadataDecoder"/> for the write path.
/// </summary>
internal static class MetadataEncoder
{
    /// <summary>
    /// Encodes a <see cref="FileMetaData"/> to Thrift Compact Protocol bytes.
    /// </summary>
    public static byte[] EncodeFileMetaData(FileMetaData metadata)
    {
        var writer = new ThriftCompactWriter(4096);
        WriteFileMetaData(writer, metadata);
        return writer.ToArray();
    }

    private static void WriteFileMetaData(ThriftCompactWriter writer, FileMetaData metadata)
    {
        writer.PushStruct();

        // field 1: version (i32)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(metadata.Version);

        // field 2: schema (list<SchemaElement>)
        writer.WriteFieldHeader(ThriftType.List, 2);
        WriteSchemaList(writer, metadata.Schema);

        // field 3: num_rows (i64)
        writer.WriteFieldHeader(ThriftType.I64, 3);
        writer.WriteZigZagInt64(metadata.NumRows);

        // field 4: row_groups (list<RowGroup>)
        writer.WriteFieldHeader(ThriftType.List, 4);
        WriteRowGroupList(writer, metadata.RowGroups);

        // field 5: key_value_metadata (optional list<KeyValue>)
        if (metadata.KeyValueMetadata is { Count: > 0 })
        {
            writer.WriteFieldHeader(ThriftType.List, 5);
            WriteKeyValueList(writer, metadata.KeyValueMetadata);
        }

        // field 6: created_by (optional string)
        if (metadata.CreatedBy != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 6);
            writer.WriteString(metadata.CreatedBy);
        }

        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteSchemaList(ThriftCompactWriter writer, IReadOnlyList<SchemaElement> schema)
    {
        writer.WriteListHeader(ThriftType.Struct, schema.Count);
        for (int i = 0; i < schema.Count; i++)
            WriteSchemaElement(writer, schema[i]);
    }

    private static void WriteSchemaElement(ThriftCompactWriter writer, SchemaElement element)
    {
        writer.PushStruct();

        // field 1: type (optional PhysicalType enum as i32)
        if (element.Type.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 1);
            writer.WriteZigZagInt32((int)element.Type.Value);
        }

        // field 2: type_length (optional i32)
        if (element.TypeLength.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 2);
            writer.WriteZigZagInt32(element.TypeLength.Value);
        }

        // field 3: repetition_type (optional enum as i32)
        if (element.RepetitionType.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 3);
            writer.WriteZigZagInt32((int)element.RepetitionType.Value);
        }

        // field 4: name (required string)
        writer.WriteFieldHeader(ThriftType.Binary, 4);
        writer.WriteString(element.Name);

        // field 5: num_children (optional i32)
        if (element.NumChildren.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 5);
            writer.WriteZigZagInt32(element.NumChildren.Value);
        }

        // field 6: converted_type (optional enum as i32)
        if (element.ConvertedType.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 6);
            writer.WriteZigZagInt32((int)element.ConvertedType.Value);
        }

        // field 7: scale (optional i32)
        if (element.Scale.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 7);
            writer.WriteZigZagInt32(element.Scale.Value);
        }

        // field 8: precision (optional i32)
        if (element.Precision.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 8);
            writer.WriteZigZagInt32(element.Precision.Value);
        }

        // field 9: field_id (optional i32)
        if (element.FieldId.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 9);
            writer.WriteZigZagInt32(element.FieldId.Value);
        }

        // field 10: logicalType (optional LogicalType union)
        if (element.LogicalType != null)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 10);
            WriteLogicalType(writer, element.LogicalType);
        }

        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteLogicalType(ThriftCompactWriter writer, LogicalType logicalType)
    {
        writer.PushStruct();

        switch (logicalType)
        {
            case LogicalType.StringType:
                writer.WriteFieldHeader(ThriftType.Struct, 1);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.MapType:
                writer.WriteFieldHeader(ThriftType.Struct, 2);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.ListType:
                writer.WriteFieldHeader(ThriftType.Struct, 3);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.EnumType:
                writer.WriteFieldHeader(ThriftType.Struct, 4);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.DecimalType dec:
                writer.WriteFieldHeader(ThriftType.Struct, 5);
                WriteDecimalLogicalType(writer, dec);
                break;
            case LogicalType.DateType:
                writer.WriteFieldHeader(ThriftType.Struct, 6);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.TimeType time:
                writer.WriteFieldHeader(ThriftType.Struct, 7);
                WriteTimeLogicalType(writer, time);
                break;
            case LogicalType.TimestampType ts:
                writer.WriteFieldHeader(ThriftType.Struct, 8);
                WriteTimestampLogicalType(writer, ts);
                break;
            case LogicalType.IntType intType:
                writer.WriteFieldHeader(ThriftType.Struct, 10);
                WriteIntLogicalType(writer, intType);
                break;
            case LogicalType.UnknownLogicalType unknown:
                writer.WriteFieldHeader(ThriftType.Struct, unknown.ThriftFieldId);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.JsonType:
                writer.WriteFieldHeader(ThriftType.Struct, 12);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.BsonType:
                writer.WriteFieldHeader(ThriftType.Struct, 13);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.UuidType:
                writer.WriteFieldHeader(ThriftType.Struct, 14);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.Float16Type:
                writer.WriteFieldHeader(ThriftType.Struct, 15);
                WriteEmptyStruct(writer);
                break;
        }

        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteEmptyStruct(ThriftCompactWriter writer)
    {
        writer.PushStruct();
        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteDecimalLogicalType(ThriftCompactWriter writer, LogicalType.DecimalType dec)
    {
        writer.PushStruct();
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(dec.Scale);
        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32(dec.Precision);
        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteTimeUnit(ThriftCompactWriter writer, TimeUnit unit)
    {
        writer.PushStruct();
        short fieldId = unit switch
        {
            TimeUnit.Millis => 1,
            TimeUnit.Micros => 2,
            TimeUnit.Nanos => 3,
            _ => throw new ArgumentOutOfRangeException(nameof(unit)),
        };
        writer.WriteFieldHeader(ThriftType.Struct, fieldId);
        WriteEmptyStruct(writer);
        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteTimeLogicalType(ThriftCompactWriter writer, LogicalType.TimeType time)
    {
        writer.PushStruct();
        writer.WriteBoolField(1, time.IsAdjustedToUtc);
        writer.WriteFieldHeader(ThriftType.Struct, 2);
        WriteTimeUnit(writer, time.Unit);
        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteTimestampLogicalType(ThriftCompactWriter writer, LogicalType.TimestampType ts)
    {
        writer.PushStruct();
        writer.WriteBoolField(1, ts.IsAdjustedToUtc);
        writer.WriteFieldHeader(ThriftType.Struct, 2);
        WriteTimeUnit(writer, ts.Unit);
        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteIntLogicalType(ThriftCompactWriter writer, LogicalType.IntType intType)
    {
        writer.PushStruct();
        // field 1: bitWidth (byte)
        writer.WriteFieldHeader(ThriftType.Byte, 1);
        writer.WriteByte((byte)intType.BitWidth);
        // field 2: isSigned (bool)
        writer.WriteBoolField(2, intType.IsSigned);
        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteRowGroupList(ThriftCompactWriter writer, IReadOnlyList<RowGroup> rowGroups)
    {
        writer.WriteListHeader(ThriftType.Struct, rowGroups.Count);
        for (int i = 0; i < rowGroups.Count; i++)
            WriteRowGroup(writer, rowGroups[i]);
    }

    private static void WriteRowGroup(ThriftCompactWriter writer, RowGroup rowGroup)
    {
        writer.PushStruct();

        // field 1: columns (list<ColumnChunk>)
        writer.WriteFieldHeader(ThriftType.List, 1);
        WriteColumnChunkList(writer, rowGroup.Columns);

        // field 2: total_byte_size (i64)
        writer.WriteFieldHeader(ThriftType.I64, 2);
        writer.WriteZigZagInt64(rowGroup.TotalByteSize);

        // field 3: num_rows (i64)
        writer.WriteFieldHeader(ThriftType.I64, 3);
        writer.WriteZigZagInt64(rowGroup.NumRows);

        // field 4: sorting_columns (optional list<SortingColumn>)
        if (rowGroup.SortingColumns is { Count: > 0 })
        {
            writer.WriteFieldHeader(ThriftType.List, 4);
            WriteSortingColumnList(writer, rowGroup.SortingColumns);
        }

        // field 6: total_compressed_size (optional i64)
        if (rowGroup.TotalCompressedSize.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 6);
            writer.WriteZigZagInt64(rowGroup.TotalCompressedSize.Value);
        }

        // field 7: ordinal (optional i16)
        if (rowGroup.Ordinal.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I16, 7);
            writer.WriteI16(rowGroup.Ordinal.Value);
        }

        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteColumnChunkList(ThriftCompactWriter writer, IReadOnlyList<ColumnChunk> chunks)
    {
        writer.WriteListHeader(ThriftType.Struct, chunks.Count);
        for (int i = 0; i < chunks.Count; i++)
            WriteColumnChunk(writer, chunks[i]);
    }

    private static void WriteColumnChunk(ThriftCompactWriter writer, ColumnChunk chunk)
    {
        writer.PushStruct();

        // field 1: file_path (optional string)
        if (chunk.FilePath != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 1);
            writer.WriteString(chunk.FilePath);
        }

        // field 2: file_offset (i64)
        writer.WriteFieldHeader(ThriftType.I64, 2);
        writer.WriteZigZagInt64(chunk.FileOffset);

        // field 3: meta_data (optional ColumnMetaData)
        if (chunk.MetaData != null)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 3);
            WriteColumnMetaData(writer, chunk.MetaData);
        }

        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteColumnMetaData(ThriftCompactWriter writer, ColumnMetaData meta)
    {
        writer.PushStruct();

        // field 1: type (PhysicalType)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32((int)meta.Type);

        // field 2: encodings (list<Encoding>)
        writer.WriteFieldHeader(ThriftType.List, 2);
        WriteEncodingList(writer, meta.Encodings);

        // field 3: path_in_schema (list<string>)
        writer.WriteFieldHeader(ThriftType.List, 3);
        WriteStringList(writer, meta.PathInSchema);

        // field 4: codec (CompressionCodec)
        writer.WriteFieldHeader(ThriftType.I32, 4);
        writer.WriteZigZagInt32((int)meta.Codec);

        // field 5: num_values (i64)
        writer.WriteFieldHeader(ThriftType.I64, 5);
        writer.WriteZigZagInt64(meta.NumValues);

        // field 6: total_uncompressed_size (i64)
        writer.WriteFieldHeader(ThriftType.I64, 6);
        writer.WriteZigZagInt64(meta.TotalUncompressedSize);

        // field 7: total_compressed_size (i64)
        writer.WriteFieldHeader(ThriftType.I64, 7);
        writer.WriteZigZagInt64(meta.TotalCompressedSize);

        // field 9: data_page_offset (i64)
        writer.WriteFieldHeader(ThriftType.I64, 9);
        writer.WriteZigZagInt64(meta.DataPageOffset);

        // field 10: index_page_offset (optional i64)
        if (meta.IndexPageOffset.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 10);
            writer.WriteZigZagInt64(meta.IndexPageOffset.Value);
        }

        // field 11: dictionary_page_offset (optional i64)
        if (meta.DictionaryPageOffset.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 11);
            writer.WriteZigZagInt64(meta.DictionaryPageOffset.Value);
        }

        // field 12: statistics (optional Statistics)
        if (meta.Statistics != null)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 12);
            WriteStatistics(writer, meta.Statistics);
        }

        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteStatistics(ThriftCompactWriter writer, Statistics stats)
    {
        writer.PushStruct();

        // field 1: max (optional binary, deprecated)
        if (stats.Max != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 1);
            writer.WriteBinary(stats.Max);
        }

        // field 2: min (optional binary, deprecated)
        if (stats.Min != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 2);
            writer.WriteBinary(stats.Min);
        }

        // field 3: null_count (optional i64)
        if (stats.NullCount.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 3);
            writer.WriteZigZagInt64(stats.NullCount.Value);
        }

        // field 4: distinct_count (optional i64)
        if (stats.DistinctCount.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 4);
            writer.WriteZigZagInt64(stats.DistinctCount.Value);
        }

        // field 5: max_value (optional binary)
        if (stats.MaxValue != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 5);
            writer.WriteBinary(stats.MaxValue);
        }

        // field 6: min_value (optional binary)
        if (stats.MinValue != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 6);
            writer.WriteBinary(stats.MinValue);
        }

        // field 7: is_max_value_exact (optional bool)
        if (stats.IsMaxValueExact.HasValue)
        {
            writer.WriteBoolField(7, stats.IsMaxValueExact.Value);
        }

        // field 8: is_min_value_exact (optional bool)
        if (stats.IsMinValueExact.HasValue)
        {
            writer.WriteBoolField(8, stats.IsMinValueExact.Value);
        }

        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteEncodingList(ThriftCompactWriter writer, IReadOnlyList<Encoding> encodings)
    {
        writer.WriteListHeader(ThriftType.I32, encodings.Count);
        for (int i = 0; i < encodings.Count; i++)
            writer.WriteZigZagInt32((int)encodings[i]);
    }

    private static void WriteStringList(ThriftCompactWriter writer, IReadOnlyList<string> strings)
    {
        writer.WriteListHeader(ThriftType.Binary, strings.Count);
        for (int i = 0; i < strings.Count; i++)
            writer.WriteString(strings[i]);
    }

    private static void WriteKeyValueList(ThriftCompactWriter writer, IReadOnlyList<KeyValue> kvs)
    {
        writer.WriteListHeader(ThriftType.Struct, kvs.Count);
        for (int i = 0; i < kvs.Count; i++)
            WriteKeyValue(writer, kvs[i]);
    }

    private static void WriteKeyValue(ThriftCompactWriter writer, KeyValue kv)
    {
        writer.PushStruct();
        writer.WriteFieldHeader(ThriftType.Binary, 1);
        writer.WriteString(kv.Key);
        if (kv.Value != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 2);
            writer.WriteString(kv.Value);
        }
        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void WriteSortingColumnList(ThriftCompactWriter writer, IReadOnlyList<SortingColumn> columns)
    {
        writer.WriteListHeader(ThriftType.Struct, columns.Count);
        for (int i = 0; i < columns.Count; i++)
            WriteSortingColumn(writer, columns[i]);
    }

    private static void WriteSortingColumn(ThriftCompactWriter writer, SortingColumn col)
    {
        writer.PushStruct();
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(col.ColumnIndex);
        writer.WriteBoolField(2, col.Descending);
        writer.WriteBoolField(3, col.NullsFirst);
        writer.WriteFieldStop();
        writer.PopStruct();
    }
}
