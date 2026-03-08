using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;

namespace EngineeredWood.Avro.Data;

/// <summary>
/// Encodes Arrow RecordBatches into Avro binary-encoded records.
/// </summary>
internal sealed class RecordBatchEncoder
{
    private readonly AvroRecordSchema _schema;
    private readonly GrowableBuffer _rowBuffer = new(1024);

    public RecordBatchEncoder(AvroRecordSchema schema)
    {
        _schema = schema;
    }

    /// <summary>
    /// Encodes all rows from a RecordBatch into the provided buffer.
    /// Returns the number of rows encoded.
    /// </summary>
    public int Encode(RecordBatch batch, GrowableBuffer output)
    {
        var writer = new AvroBinaryWriter(_rowBuffer);

        for (int row = 0; row < batch.Length; row++)
        {
            _rowBuffer.Reset();
            EncodeRecord(writer, batch, row, _schema);
            output.Write(_rowBuffer.WrittenSpan);
        }

        return batch.Length;
    }

    private static void EncodeRecord(AvroBinaryWriter writer, RecordBatch batch, int row, AvroRecordSchema schema)
    {
        for (int col = 0; col < schema.Fields.Count; col++)
        {
            var fieldSchema = schema.Fields[col].Schema;
            var array = batch.Column(col);
            EncodeValue(writer, array, row, fieldSchema);
        }
    }

    internal static void EncodeValue(AvroBinaryWriter writer, IArrowArray array, int row, AvroSchemaNode schema)
    {
        switch (schema)
        {
            case AvroUnionSchema union when union.IsNullable(out var inner, out int nullIndex):
                if (!array.IsValid(row))
                {
                    writer.WriteUnionIndex(nullIndex);
                }
                else
                {
                    writer.WriteUnionIndex(nullIndex == 0 ? 1 : 0);
                    EncodeNonNullValue(writer, array, row, inner);
                }
                break;

            case AvroUnionSchema union:
                EncodeDenseUnion(writer, array, row, union);
                break;

            default:
                EncodeNonNullValue(writer, array, row, schema);
                break;
        }
    }

    private static void EncodeDenseUnion(AvroBinaryWriter writer, IArrowArray array, int row, AvroUnionSchema union)
    {
        var unionArray = (UnionArray)array;
        byte typeId = unionArray.TypeIds[row];
        writer.WriteUnionIndex(typeId);

        if (unionArray is DenseUnionArray denseUnion)
        {
            int offset = denseUnion.ValueOffsets[row];
            var childArray = denseUnion.Fields[typeId];
            EncodeNonNullValue(writer, childArray, offset, union.Branches[typeId]);
        }
        else
        {
            // SparseUnion: child index equals row
            var childArray = unionArray.Fields[typeId];
            EncodeNonNullValue(writer, childArray, row, union.Branches[typeId]);
        }
    }

    private static void EncodeNonNullValue(AvroBinaryWriter writer, IArrowArray array, int row, AvroSchemaNode schema)
    {
        switch (schema)
        {
            case AvroPrimitiveSchema p:
                EncodePrimitive(writer, array, row, p);
                break;
            case AvroEnumSchema:
                EncodeEnum(writer, array, row);
                break;
            case AvroFixedSchema f when f.LogicalType == "decimal":
                EncodeDecimalFixed(writer, array, row, f.Size);
                break;
            case AvroFixedSchema f:
                EncodeFixed(writer, array, row, f.Size);
                break;
            case AvroArraySchema a:
                EncodeArray(writer, array, row, a);
                break;
            case AvroMapSchema m:
                EncodeMap(writer, array, row, m);
                break;
            case AvroRecordSchema r:
                EncodeStruct(writer, array, row, r);
                break;
            default:
                throw new NotSupportedException($"Encoding Avro type {schema.Type} not yet supported.");
        }
    }

    private static void EncodePrimitive(AvroBinaryWriter writer, IArrowArray array, int row, AvroPrimitiveSchema schema)
    {
        // Logical types use the same base encoding but with different Arrow array types
        if (schema.LogicalType != null)
        {
            EncodePrimitiveWithLogicalType(writer, array, row, schema);
            return;
        }

        switch (schema.Type)
        {
            case AvroType.Null:
                writer.WriteNull();
                break;
            case AvroType.Boolean:
                writer.WriteBoolean(((BooleanArray)array).GetValue(row)!.Value);
                break;
            case AvroType.Int:
                writer.WriteInt(((Int32Array)array).GetValue(row)!.Value);
                break;
            case AvroType.Long:
                writer.WriteLong(((Int64Array)array).GetValue(row)!.Value);
                break;
            case AvroType.Float:
                writer.WriteFloat(((FloatArray)array).GetValue(row)!.Value);
                break;
            case AvroType.Double:
                writer.WriteDouble(((DoubleArray)array).GetValue(row)!.Value);
                break;
            case AvroType.String:
                writer.WriteString(((StringArray)array).GetString(row)!);
                break;
            case AvroType.Bytes:
                writer.WriteBytes(((BinaryArray)array).GetBytes(row));
                break;
            default:
                throw new NotSupportedException($"Encoding Avro primitive {schema.Type} not supported.");
        }
    }

    private static void EncodePrimitiveWithLogicalType(
        AvroBinaryWriter writer, IArrowArray array, int row, AvroPrimitiveSchema schema)
    {
        switch (schema.LogicalType)
        {
            case "date":
                writer.WriteInt(((Date32Array)array).GetValue(row)!.Value);
                break;
            case "time-millis":
                writer.WriteInt(((Time32Array)array).GetValue(row)!.Value);
                break;
            case "time-micros":
                writer.WriteLong(((Time64Array)array).GetValue(row)!.Value);
                break;
            case "timestamp-millis" or "local-timestamp-millis":
                writer.WriteLong(((TimestampArray)array).GetValue(row)!.Value);
                break;
            case "timestamp-micros" or "local-timestamp-micros":
                writer.WriteLong(((TimestampArray)array).GetValue(row)!.Value);
                break;
            case "timestamp-nanos" or "local-timestamp-nanos":
                writer.WriteLong(((TimestampArray)array).GetValue(row)!.Value);
                break;
            case "decimal":
                EncodeDecimalBytes(writer, array, row);
                break;
            case "uuid":
                writer.WriteString(((StringArray)array).GetString(row)!);
                break;
            default:
                // Unknown logical type: fall through to base encoding
                EncodePrimitive(writer, array, row,
                    new AvroPrimitiveSchema(schema.Type));
                break;
        }
    }

    private static void EncodeEnum(AvroBinaryWriter writer, IArrowArray array, int row)
    {
        var dictArray = (DictionaryArray)array;
        var indices = (Int32Array)dictArray.Indices;
        writer.WriteInt(indices.GetValue(row)!.Value);
    }

    private static void EncodeFixed(AvroBinaryWriter writer, IArrowArray array, int row, int size)
    {
        var fixedArray = (FixedSizeBinaryArray)array;
        writer.WriteFixed(fixedArray.GetBytes(row));
    }

    private static void EncodeArray(AvroBinaryWriter writer, IArrowArray array, int row, AvroArraySchema schema)
    {
        var listArray = (ListArray)array;
        int start = listArray.ValueOffsets[row];
        int length = listArray.GetValueLength(row);

        if (length > 0)
        {
            writer.WriteLong(length); // block count
            var values = listArray.Values;
            for (int i = start; i < start + length; i++)
                EncodeValue(writer, values, i, schema.Items);
        }
        writer.WriteLong(0); // terminating 0-count block
    }

    private static void EncodeMap(AvroBinaryWriter writer, IArrowArray array, int row, AvroMapSchema schema)
    {
        var mapArray = (MapArray)array;
        int start = mapArray.ValueOffsets[row];
        int length = mapArray.GetValueLength(row);

        if (length > 0)
        {
            writer.WriteLong(length);
            var keys = (StringArray)mapArray.Keys;
            var values = mapArray.Values;
            for (int i = start; i < start + length; i++)
            {
                writer.WriteString(keys.GetString(i)!);
                EncodeValue(writer, values, i, schema.Values);
            }
        }
        writer.WriteLong(0); // terminating 0-count block
    }

    /// <summary>Encodes a Decimal128 value as Avro fixed bytes (little-endian → big-endian).</summary>
    private static void EncodeDecimalFixed(AvroBinaryWriter writer, IArrowArray array, int row, int fixedSize)
    {
        var fixedArray = (FixedSizeBinaryArray)array;
        var leBytes = fixedArray.GetBytes(row); // 16-byte little-endian
        Span<byte> bigEndian = stackalloc byte[fixedSize];
        Decimal128ToAvroFixed(leBytes, bigEndian);
        writer.WriteFixed(bigEndian);
    }

    /// <summary>Encodes a Decimal128 value as Avro bytes (little-endian → big-endian, minimal representation).</summary>
    private static void EncodeDecimalBytes(AvroBinaryWriter writer, IArrowArray array, int row)
    {
        var fixedArray = (FixedSizeBinaryArray)array;
        var leBytes = fixedArray.GetBytes(row); // 16-byte little-endian
        writer.WriteBytes(Decimal128ToAvroBytes(leBytes));
    }

    /// <summary>Converts 16-byte little-endian Arrow Decimal128 to big-endian for Avro fixed encoding.</summary>
    private static void Decimal128ToAvroFixed(ReadOnlySpan<byte> leBytes, Span<byte> bigEndian)
    {
        // Reverse 16-byte LE to BE, sign-extend or truncate to target size
        byte signExtend = (leBytes[15] & 0x80) != 0 ? (byte)0xFF : (byte)0x00;
        bigEndian.Fill(signExtend);
        int copyLen = Math.Min(leBytes.Length, bigEndian.Length);
        for (int i = 0; i < copyLen; i++)
            bigEndian[bigEndian.Length - 1 - i] = leBytes[i];
    }

    /// <summary>Converts 16-byte little-endian Arrow Decimal128 to minimal big-endian bytes for Avro bytes encoding.</summary>
    private static ReadOnlySpan<byte> Decimal128ToAvroBytes(ReadOnlySpan<byte> leBytes)
    {
        // Convert LE to BE (full 16 bytes)
        Span<byte> bigEndian = stackalloc byte[16];
        for (int i = 0; i < 16; i++)
            bigEndian[i] = leBytes[15 - i];

        // Trim redundant leading bytes (matching sign extension)
        bool isNegative = (bigEndian[0] & 0x80) != 0;
        byte signByte = isNegative ? (byte)0xFF : (byte)0x00;
        int start = 0;
        while (start < 15 && bigEndian[start] == signByte)
        {
            // Keep at least one byte, and don't trim if next byte's sign bit differs
            if ((bigEndian[start + 1] & 0x80) != (signByte & 0x80))
                break;
            start++;
        }
        return bigEndian[start..].ToArray();
    }

    private static void EncodeStruct(AvroBinaryWriter writer, IArrowArray array, int row, AvroRecordSchema schema)
    {
        var structArray = (StructArray)array;
        for (int i = 0; i < schema.Fields.Count; i++)
        {
            var childArray = structArray.Fields[i];
            EncodeValue(writer, childArray, row, schema.Fields[i].Schema);
        }
    }
}
