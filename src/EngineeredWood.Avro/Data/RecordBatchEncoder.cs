using Apache.Arrow;
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

    private static void EncodeValue(AvroBinaryWriter writer, IArrowArray array, int row, AvroSchemaNode schema)
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

            default:
                EncodeNonNullValue(writer, array, row, schema);
                break;
        }
    }

    private static void EncodeNonNullValue(AvroBinaryWriter writer, IArrowArray array, int row, AvroSchemaNode schema)
    {
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
                throw new NotSupportedException($"Encoding Avro type {schema.Type} not yet supported.");
        }
    }
}
