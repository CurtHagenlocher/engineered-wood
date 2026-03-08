using System.Security.Cryptography;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Container;

/// <summary>
/// Writes Avro Object Container Format (OCF) files: header + data blocks.
/// </summary>
internal sealed class OcfWriter : IDisposable
{
    private static readonly byte[] Magic = "Obj\x01"u8.ToArray();

    private readonly Stream _stream;
    private readonly AvroCodec _codec;
    private readonly byte[] _syncMarker;
    private readonly GrowableBuffer _blockBuffer = new(4096);
    private long _blockObjectCount;
    private bool _headerWritten;
    private bool _finished;

    public OcfWriter(Stream stream, AvroCodec codec)
    {
        _stream = stream;
        _codec = codec;
        _syncMarker = RandomNumberGenerator.GetBytes(16);
    }

    public void WriteHeader(AvroRecordSchema schema)
    {
        if (_headerWritten) throw new InvalidOperationException("Header already written.");
        _headerWritten = true;

        // Magic
        _stream.Write(Magic);

        // Metadata map
        var schemaJson = AvroSchemaWriter.ToJson(schema);
        var entries = new Dictionary<string, byte[]>
        {
            ["avro.schema"] = System.Text.Encoding.UTF8.GetBytes(schemaJson),
        };

        if (_codec != AvroCodec.Null)
            entries["avro.codec"] = System.Text.Encoding.UTF8.GetBytes(AvroCompression.CodecName(_codec));

        WriteMetadataMap(_stream, entries);

        // Sync marker
        _stream.Write(_syncMarker);
    }

    /// <summary>
    /// Writes a complete block of pre-encoded data with a known object count.
    /// </summary>
    public void WriteBlock(ReadOnlySpan<byte> encodedData, int objectCount)
    {
        var compressed = AvroCompression.Compress(_codec, encodedData);
        WriteVarLong(_stream, objectCount);
        WriteVarLong(_stream, compressed.Length);
        _stream.Write(compressed);
        _stream.Write(_syncMarker);
    }

    /// <summary>
    /// Appends raw encoded record bytes to the current block buffer.
    /// </summary>
    public void AppendRecord(ReadOnlySpan<byte> encodedRecord)
    {
        _blockBuffer.Write(encodedRecord);
        _blockObjectCount++;
    }

    /// <summary>
    /// Flushes the current block to the output stream.
    /// </summary>
    public void FlushBlock()
    {
        if (_blockObjectCount == 0) return;

        var rawData = _blockBuffer.WrittenSpan;
        var compressed = AvroCompression.Compress(_codec, rawData);

        // Object count (long varint)
        WriteVarLong(_stream, _blockObjectCount);
        // Block byte size (long varint)
        WriteVarLong(_stream, compressed.Length);
        // Compressed data
        _stream.Write(compressed);
        // Sync marker
        _stream.Write(_syncMarker);

        _blockBuffer.Reset();
        _blockObjectCount = 0;
    }

    public void Finish()
    {
        if (_finished) return;
        _finished = true;
        FlushBlock();
        _stream.Flush();
    }

    public void Dispose()
    {
        if (!_finished) Finish();
    }

    private static void WriteMetadataMap(Stream stream, Dictionary<string, byte[]> entries)
    {
        if (entries.Count > 0)
        {
            // Write block count
            WriteVarLong(stream, entries.Count);
            foreach (var (key, value) in entries)
            {
                WriteAvroString(stream, key);
                WriteAvroBytes(stream, value);
            }
        }
        // Terminate map with 0-count block
        WriteVarLong(stream, 0);
    }

    private static void WriteAvroString(Stream stream, string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        WriteVarLong(stream, bytes.Length);
        stream.Write(bytes);
    }

    private static void WriteAvroBytes(Stream stream, ReadOnlySpan<byte> value)
    {
        WriteVarLong(stream, value.Length);
        stream.Write(value);
    }

    private static void WriteVarLong(Stream stream, long value)
    {
        Varint.WriteSigned(stream, value);
    }
}
