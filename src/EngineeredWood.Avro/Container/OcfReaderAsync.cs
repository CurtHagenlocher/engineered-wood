using EngineeredWood.Avro.Schema;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Container;

/// <summary>
/// Asynchronously reads Avro Object Container Format (OCF) files: header + data blocks.
/// </summary>
internal sealed class OcfReaderAsync : IAsyncDisposable
{
    private static readonly byte[] Magic = "Obj\x01"u8.ToArray();

    private readonly Stream _stream;
    private readonly byte[] _syncMarker;
    private readonly AvroCodec _codec;
    private readonly AvroRecordSchema _writerSchema;
    private readonly IReadOnlyDictionary<string, byte[]> _metadata;
    private bool _eof;

    public AvroRecordSchema WriterSchema => _writerSchema;
    public AvroCodec Codec => _codec;
    public IReadOnlyDictionary<string, byte[]> Metadata => _metadata;

    private OcfReaderAsync(Stream stream, AvroRecordSchema writerSchema, AvroCodec codec,
        byte[] syncMarker, IReadOnlyDictionary<string, byte[]> metadata)
    {
        _stream = stream;
        _writerSchema = writerSchema;
        _codec = codec;
        _syncMarker = syncMarker;
        _metadata = metadata;
    }

    public static async ValueTask<OcfReaderAsync> OpenAsync(Stream stream, CancellationToken ct = default)
    {
        // Read magic bytes
        var magic = new byte[4];
        await ReadExactAsync(stream, magic, ct).ConfigureAwait(false);
        if (!magic.AsSpan().SequenceEqual(Magic))
            throw new InvalidDataException("Not an Avro Object Container File (invalid magic).");

        // Read file metadata (Avro map<bytes>)
        var metadata = await ReadMetadataMapAsync(stream, ct).ConfigureAwait(false);

        // Read 16-byte sync marker
        var syncMarker = new byte[16];
        await ReadExactAsync(stream, syncMarker, ct).ConfigureAwait(false);

        // Extract schema
        if (!metadata.TryGetValue("avro.schema", out var schemaBytes))
            throw new InvalidDataException("Missing avro.schema in OCF header.");
        var schemaJson = System.Text.Encoding.UTF8.GetString(schemaBytes);
        var schemaNode = AvroSchemaParser.Parse(schemaJson);
        if (schemaNode is not AvroRecordSchema recordSchema)
            throw new InvalidDataException("Top-level Avro schema must be a record.");

        // Extract codec
        var codec = AvroCodec.Null;
        if (metadata.TryGetValue("avro.codec", out var codecBytes))
        {
            var codecName = System.Text.Encoding.UTF8.GetString(codecBytes);
            codec = AvroCompression.ParseCodecName(codecName);
        }

        return new OcfReaderAsync(stream, recordSchema, codec, syncMarker, metadata);
    }

    /// <summary>
    /// Reads the next data block asynchronously. Returns null on EOF.
    /// The returned array is the decompressed block data containing encoded records.
    /// </summary>
    public async ValueTask<(byte[] data, long objectCount)?> ReadBlockAsync(CancellationToken ct = default)
    {
        if (_eof) return null;

        // Read object count (long varint)
        long objectCount;
        try
        {
            objectCount = await ReadVarLongAsync(_stream, ct).ConfigureAwait(false);
        }
        catch (EndOfStreamException)
        {
            _eof = true;
            return null;
        }

        // Read block byte size (long varint)
        long blockSize = await ReadVarLongAsync(_stream, ct).ConfigureAwait(false);
        if (blockSize < 0 || blockSize > int.MaxValue)
            throw new InvalidDataException($"Invalid block size: {blockSize}");

        // Read compressed block data
        var blockData = new byte[(int)blockSize];
        await ReadExactAsync(_stream, blockData, ct).ConfigureAwait(false);

        // Read and verify sync marker
        var sync = new byte[16];
        await ReadExactAsync(_stream, sync, ct).ConfigureAwait(false);
        if (!sync.AsSpan().SequenceEqual(_syncMarker))
            throw new InvalidDataException("Sync marker mismatch in OCF data block.");

        // Decompress if needed (CPU-bound, stays synchronous)
        byte[] decompressed;
        if (_codec == AvroCodec.Null)
        {
            decompressed = blockData;
        }
        else
        {
            decompressed = AvroCompression.Decompress(_codec, blockData, 0);
        }

        return (decompressed, objectCount);
    }

    private static async ValueTask<Dictionary<string, byte[]>> ReadMetadataMapAsync(
        Stream stream, CancellationToken ct)
    {
        var metadata = new Dictionary<string, byte[]>();

        // Avro map encoding: blocks of (count, key-value pairs), terminated by 0-count block
        while (true)
        {
            long count = await ReadVarLongAsync(stream, ct).ConfigureAwait(false);
            if (count == 0) break;

            if (count < 0)
            {
                // Negative count: next long is block byte size (skip it)
                count = -count;
                await ReadVarLongAsync(stream, ct).ConfigureAwait(false); // block byte size
            }

            for (long i = 0; i < count; i++)
            {
                var key = await ReadAvroStringAsync(stream, ct).ConfigureAwait(false);
                var value = await ReadAvroBytesAsync(stream, ct).ConfigureAwait(false);
                metadata[key] = value;
            }
        }

        return metadata;
    }

    private static async ValueTask<long> ReadVarLongAsync(Stream stream, CancellationToken ct)
    {
        long result = 0;
        int shift = 0;
        var buf = new byte[1];
        while (true)
        {
            int read = await stream.ReadAsync(buf, ct).ConfigureAwait(false);
            if (read == 0) throw new EndOfStreamException("Unexpected end of stream reading varint.");
            result |= (long)(buf[0] & 0x7F) << shift;
            if ((buf[0] & 0x80) == 0)
                return Varint.ZigzagDecode(result);
            shift += 7;
        }
    }

    private static async ValueTask<string> ReadAvroStringAsync(Stream stream, CancellationToken ct)
    {
        long len = await ReadVarLongAsync(stream, ct).ConfigureAwait(false);
        if (len < 0 || len > int.MaxValue)
            throw new InvalidDataException($"Invalid string length: {len}");
        var buf = new byte[(int)len];
        await ReadExactAsync(stream, buf, ct).ConfigureAwait(false);
        return System.Text.Encoding.UTF8.GetString(buf);
    }

    private static async ValueTask<byte[]> ReadAvroBytesAsync(Stream stream, CancellationToken ct)
    {
        long len = await ReadVarLongAsync(stream, ct).ConfigureAwait(false);
        if (len < 0 || len > int.MaxValue)
            throw new InvalidDataException($"Invalid bytes length: {len}");
        var buf = new byte[(int)len];
        await ReadExactAsync(stream, buf, ct).ConfigureAwait(false);
        return buf;
    }

    private static async ValueTask ReadExactAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(total), ct).ConfigureAwait(false);
            if (read == 0) throw new EndOfStreamException("Unexpected end of stream.");
            total += read;
        }
    }

    public ValueTask DisposeAsync() => default; // We don't own the stream
}
