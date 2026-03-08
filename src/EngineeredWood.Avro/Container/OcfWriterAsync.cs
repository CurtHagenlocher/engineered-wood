using System.Security.Cryptography;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Container;

/// <summary>
/// Asynchronously writes Avro Object Container Format (OCF) files: header + data blocks.
/// </summary>
internal sealed class OcfWriterAsync : IAsyncDisposable
{
    private static readonly byte[] Magic = "Obj\x01"u8.ToArray();

    private readonly Stream _stream;
    private readonly AvroCodec _codec;
    private readonly byte[] _syncMarker;
    private bool _headerWritten;
    private bool _finished;

    public OcfWriterAsync(Stream stream, AvroCodec codec)
    {
        _stream = stream;
        _codec = codec;
        _syncMarker = RandomNumberGenerator.GetBytes(16);
    }

    public async ValueTask WriteHeaderAsync(AvroRecordSchema schema, CancellationToken ct = default)
    {
        if (_headerWritten) throw new InvalidOperationException("Header already written.");
        _headerWritten = true;

        // Magic
        await _stream.WriteAsync(Magic, ct).ConfigureAwait(false);

        // Metadata map
        var schemaJson = AvroSchemaWriter.ToJson(schema);
        var entries = new Dictionary<string, byte[]>
        {
            ["avro.schema"] = System.Text.Encoding.UTF8.GetBytes(schemaJson),
        };

        if (_codec != AvroCodec.Null)
            entries["avro.codec"] = System.Text.Encoding.UTF8.GetBytes(AvroCompression.CodecName(_codec));

        await WriteMetadataMapAsync(entries, ct).ConfigureAwait(false);

        // Sync marker
        await _stream.WriteAsync(_syncMarker, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a complete block of pre-encoded data with a known object count.
    /// Compression is CPU-bound and performed synchronously; only I/O is async.
    /// </summary>
    public async ValueTask WriteBlockAsync(ReadOnlyMemory<byte> encodedData, int objectCount,
        CancellationToken ct = default)
    {
        var compressed = AvroCompression.Compress(_codec, encodedData.Span);
        await WriteVarLongAsync(objectCount, ct).ConfigureAwait(false);
        await WriteVarLongAsync(compressed.Length, ct).ConfigureAwait(false);
        await _stream.WriteAsync(compressed, ct).ConfigureAwait(false);
        await _stream.WriteAsync(_syncMarker, ct).ConfigureAwait(false);
    }

    public async ValueTask FinishAsync(CancellationToken ct = default)
    {
        if (_finished) return;
        _finished = true;
        await _stream.FlushAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_finished) await FinishAsync().ConfigureAwait(false);
    }

    private async ValueTask WriteMetadataMapAsync(Dictionary<string, byte[]> entries,
        CancellationToken ct)
    {
        if (entries.Count > 0)
        {
            // Write block count
            await WriteVarLongAsync(entries.Count, ct).ConfigureAwait(false);
            foreach (var (key, value) in entries)
            {
                await WriteAvroStringAsync(key, ct).ConfigureAwait(false);
                await WriteAvroBytesAsync(value, ct).ConfigureAwait(false);
            }
        }
        // Terminate map with 0-count block
        await WriteVarLongAsync(0, ct).ConfigureAwait(false);
    }

    private async ValueTask WriteAvroStringAsync(string value, CancellationToken ct)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        await WriteVarLongAsync(bytes.Length, ct).ConfigureAwait(false);
        await _stream.WriteAsync(bytes, ct).ConfigureAwait(false);
    }

    private async ValueTask WriteAvroBytesAsync(ReadOnlyMemory<byte> value, CancellationToken ct)
    {
        await WriteVarLongAsync(value.Length, ct).ConfigureAwait(false);
        await _stream.WriteAsync(value, ct).ConfigureAwait(false);
    }

    private async ValueTask WriteVarLongAsync(long value, CancellationToken ct)
    {
        // Encode varint into a small stack buffer, then write async
        var buf = new byte[10]; // max varint size
        int written = Varint.WriteSigned(buf, value);
        await _stream.WriteAsync(buf.AsMemory(0, written), ct).ConfigureAwait(false);
    }
}
