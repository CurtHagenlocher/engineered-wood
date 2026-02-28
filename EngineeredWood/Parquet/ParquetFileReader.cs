using System.Buffers;
using System.Buffers.Binary;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// Reads Parquet file metadata and schema via an <see cref="IRandomAccessFile"/>.
/// </summary>
public sealed class ParquetFileReader : IAsyncDisposable, IDisposable
{
    private static readonly byte[] Par1Magic = "PAR1"u8.ToArray();
    private const int MagicSize = 4;
    private const int FooterSuffixSize = 8; // 4-byte footer length + 4-byte magic
    private const int MinFileSize = MagicSize + FooterSuffixSize; // leading PAR1 + trailing 8

    private readonly IRandomAccessFile _file;
    private readonly bool _ownsFile;
    private FileMetaData? _metadata;
    private SchemaDescriptor? _schema;
    private bool _disposed;

    /// <summary>
    /// Creates a new reader over the given file.
    /// </summary>
    /// <param name="file">The random access file to read from.</param>
    /// <param name="ownsFile">If true, the file will be disposed when this reader is disposed.</param>
    public ParquetFileReader(IRandomAccessFile file, bool ownsFile = true)
    {
        _file = file;
        _ownsFile = ownsFile;
    }

    /// <summary>
    /// Reads and caches the file metadata from the Parquet footer.
    /// </summary>
    public async ValueTask<FileMetaData> ReadMetadataAsync(
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_metadata != null)
            return _metadata;

        long fileLength = await _file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        if (fileLength < MinFileSize)
            throw new ParquetFormatException(
                $"File is too small to be a valid Parquet file ({fileLength} bytes).");

        // Read the last 8 bytes: 4-byte footer length (LE) + 4-byte PAR1 magic
        using var suffixBuffer = await _file.ReadAsync(
            new FileRange(fileLength - FooterSuffixSize, FooterSuffixSize),
            cancellationToken).ConfigureAwait(false);

        var suffix = suffixBuffer.Memory.Span;

        // Validate trailing magic
        if (suffix[4] != Par1Magic[0] || suffix[5] != Par1Magic[1] ||
            suffix[6] != Par1Magic[2] || suffix[7] != Par1Magic[3])
            throw new ParquetFormatException("Invalid Parquet file: missing trailing PAR1 magic.");

        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(suffix);
        if (footerLength <= 0 || footerLength > fileLength - MinFileSize)
            throw new ParquetFormatException(
                $"Invalid Parquet footer length: {footerLength}.");

        // Read the footer (Thrift-encoded FileMetaData)
        long footerOffset = fileLength - FooterSuffixSize - footerLength;
        using var footerBuffer = await _file.ReadAsync(
            new FileRange(footerOffset, footerLength),
            cancellationToken).ConfigureAwait(false);

        _metadata = MetadataDecoder.DecodeFileMetaData(footerBuffer.Memory.Span);
        return _metadata;
    }

    /// <summary>
    /// Gets the schema descriptor, building it from cached metadata.
    /// </summary>
    public async ValueTask<SchemaDescriptor> GetSchemaAsync(
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_schema != null)
            return _schema;

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        _schema = new SchemaDescriptor(metadata.Schema);
        return _schema;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_ownsFile)
            await _file.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_ownsFile)
            _file.Dispose();
    }
}
