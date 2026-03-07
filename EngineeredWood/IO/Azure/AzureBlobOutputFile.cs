using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="IOutputFile"/> implementation for Azure Blob Storage.
/// Buffers writes in memory and stages them as blocks via <see cref="BlockBlobClient"/>,
/// then commits all blocks on dispose/flush.
/// </summary>
public sealed class AzureBlobOutputFile : IOutputFile
{
    private readonly BlockBlobClient _blockClient;
    private readonly BlobHttpHeaders? _headers;
    private readonly int _blockSize;
    private readonly MemoryStream _buffer;
    private readonly List<string> _blockIds = new();
    private long _position;
    private bool _committed;
    private bool _disposed;

    /// <param name="blockBlobClient">The block blob to write to.</param>
    /// <param name="blockSize">
    /// Size in bytes at which buffered data is staged as a block.
    /// Default: 4 MB (Azure's minimum recommended block size).
    /// </param>
    /// <param name="headers">Optional HTTP headers (e.g., content type) for the final blob.</param>
    public AzureBlobOutputFile(
        BlockBlobClient blockBlobClient,
        int blockSize = 4 * 1024 * 1024,
        BlobHttpHeaders? headers = null)
    {
        _blockClient = blockBlobClient;
        _blockSize = blockSize;
        _headers = headers;
        _buffer = new MemoryStream(blockSize);
    }

    public long Position => _position;

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _buffer.Write(data.Span);
        _position += data.Length;

        if (_buffer.Position >= _blockSize)
            await StageBufferAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_buffer.Position > 0)
            await StageBufferAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task StageBufferAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Position == 0)
            return;

        string blockId = Convert.ToBase64String(
            BitConverter.GetBytes(_blockIds.Count));
        _blockIds.Add(blockId);

        _buffer.Position = 0;
        await _blockClient.StageBlockAsync(
            blockId, _buffer, cancellationToken: cancellationToken).ConfigureAwait(false);

        _buffer.SetLength(0);
    }

    private async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_committed) return;
        _committed = true;

        if (_buffer.Position > 0)
            await StageBufferAsync(cancellationToken).ConfigureAwait(false);

        if (_blockIds.Count > 0)
        {
            await _blockClient.CommitBlockListAsync(
                _blockIds,
                _headers,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await CommitAsync(CancellationToken.None).ConfigureAwait(false);
        _buffer.Dispose();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Synchronous dispose cannot commit — caller should use DisposeAsync.
        _buffer.Dispose();
    }
}
