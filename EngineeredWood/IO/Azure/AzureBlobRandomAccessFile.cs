using System.Buffers;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="IRandomAccessFile"/> implementation for Azure Blob Storage.
/// Uses <see cref="BlobClient.DownloadStreamingAsync"/> with HTTP range requests.
/// Concurrent requests are throttled via a semaphore.
/// </summary>
public sealed class AzureBlobRandomAccessFile : IRandomAccessFile
{
    private readonly BlobClient _blobClient;
    private readonly BufferAllocator _allocator;
    private readonly SemaphoreSlim _semaphore;
    private readonly bool _ownsSemaphore;
    private long _cachedLength = -1;

    public AzureBlobRandomAccessFile(
        BlobClient blobClient,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16)
    {
        _blobClient = blobClient;
        _allocator = allocator ?? PooledBufferAllocator.Default;
        _semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        _ownsSemaphore = true;
    }

    /// <summary>
    /// Creates an instance with a pre-known file size, avoiding the initial
    /// <c>GetProperties</c> HEAD request. Useful when the size is already
    /// known from a listing or prior metadata fetch.
    /// </summary>
    public AzureBlobRandomAccessFile(
        BlobClient blobClient,
        long knownLength,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16)
        : this(blobClient, allocator, maxConcurrency)
    {
        _cachedLength = knownLength;
    }

    public async ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
    {
        if (_cachedLength >= 0)
            return _cachedLength;

        BlobProperties properties = await _blobClient
            .GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        _cachedLength = properties.ContentLength;
        return _cachedLength;
    }

    public async ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default)
    {
        if (range.Length == 0)
            return _allocator.Allocate(0);

        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await DownloadRangeAsync(range, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
    {
        if (ranges.Count == 0)
            return [];

        if (ranges.Count == 1)
        {
            IMemoryOwner<byte> single = await ReadAsync(ranges[0], cancellationToken)
                .ConfigureAwait(false);
            return [single];
        }

        var buffers = new IMemoryOwner<byte>[ranges.Count];
        try
        {
            var tasks = new Task[ranges.Count];
            for (int i = 0; i < ranges.Count; i++)
            {
                int index = i;
                FileRange range = ranges[index];
                tasks[index] = Task.Run(async () =>
                {
                    IMemoryOwner<byte> buf = await ReadAsync(range, cancellationToken)
                        .ConfigureAwait(false);
                    buffers[index] = buf;
                }, cancellationToken);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            return buffers;
        }
        catch
        {
            foreach (IMemoryOwner<byte>? buf in buffers)
                buf?.Dispose();
            throw;
        }
    }

    private async ValueTask<IMemoryOwner<byte>> DownloadRangeAsync(
        FileRange range, CancellationToken cancellationToken)
    {
        IMemoryOwner<byte> buffer = _allocator.Allocate(checked((int)range.Length));
        try
        {
            BlobDownloadStreamingResult result = await _blobClient.DownloadStreamingAsync(
                new BlobDownloadOptions
                {
                    Range = new global::Azure.HttpRange(range.Offset, range.Length),
                },
                cancellationToken).ConfigureAwait(false);

            await using Stream stream = result.Content;
            Memory<byte> memory = buffer.Memory;
            int totalRead = 0;
            while (totalRead < memory.Length)
            {
                int bytesRead = await stream.ReadAsync(
                    memory[totalRead..], cancellationToken).ConfigureAwait(false);

                if (bytesRead == 0)
                    throw new IOException(
                        $"Unexpected end of blob stream at offset {range.Offset + totalRead}. " +
                        $"Expected {range.Length} bytes starting at offset {range.Offset}.");

                totalRead += bytesRead;
            }

            return buffer;
        }
        catch
        {
            buffer.Dispose();
            throw;
        }
    }

    public void Dispose()
    {
        if (_ownsSemaphore)
            _semaphore.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }
}
