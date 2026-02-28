using System.Buffers;
using Microsoft.Win32.SafeHandles;

namespace EngineeredWood.IO.Local;

/// <summary>
/// <see cref="IRandomAccessFile"/> implementation for local files using the
/// <see cref="RandomAccess"/> API. Supports fully concurrent offset-based reads
/// with no shared position cursor.
/// </summary>
public sealed class LocalRandomAccessFile : IRandomAccessFile
{
    private readonly SafeFileHandle _handle;
    private readonly BufferAllocator _allocator;
    private long _cachedLength = -1;

    public LocalRandomAccessFile(string path, BufferAllocator? allocator = null)
    {
        _allocator = allocator ?? PooledBufferAllocator.Default;
        _handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read,
            FileShare.Read, FileOptions.Asynchronous);
    }

    public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_cachedLength >= 0)
            return new ValueTask<long>(_cachedLength);

        long length = RandomAccess.GetLength(_handle);
        _cachedLength = length;
        return new ValueTask<long>(length);
    }

    public async ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default)
    {
        if (range.Length == 0)
            return _allocator.Allocate(0);

        IMemoryOwner<byte> buffer = _allocator.Allocate(checked((int)range.Length));
        try
        {
            await ReadExactAsync(buffer.Memory, range.Offset, cancellationToken)
                .ConfigureAwait(false);
            return buffer;
        }
        catch
        {
            buffer.Dispose();
            throw;
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

    private async ValueTask ReadExactAsync(
        Memory<byte> buffer, long offset, CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int bytesRead = await RandomAccess.ReadAsync(
                _handle, buffer[totalRead..], offset + totalRead, cancellationToken)
                .ConfigureAwait(false);

            if (bytesRead == 0)
                throw new IOException(
                    $"Unexpected end of file at offset {offset + totalRead}. " +
                    $"Expected {buffer.Length} bytes starting at offset {offset}.");

            totalRead += bytesRead;
        }
    }

    public void Dispose() => _handle.Dispose();

    public ValueTask DisposeAsync()
    {
        _handle.Dispose();
        return default;
    }
}
