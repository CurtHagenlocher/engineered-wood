namespace EngineeredWood.IO;

/// <summary>
/// Provides sequential write access to a file or blob.
/// Unlike <see cref="IRandomAccessFile"/>, writes are append-only
/// and the position advances automatically.
/// </summary>
public interface IOutputFile : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets the current write position (total bytes written so far).
    /// </summary>
    long Position { get; }

    /// <summary>
    /// Writes the specified bytes at the current position and advances it.
    /// </summary>
    ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);

    /// <summary>
    /// Flushes any buffered data to the underlying storage.
    /// </summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);
}
