namespace EngineeredWood.IO.Local;

/// <summary>
/// <see cref="IOutputFile"/> implementation for local files using <see cref="FileStream"/>.
/// </summary>
public sealed class LocalOutputFile : IOutputFile
{
    private readonly FileStream _stream;
    private bool _disposed;

    public LocalOutputFile(string path)
    {
        _stream = new FileStream(path, FileMode.Create, FileAccess.Write,
            FileShare.None, bufferSize: 81920, useAsync: false);
    }

    /// <summary>
    /// Creates an instance wrapping an existing <see cref="FileStream"/>.
    /// The caller retains ownership unless <paramref name="ownsStream"/> is true.
    /// </summary>
    internal LocalOutputFile(FileStream stream, bool ownsStream = true)
    {
        _stream = stream;
        if (!ownsStream)
            _disposed = false; // normal path
    }

    public long Position => _stream.Position;

    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        // For local files, synchronous writes are faster (no thread-pool overhead).
        _stream.Write(data.Span);
        return default;
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        _stream.Flush();
        return default;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _stream.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }
}
