namespace EngineeredWood.IO.Local;

/// <summary>
/// <see cref="ISequentialFile"/> implementation for local files using <see cref="FileStream"/>.
/// </summary>
public sealed class LocalSequentialFile : ISequentialFile
{
    private readonly FileStream _stream;

    /// <summary>
    /// Creates a new file for writing. Overwrites existing files.
    /// </summary>
    public LocalSequentialFile(string path)
    {
        _stream = new FileStream(path, FileMode.Create, FileAccess.Write,
            FileShare.None, bufferSize: 81920, useAsync: false);
    }

    /// <inheritdoc/>
    public long Position => _stream.Position;

    /// <inheritdoc/>
    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _stream.Write(data.Span);
        return default;
    }

    /// <inheritdoc/>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _stream.Flush();
        return default;
    }

    public void Dispose() => _stream.Dispose();

    public async ValueTask DisposeAsync() => await _stream.DisposeAsync().ConfigureAwait(false);
}
