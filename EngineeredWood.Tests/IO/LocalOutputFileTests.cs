using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Tests.IO;

public class LocalOutputFileTests : IDisposable
{
    private readonly string _tempFile;

    public LocalOutputFileTests()
    {
        _tempFile = Path.GetTempFileName();
        // Delete the file created by GetTempFileName so LocalOutputFile creates it fresh
        File.Delete(_tempFile);
    }

    public void Dispose()
    {
        if (File.Exists(_tempFile))
            File.Delete(_tempFile);
    }

    [Fact]
    public async Task WriteAsync_WritesData()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };

        await using (var file = new LocalOutputFile(_tempFile))
        {
            await file.WriteAsync(data);
        }

        var result = await File.ReadAllBytesAsync(_tempFile);
        Assert.Equal(data, result);
    }

    [Fact]
    public async Task Position_TracksWrittenBytes()
    {
        await using var file = new LocalOutputFile(_tempFile);

        Assert.Equal(0, file.Position);

        await file.WriteAsync(new byte[10]);
        Assert.Equal(10, file.Position);

        await file.WriteAsync(new byte[20]);
        Assert.Equal(30, file.Position);
    }

    [Fact]
    public async Task MultipleWrites_ProducesCorrectFile()
    {
        await using (var file = new LocalOutputFile(_tempFile))
        {
            await file.WriteAsync(new byte[] { 0xAA, 0xBB });
            await file.WriteAsync(new byte[] { 0xCC, 0xDD, 0xEE });
        }

        var result = await File.ReadAllBytesAsync(_tempFile);
        Assert.Equal(new byte[] { 0xAA, 0xBB, 0xCC, 0xDD, 0xEE }, result);
    }

    [Fact]
    public async Task FlushAsync_DoesNotThrow()
    {
        await using var file = new LocalOutputFile(_tempFile);
        await file.WriteAsync(new byte[] { 1, 2, 3 });
        await file.FlushAsync();
    }

    [Fact]
    public async Task WriteAsync_LargeData()
    {
        var data = new byte[1_000_000];
        Random.Shared.NextBytes(data);

        await using (var file = new LocalOutputFile(_tempFile))
        {
            await file.WriteAsync(data);
        }

        var result = await File.ReadAllBytesAsync(_tempFile);
        Assert.Equal(data, result);
    }

    [Fact]
    public async Task WriteAsync_EmptyData()
    {
        await using (var file = new LocalOutputFile(_tempFile))
        {
            await file.WriteAsync(ReadOnlyMemory<byte>.Empty);
            Assert.Equal(0, file.Position);
        }

        var result = await File.ReadAllBytesAsync(_tempFile);
        Assert.Empty(result);
    }

    [Fact]
    public async Task RoundTrip_WithLocalRandomAccessFile()
    {
        var data = new byte[256];
        for (int i = 0; i < data.Length; i++)
            data[i] = (byte)i;

        await using (var output = new LocalOutputFile(_tempFile))
        {
            await output.WriteAsync(data);
        }

        using var input = new LocalRandomAccessFile(_tempFile);
        var length = await input.GetLengthAsync();
        Assert.Equal(256, length);

        using var buffer = await input.ReadAsync(new FileRange(0, length));
        Assert.Equal(data, buffer.Memory.ToArray());
    }
}
