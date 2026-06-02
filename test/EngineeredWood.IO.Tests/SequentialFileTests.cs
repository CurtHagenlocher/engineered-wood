// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.IO.Tests;

/// <summary>
/// Contract tests for <see cref="ISequentialFile"/> implementations.
/// Derived classes provide a cloud-specific factory and read-back mechanism.
/// </summary>
public abstract class SequentialFileTests
{
    /// <summary>
    /// Creates a sequential file for testing.
    /// </summary>
    /// <param name="testId">A unique identifier for this test run (used to generate unique keys).</param>
    /// <returns>A tuple of (file, readBack, cleanup).</returns>
    protected abstract Task<(ISequentialFile File, Func<Task<byte[]>> ReadBack, Func<Task> Cleanup)> CreateFileAsync(
        string testId);

    private static byte[] CreateTestData(int size)
    {
        var data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte)(i % 256);
        return data;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(1024)]
    [InlineData(100 * 1024)]
    public async Task WriteAndFlush_RoundTrips(int dataSize)
    {
        (ISequentialFile file, Func<Task<byte[]>> readBack, Func<Task> cleanup) = await CreateFileAsync($"{nameof(WriteAndFlush_RoundTrips)}_{dataSize}");
        try
        {
            byte[] data = CreateTestData(dataSize);

            await file.WriteAsync(data);
            Assert.Equal(data.Length, file.Position);

            await file.FlushAsync();

            byte[] actual = await readBack();
            Assert.Equal(data, actual);
        }
        finally
        {
            await file.DisposeAsync();
            await cleanup();
        }
    }

    [Theory]
    [InlineData(100, 7)]
    [InlineData(1024, 3)]
    public async Task WriteMultipleTimes_Flush_RoundTrips(int chunkSize, int chunkCount)
    {
        (ISequentialFile file, Func<Task<byte[]>> readBack, Func<Task> cleanup) = await CreateFileAsync($"{nameof(WriteMultipleTimes_Flush_RoundTrips)}_{chunkSize}x{chunkCount}");
        try
        {
            byte[] expected = new byte[chunkSize * chunkCount];
            for (int i = 0; i < chunkCount; i++)
            {
                var chunk = CreateTestData(chunkSize);
                Array.Copy(chunk, 0, expected, i * chunkSize, chunkSize);
                await file.WriteAsync(chunk);
            }

            Assert.Equal(expected.Length, file.Position);
            await file.FlushAsync();

            byte[] actual = await readBack();
            Assert.Equal(expected, actual);
        }
        finally
        {
            await file.DisposeAsync();
            await cleanup();
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(1024)]
    [InlineData(10 * 1024)]
    public async Task DisposeWithoutExplicitFlush_SavesData(int dataSize)
    {
        byte[] data = CreateTestData(dataSize);
        byte[] actual;

        (ISequentialFile file, Func<Task<byte[]>> readBack, Func<Task> cleanup) = await CreateFileAsync($"{nameof(DisposeWithoutExplicitFlush_SavesData)}_{dataSize}");
        try
        {
            await file.WriteAsync(data);
            // no explicit Flush — DisposeAsync should flush
        }
        finally
        {
            await file.DisposeAsync();
        }

        actual = await readBack();
        Assert.Equal(data, actual);
        await cleanup();
    }

    [Fact]
    public async Task Position_TracksBytesWritten()
    {
        (ISequentialFile file, _, Func<Task> cleanup) = await CreateFileAsync(nameof(Position_TracksBytesWritten));
        try
        {
            Assert.Equal(0, file.Position);

            var data = new byte[100];
            await file.WriteAsync(data);
            Assert.Equal(100, file.Position);

            await file.WriteAsync(data);
            Assert.Equal(200, file.Position);

            await file.WriteAsync(Array.Empty<byte>());
            Assert.Equal(200, file.Position);
        }
        finally
        {
            await file.DisposeAsync();
            await cleanup();
        }
    }

    [Fact]
    public async Task WriteLargeData_MultiBlock_RoundTrips()
    {
        // Write enough data to cross the default block/part boundary.
        // Azure default is 4 MiB, S3 default is 5 MiB — 6 MiB covers both.
        const int size = 6 * 1024 * 1024;
        byte[] data = CreateTestData(size);

        (ISequentialFile file, Func<Task<byte[]>> readBack, Func<Task> cleanup) =
            await CreateFileAsync(nameof(WriteLargeData_MultiBlock_RoundTrips));
        try
        {
            await file.WriteAsync(data);
            Assert.Equal(size, file.Position);
            await file.FlushAsync();

            byte[] actual = await readBack();
            Assert.Equal(data, actual);
        }
        finally
        {
            await file.DisposeAsync();
            await cleanup();
        }
    }

    [Fact]
    public async Task FlushWithNoData_Succeeds()
    {
        (ISequentialFile file, _, Func<Task> cleanup) = await CreateFileAsync(nameof(FlushWithNoData_Succeeds));
        try
        {
            await file.FlushAsync();
            Assert.Equal(0, file.Position);
        }
        finally
        {
            await file.DisposeAsync();
            await cleanup();
        }
    }

    [Fact]
    public async Task WriteAfterDispose_ThrowsObjectDisposedException()
    {
        (ISequentialFile file, _, Func<Task> cleanup) = await CreateFileAsync(nameof(WriteAfterDispose_ThrowsObjectDisposedException));
        await file.WriteAsync(new byte[] { 1, 2, 3 });
        await file.FlushAsync();
        await file.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => file.WriteAsync(new byte[] { 4 }).AsTask());
        await cleanup();
    }

    [Fact]
    public async Task FlushAfterDispose_ThrowsObjectDisposedException()
    {
        (ISequentialFile file, _, Func<Task> cleanup) = await CreateFileAsync(nameof(FlushAfterDispose_ThrowsObjectDisposedException));
        await file.WriteAsync(new byte[] { 1 });
        await file.FlushAsync();
        await file.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => file.FlushAsync().AsTask());
        await cleanup();
    }
}
