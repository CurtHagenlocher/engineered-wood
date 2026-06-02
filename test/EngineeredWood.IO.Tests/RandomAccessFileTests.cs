// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.IO;

namespace EngineeredWood.IO.Tests;

/// <summary>
/// Contract tests for <see cref="IRandomAccessFile"/> implementations.
/// Derived classes provide a cloud-specific factory that pre-populates a file with known data.
/// </summary>
public abstract class RandomAccessFileTests
{
    /// <summary>
    /// Creates a random access file backed by known test data already stored in the cloud.
    /// </summary>
    /// <param name="data">The exact byte content to store and read back.</param>
    /// <param name="testId">A unique identifier for this test run (used to generate unique keys).</param>
    /// <returns>A tuple of (file, content, cleanup).</returns>
    protected abstract Task<(IRandomAccessFile File, byte[] Content, Func<Task> Cleanup)>
        CreateFileAsync(byte[] data, string testId);

    private static byte[] CreateTestData(int size)
    {
        var data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte)(i % 256);
        return data;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(1024)]
    [InlineData(100 * 1024)]
    public async Task GetLengthAsync_ReturnsCorrectLength(int size)
    {
        byte[] data = CreateTestData(size);
        (IRandomAccessFile file, _, Func<Task> cleanup) = await CreateFileAsync(data, $"{nameof(GetLengthAsync_ReturnsCorrectLength)}_{size}");
        try
        {
            long length = await file.GetLengthAsync();
            Assert.Equal(size, length);
        }
        finally
        {
            file.Dispose();
            await cleanup();
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(1024)]
    [InlineData(100 * 1024)]
    public async Task ReadAsync_FullFile(int size)
    {
        byte[] data = CreateTestData(size);
        (IRandomAccessFile file, byte[] content, Func<Task> cleanup) = await CreateFileAsync(data, $"{nameof(ReadAsync_FullFile)}_{size}");
        try
        {
            using IMemoryOwner<byte> result = await file.ReadAsync(new FileRange(0, size));
            Assert.Equal(content, result.Memory.ToArray());
        }
        finally
        {
            file.Dispose();
            await cleanup();
        }
    }

    [Fact]
    public async Task ReadAsync_PartialRange()
    {
        byte[] data = CreateTestData(4096);
        (IRandomAccessFile file, byte[] content, Func<Task> cleanup) = await CreateFileAsync(data, nameof(ReadAsync_PartialRange));
        try
        {
            using IMemoryOwner<byte> result = await file.ReadAsync(new FileRange(1024, 2048));
            byte[] expected = content.Skip(1024).Take(2048).ToArray();
            Assert.Equal(expected, result.Memory.ToArray());
        }
        finally
        {
            file.Dispose();
            await cleanup();
        }
    }

    [Fact]
    public async Task ReadAsync_EmptyRange()
    {
        byte[] data = CreateTestData(100);
        (IRandomAccessFile file, _, Func<Task> cleanup) = await CreateFileAsync(data, nameof(ReadAsync_EmptyRange));
        try
        {
            using IMemoryOwner<byte> result = await file.ReadAsync(new FileRange(50, 0));
            Assert.Equal(0, result.Memory.Length);
        }
        finally
        {
            file.Dispose();
            await cleanup();
        }
    }

    [Fact]
    public async Task ReadAsync_PastEnd_Throws()
    {
        byte[] data = CreateTestData(100);
        (IRandomAccessFile file, _, Func<Task> cleanup) = await CreateFileAsync(data, nameof(ReadAsync_PastEnd_Throws));
        try
        {
            await Assert.ThrowsAsync<IOException>(
                () => file.ReadAsync(new FileRange(50, 100)).AsTask());
        }
        finally
        {
            file.Dispose();
            await cleanup();
        }
    }

    [Fact]
    public async Task ReadRangesAsync_MultipleDisjointRanges()
    {
        byte[] data = CreateTestData(4096);
        (IRandomAccessFile file, byte[] content, Func<Task> cleanup) = await CreateFileAsync(data, nameof(ReadRangesAsync_MultipleDisjointRanges));
        try
        {
            FileRange[] ranges = [new(0, 512), new(2048, 1024), new(3500, 596)];
            IReadOnlyList<IMemoryOwner<byte>> results = await file.ReadRangesAsync(ranges);

            Assert.Equal(3, results.Count);
            for (int i = 0; i < ranges.Length; i++)
            {
                byte[] expected = content.Skip((int)ranges[i].Offset).Take((int)ranges[i].Length).ToArray();
                Assert.Equal(expected, results[i].Memory.ToArray());
                results[i].Dispose();
            }
        }
        finally
        {
            file.Dispose();
            await cleanup();
        }
    }

    [Fact]
    public async Task ReadRangesAsync_SingleRange()
    {
        byte[] data = CreateTestData(2048);
        (IRandomAccessFile file, byte[] content, Func<Task> cleanup) = await CreateFileAsync(data, nameof(ReadRangesAsync_SingleRange));
        try
        {
            FileRange[] ranges = [new(256, 1024)];
            IReadOnlyList<IMemoryOwner<byte>> results = await file.ReadRangesAsync(ranges);

            Assert.Single(results);
            byte[] expected = content.Skip(256).Take(1024).ToArray();
            Assert.Equal(expected, results[0].Memory.ToArray());
            results[0].Dispose();
        }
        finally
        {
            file.Dispose();
            await cleanup();
        }
    }
}
