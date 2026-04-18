// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace EngineeredWood.Parquet.BloomFilter;

/// <summary>
/// Builds a Parquet Split Block Bloom Filter (SBBF) by inserting values.
/// The resulting filter can be serialized to bytes and later probed via
/// <see cref="SplitBlockBloomFilter"/>.
/// Uses AVX2 SIMD when available for parallel block operations.
/// </summary>
internal sealed class SplitBlockBloomFilterBuilder
{
    private const int BytesPerBlock = 32;

    private readonly byte[] _data;
    private readonly int _numBlocks;

    /// <summary>
    /// Creates a bloom filter builder with the given bitset size.
    /// </summary>
    /// <param name="numBytes">Filter size in bytes. Must be a positive multiple of 32.</param>
    public SplitBlockBloomFilterBuilder(int numBytes)
    {
        if (numBytes <= 0 || numBytes % BytesPerBlock != 0)
            throw new ArgumentException(
                $"Filter size must be a positive multiple of {BytesPerBlock}, got {numBytes}.",
                nameof(numBytes));

        _data = new byte[numBytes];
        _numBlocks = numBytes / BytesPerBlock;
    }

    /// <summary>
    /// Computes the optimal filter size in bytes for a given number of distinct values and FPP,
    /// capped at <paramref name="maxBytes"/>.
    /// </summary>
    public static int OptimalNumBytes(int ndv, double fpp, int maxBytes)
    {
        if (ndv <= 0) return BytesPerBlock; // minimum one block

        double ln2 = Math.Log(2);
        double optimalBits = -ndv * Math.Log(fpp) / (ln2 * ln2);
        int optimalBytes = Math.Max(BytesPerBlock, (int)Math.Ceiling(optimalBits / 8.0));

        // Round up to SBBF block boundary.
        optimalBytes = ((optimalBytes + BytesPerBlock - 1) / BytesPerBlock) * BytesPerBlock;

        return Math.Min(optimalBytes, maxBytes);
    }

    /// <summary>
    /// Inserts a plain-encoded value into the bloom filter.
    /// </summary>
    public void Add(ReadOnlySpan<byte> value)
    {
        ulong hash = XxHash64.HashToUInt64(value);
        AddHash(hash);
    }

    /// <summary>
    /// Returns the raw bitset bytes.
    /// </summary>
    public byte[] ToArray() => _data;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddHash(ulong hash)
    {
        uint upper = (uint)(hash >> 32);
        int blockIndex = (int)(((ulong)upper * (ulong)_numBlocks) >> 32);
        uint key = (uint)hash;

        SbbfBlock.BlockInsert(_data, blockIndex * BytesPerBlock, key);
    }
}
