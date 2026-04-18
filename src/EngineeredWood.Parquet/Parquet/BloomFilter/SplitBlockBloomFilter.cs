// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace EngineeredWood.Parquet.BloomFilter;

/// <summary>
/// Implements the Parquet Split Block Bloom Filter (SBBF) algorithm.
/// Each block is 256 bits (32 bytes, 8 × uint32 words). The filter uses
/// xxHash64 for hashing and 8 salt constants for bit selection within a block.
/// Uses AVX2 SIMD when available for parallel block operations.
/// </summary>
internal sealed class SplitBlockBloomFilter
{
    private const int BytesPerBlock = 32;

    private readonly byte[] _data;
    private readonly int _numBlocks;

    /// <summary>
    /// Creates a bloom filter over the given raw bitset data.
    /// </summary>
    /// <param name="data">The bloom filter bitset. Length must be a multiple of 32.</param>
    public SplitBlockBloomFilter(byte[] data)
    {
        if (data.Length == 0 || data.Length % BytesPerBlock != 0)
            throw new ArgumentException(
                $"Bloom filter data length must be a positive multiple of {BytesPerBlock}, got {data.Length}.",
                nameof(data));

        _data = data;
        _numBlocks = data.Length / BytesPerBlock;
    }

    /// <summary>
    /// Tests whether the given plain-encoded value might be present in the filter.
    /// </summary>
    /// <returns><c>true</c> if the value might be present; <c>false</c> if it is definitely absent.</returns>
    public bool MightContain(ReadOnlySpan<byte> value)
    {
        ulong hash = XxHash64.HashToUInt64(value);
        return MightContainHash(hash);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool MightContainHash(ulong hash)
    {
        uint upper = (uint)(hash >> 32);
        int blockIndex = (int)(((ulong)upper * (ulong)_numBlocks) >> 32);
        uint key = (uint)hash;

        return SbbfBlock.BlockProbe(_data, blockIndex * BytesPerBlock, key);
    }
}
