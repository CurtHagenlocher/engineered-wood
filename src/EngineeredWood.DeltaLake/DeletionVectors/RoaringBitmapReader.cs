// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Reads a Roaring Bitmap from the portable serialization format.
/// Only supports deserialization — enough for reading deletion vectors.
/// See https://github.com/RoaringBitmap/RoaringFormatSpec
/// </summary>
internal static class RoaringBitmapReader
{
    /// <summary>
    /// Magic number for the "RoaringBitmapArray" format used by Delta Lake DVs.
    /// The first 4 bytes of a DV file are this magic number (little-endian).
    /// </summary>
    private const uint RoaringBitmapArrayMagic = 1681511377; // 0x6431544D "MT1d"

    /// <summary>
    /// Deserializes a RoaringBitmapArray (Delta Lake DV format) into a set of deleted row indices.
    /// The format is: 4-byte magic + portable Roaring Bitmap serialization.
    /// </summary>
    public static HashSet<long> Deserialize(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4)
            throw new DeltaFormatException("Deletion vector data too short.");

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(data);
        if (magic != RoaringBitmapArrayMagic)
            throw new DeltaFormatException(
                $"Invalid deletion vector magic: 0x{magic:X8}, expected 0x{RoaringBitmapArrayMagic:X8}.");

        return DeserializePortable(data[4..]);
    }

    /// <summary>
    /// Deserializes a standard portable Roaring Bitmap serialization.
    /// Format: cookie header, then container data.
    /// </summary>
    private static HashSet<long> DeserializePortable(ReadOnlySpan<byte> data)
    {
        var result = new HashSet<long>();
        int pos = 0;

        // Read the cookie/header
        if (data.Length < 4)
            return result;

        uint cookie = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]);
        pos += 4;

        int containerCount;
        bool hasRunContainers;
        byte[]? runBitmap = null;

        if ((cookie & 0xFFFF) == 12346)
        {
            // "No-run" cookie: little-endian 32-bit integer
            // Upper 16 bits + 1 = number of containers
            containerCount = (int)((cookie >> 16) + 1);
            hasRunContainers = false;
        }
        else if ((cookie & 0xFFFF) == 12347)
        {
            // "Run" cookie: containers may include run containers
            containerCount = (int)((cookie >> 16) + 1);
            hasRunContainers = true;

            // Read run bitmap: ceil(containerCount / 8) bytes
            int runBitmapBytes = (containerCount + 7) / 8;
            runBitmap = data[pos..(pos + runBitmapBytes)].ToArray();
            pos += runBitmapBytes;
        }
        else
        {
            // Legacy serial cookie (just container count as uint32)
            containerCount = (int)cookie;
            pos = 4; // Reset — the cookie was just the count
            hasRunContainers = false;
        }

        if (containerCount == 0)
            return result;

        // Read descriptive header: (key, cardinality-1) pairs, each 2x uint16
        var keys = new ushort[containerCount];
        var cardinalities = new int[containerCount];

        for (int i = 0; i < containerCount; i++)
        {
            keys[i] = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
            pos += 2;
            cardinalities[i] = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]) + 1;
            pos += 2;
        }

        // If no-run cookie and container count > 4, skip offset header
        if (!hasRunContainers || (cookie & 0xFFFF) == 12346)
        {
            // The "no-run" format has offset header for containers with count > 4
            // Actually, offsets are only present in the no-run format when containerCount > 4
            // But we'll just skip them since we read sequentially
            if ((cookie & 0xFFFF) == 12346 && containerCount >= 4)
            {
                // Skip offset header: containerCount * 4 bytes
                pos += containerCount * 4;
            }
        }

        // Read container data
        for (int i = 0; i < containerCount; i++)
        {
            long highBits = (long)keys[i] << 16;
            bool isRun = hasRunContainers && runBitmap != null &&
                         (runBitmap[i / 8] & (1 << (i % 8))) != 0;

            if (isRun)
            {
                // Run container: uint16 numRuns, then (start, length-1) pairs
                int numRuns = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                pos += 2;

                for (int r = 0; r < numRuns; r++)
                {
                    ushort start = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                    pos += 2;
                    ushort length = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                    pos += 2;

                    for (int v = start; v <= start + length; v++)
                        result.Add(highBits | (uint)v);
                }
            }
            else if (cardinalities[i] > 4096)
            {
                // Bitmap container: 8192 bytes (1024 uint64 words)
                for (int word = 0; word < 1024; word++)
                {
                    ulong bits = BinaryPrimitives.ReadUInt64LittleEndian(data[pos..]);
                    pos += 8;

                    while (bits != 0)
                    {
                        int bit = System.Numerics.BitOperations.TrailingZeroCount(bits);
                        result.Add(highBits | (uint)(word * 64 + bit));
                        bits &= bits - 1; // Clear lowest set bit
                    }
                }
            }
            else
            {
                // Array container: sorted uint16 values
                for (int j = 0; j < cardinalities[i]; j++)
                {
                    ushort value = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                    pos += 2;
                    result.Add(highBits | value);
                }
            }
        }

        return result;
    }
}
