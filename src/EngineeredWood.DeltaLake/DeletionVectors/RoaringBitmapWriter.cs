// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Serializes a set of row indices into the RoaringBitmapArray format
/// used by Delta Lake deletion vectors.
/// </summary>
internal static class RoaringBitmapWriter
{
    /// <summary>
    /// Magic number for the RoaringBitmapArray format.
    /// </summary>
    private const uint RoaringBitmapArrayMagic = 1681511377; // 0x6439D3D1

    /// <summary>
    /// Serializes a set of row indices into the Delta Lake DV binary format.
    /// Format: 4-byte magic + portable Roaring Bitmap serialization.
    /// </summary>
    public static byte[] Serialize(IEnumerable<long> rowIndices)
    {
        // Group indices by high 16-bit container key
        var containers = new SortedDictionary<ushort, List<ushort>>();

        foreach (long idx in rowIndices)
        {
            // Delta DVs use 64-bit indices, but for files < 2^32 rows
            // we only need the lower 32 bits, split into 16-bit key + 16-bit value
            ushort key = (ushort)(idx >> 16);
            ushort value = (ushort)(idx & 0xFFFF);

            if (!containers.TryGetValue(key, out var values))
            {
                values = new List<ushort>();
                containers[key] = values;
            }
            values.Add(value);
        }

        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);

        // Write magic
        bw.Write(RoaringBitmapArrayMagic);

        // Write portable Roaring Bitmap
        WritePortableRoaringBitmap(bw, containers);

        bw.Flush();
        return ms.ToArray();
    }

    private static void WritePortableRoaringBitmap(
        BinaryWriter bw, SortedDictionary<ushort, List<ushort>> containers)
    {
        int containerCount = containers.Count;
        if (containerCount == 0)
        {
            // Empty bitmap: write no-run cookie with 0 containers
            // Actually the cookie encodes (containerCount - 1), so for 0 containers
            // we write a legacy format with just count = 0
            bw.Write((uint)0);
            return;
        }

        // No-run cookie: (containerCount - 1) << 16 | 12346
        uint cookie = (uint)((containerCount - 1) << 16) | 12346;
        bw.Write(cookie);

        // Prepare container data
        var keys = new ushort[containerCount];
        var cardinalities = new int[containerCount];
        var sortedValues = new List<ushort>[containerCount];

        int i = 0;
        foreach (var kvp in containers)
        {
            keys[i] = kvp.Key;
            var vals = kvp.Value;
            vals.Sort();
            // Deduplicate
            var deduped = new List<ushort>(vals.Count);
            ushort prev = ushort.MaxValue;
            foreach (ushort v in vals)
            {
                if (v != prev) deduped.Add(v);
                prev = v;
            }
            sortedValues[i] = deduped;
            cardinalities[i] = deduped.Count;
            i++;
        }

        // Write descriptive header: (key, cardinality-1) pairs
        for (i = 0; i < containerCount; i++)
        {
            bw.Write(keys[i]);
            bw.Write((ushort)(cardinalities[i] - 1));
        }

        // Offset header (only when containerCount >= 4 for no-run cookie)
        if (containerCount >= 4)
        {
            // Calculate offsets
            int headerSize = 4 + containerCount * 4 + containerCount * 4; // cookie + descriptive + offsets
            int offset = headerSize;
            for (i = 0; i < containerCount; i++)
            {
                bw.Write(offset);
                if (cardinalities[i] > 4096)
                    offset += 8192; // Bitmap container: 1024 * 8 bytes
                else
                    offset += cardinalities[i] * 2; // Array container: cardinality * 2 bytes
            }
        }

        // Write container data
        for (i = 0; i < containerCount; i++)
        {
            if (cardinalities[i] > 4096)
            {
                // Bitmap container: 1024 uint64 words
                var bitmap = new ulong[1024];
                foreach (ushort v in sortedValues[i])
                    bitmap[v / 64] |= 1UL << (v % 64);

                foreach (ulong word in bitmap)
                    bw.Write(word);
            }
            else
            {
                // Array container: sorted uint16 values
                foreach (ushort v in sortedValues[i])
                    bw.Write(v);
            }
        }
    }
}
