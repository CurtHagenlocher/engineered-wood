// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Tests;

public class RoaringBitmapReaderTests
{
    /// <summary>
    /// Builds a minimal RoaringBitmapArray with the Delta Lake magic number
    /// and an array container containing the specified values (all in container 0, i.e., values 0-65535).
    /// </summary>
    private static byte[] BuildSimpleDv(params ushort[] values)
    {
        // RoaringBitmapArray format:
        // 4 bytes: magic (0x6431544D)
        // Then portable Roaring Bitmap:
        //   4 bytes: cookie = (containerCount-1) << 16 | 12346 (no-run cookie)
        //   For each container: 2 bytes key + 2 bytes (cardinality-1)
        //   If containerCount >= 4: containerCount * 4 bytes offset header
        //   Then container data

        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);

        // Magic
        bw.Write((uint)1681511377);

        int containerCount = 1;
        int cardinality = values.Length;

        // Cookie: no-run, 1 container
        uint cookie = (uint)((containerCount - 1) << 16) | 12346;
        bw.Write(cookie);

        // Descriptive header: key=0, cardinality-1
        bw.Write((ushort)0); // key
        bw.Write((ushort)(cardinality - 1)); // cardinality - 1

        // No offset header (containerCount < 4)

        // Array container: sorted uint16 values
        foreach (ushort v in values.OrderBy(v => v))
            bw.Write(v);

        bw.Flush();
        return ms.ToArray();
    }

    [Fact]
    public void Deserialize_SingleValue()
    {
        byte[] data = BuildSimpleDv(42);
        var result = RoaringBitmapReader.Deserialize(data);

        Assert.Single(result);
        Assert.Contains(42L, result);
    }

    [Fact]
    public void Deserialize_MultipleValues()
    {
        byte[] data = BuildSimpleDv(1, 5, 10, 100);
        var result = RoaringBitmapReader.Deserialize(data);

        Assert.Equal(4, result.Count);
        Assert.Contains(1L, result);
        Assert.Contains(5L, result);
        Assert.Contains(10L, result);
        Assert.Contains(100L, result);
    }

    [Fact]
    public void Deserialize_InvalidMagic_Throws()
    {
        byte[] data = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(data));
    }

    [Fact]
    public void Deserialize_TooShort_Throws()
    {
        byte[] data = [0x01, 0x02];
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(data));
    }

    [Fact]
    public void Deserialize_ConsecutiveValues()
    {
        // Values 0, 1, 2, 3, 4
        byte[] data = BuildSimpleDv(0, 1, 2, 3, 4);
        var result = RoaringBitmapReader.Deserialize(data);

        Assert.Equal(5, result.Count);
        for (long i = 0; i < 5; i++)
            Assert.Contains(i, result);
    }
}
