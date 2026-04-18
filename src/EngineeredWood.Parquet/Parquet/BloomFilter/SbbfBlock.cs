// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
#if NET8_0_OR_GREATER
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics.Arm;
#endif

namespace EngineeredWood.Parquet.BloomFilter;

/// <summary>
/// Low-level operations on a single SBBF block (256 bits = 8 × uint32).
/// Uses AVX2 (x86) or NEON (ARM) SIMD when available, with scalar fallback.
/// </summary>
internal static class SbbfBlock
{
    private const int WordsPerBlock = 8;

    private static ReadOnlySpan<uint> Salt =>
    [
        0x47b6137bu, 0x44974d91u, 0x8824ad5bu, 0xa2b7289du,
        0x705495c7u, 0x2df1424bu, 0x9efc4947u, 0x5c6bfb31u,
    ];

#if NET8_0_OR_GREATER
    // AVX2: full 256-bit (8 lanes)
    private static readonly Vector256<uint> SaltVector256 = Vector256.Create(
        0x47b6137bu, 0x44974d91u, 0x8824ad5bu, 0xa2b7289du,
        0x705495c7u, 0x2df1424bu, 0x9efc4947u, 0x5c6bfb31u);
    private static readonly Vector256<uint> Ones256 = Vector256.Create(1u);

    // NEON / SSE: 128-bit (4 lanes, process block as two halves)
    private static readonly Vector128<uint> SaltLo = Vector128.Create(
        0x47b6137bu, 0x44974d91u, 0x8824ad5bu, 0xa2b7289du);
    private static readonly Vector128<uint> SaltHi = Vector128.Create(
        0x705495c7u, 0x2df1424bu, 0x9efc4947u, 0x5c6bfb31u);
    private static readonly Vector128<uint> Ones128 = Vector128.Create(1u);
#endif

    /// <summary>
    /// Tests whether a block might contain the given key (lower 32 bits of hash).
    /// Returns <c>false</c> if the value is definitely absent.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool BlockProbe(byte[] data, int blockOffset, uint key)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported)
            return BlockProbeAvx2(data, blockOffset, key);
        if (AdvSimd.IsSupported)
            return BlockProbeNeon(data, blockOffset, key);
#endif
        return BlockProbeScalar(data, blockOffset, key);
    }

    /// <summary>
    /// Inserts a key's bit pattern into a block (lower 32 bits of hash).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void BlockInsert(byte[] data, int blockOffset, uint key)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported)
        {
            BlockInsertAvx2(data, blockOffset, key);
            return;
        }
        if (AdvSimd.IsSupported)
        {
            BlockInsertNeon(data, blockOffset, key);
            return;
        }
#endif
        BlockInsertScalar(data, blockOffset, key);
    }

#if NET8_0_OR_GREATER
    // ───────────────────── AVX2 (x86-64, 8 lanes) ─────────────────────

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool BlockProbeAvx2(byte[] data, int blockOffset, uint key)
    {
        var keyVec = Vector256.Create(key);
        var products = Avx2.MultiplyLow(keyVec, SaltVector256);
        var positions = Vector256.ShiftRightLogical(products, 27);
        var masks = Avx2.ShiftLeftLogicalVariable(Ones256, positions);

        ref byte blockRef = ref data[blockOffset];
        var block = Vector256.LoadUnsafe(ref Unsafe.As<byte, uint>(ref blockRef));

        var test = Vector256.BitwiseAnd(block, masks);
        var isSet = Vector256.Equals(test, masks);
        return isSet.Equals(Vector256<uint>.AllBitsSet);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BlockInsertAvx2(byte[] data, int blockOffset, uint key)
    {
        var keyVec = Vector256.Create(key);
        var products = Avx2.MultiplyLow(keyVec, SaltVector256);
        var positions = Vector256.ShiftRightLogical(products, 27);
        var masks = Avx2.ShiftLeftLogicalVariable(Ones256, positions);

        ref byte blockRef = ref data[blockOffset];
        ref uint wordRef = ref Unsafe.As<byte, uint>(ref blockRef);
        var block = Vector256.LoadUnsafe(ref wordRef);
        var updated = Vector256.BitwiseOr(block, masks);
        updated.StoreUnsafe(ref wordRef);
    }

    // ───────────────────── NEON (ARM64, 2 × 4 lanes) ─────────────────────
    //
    // NEON has 128-bit vectors, so we process the 256-bit block as two halves.
    // AdvSimd.ShiftLogical(Vector128<uint>, Vector128<int>) does per-lane
    // variable left shift (positive counts = left shift), which maps to
    // the ARM vshlq_u32 instruction.

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool BlockProbeNeon(byte[] data, int blockOffset, uint key)
    {
        ref byte blockRef = ref data[blockOffset];

        // Lower half (words 0-3)
        if (!NeonHalfProbe(ref blockRef, key, SaltLo))
            return false;

        // Upper half (words 4-7)
        return NeonHalfProbe(ref Unsafe.Add(ref blockRef, 16), key, SaltHi);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BlockInsertNeon(byte[] data, int blockOffset, uint key)
    {
        ref byte blockRef = ref data[blockOffset];

        // Lower half (words 0-3)
        NeonHalfInsert(ref blockRef, key, SaltLo);

        // Upper half (words 4-7)
        NeonHalfInsert(ref Unsafe.Add(ref blockRef, 16), key, SaltHi);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool NeonHalfProbe(ref byte blockRef, uint key, Vector128<uint> salt)
    {
        var keyVec = Vector128.Create(key);
        var products = Vector128.Multiply(keyVec, salt);
        var positions = Vector128.ShiftRightLogical(products, 27);

        // Per-element variable left shift: vshlq_u32.
        // AdvSimd.ShiftLogical interprets positive int values as left shift counts.
        var masks = AdvSimd.ShiftLogical(Ones128, positions.AsInt32());

        ref uint wordRef = ref Unsafe.As<byte, uint>(ref blockRef);
        var block = Vector128.LoadUnsafe(ref wordRef);

        var test = Vector128.BitwiseAnd(block, masks);
        var isSet = Vector128.Equals(test, masks);
        return isSet.Equals(Vector128<uint>.AllBitsSet);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void NeonHalfInsert(ref byte blockRef, uint key, Vector128<uint> salt)
    {
        var keyVec = Vector128.Create(key);
        var products = Vector128.Multiply(keyVec, salt);
        var positions = Vector128.ShiftRightLogical(products, 27);
        var masks = AdvSimd.ShiftLogical(Ones128, positions.AsInt32());

        ref uint wordRef = ref Unsafe.As<byte, uint>(ref blockRef);
        var block = Vector128.LoadUnsafe(ref wordRef);
        var updated = Vector128.BitwiseOr(block, masks);
        updated.StoreUnsafe(ref wordRef);
    }
#endif

    // ───────────────────── Scalar fallback ─────────────────────

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool BlockProbeScalar(byte[] data, int blockOffset, uint key)
    {
        var salt = Salt;
        for (int i = 0; i < WordsPerBlock; i++)
        {
            uint mask = 1u << (int)((key * salt[i]) >> 27);
            uint word = BinaryPrimitives.ReadUInt32LittleEndian(
                data.AsSpan(blockOffset + i * 4));
            if ((word & mask) == 0)
                return false;
        }
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BlockInsertScalar(byte[] data, int blockOffset, uint key)
    {
        var salt = Salt;
        for (int i = 0; i < WordsPerBlock; i++)
        {
            uint mask = 1u << (int)((key * salt[i]) >> 27);
            int wordOffset = blockOffset + i * 4;
            uint word = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(wordOffset));
            BinaryPrimitives.WriteUInt32LittleEndian(data.AsSpan(wordOffset), word | mask);
        }
    }
}
