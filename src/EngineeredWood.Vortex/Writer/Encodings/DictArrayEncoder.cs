// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.DictArrayDecoder"/>:
/// emits a <c>vortex.dict</c> ArrayNode subtree for repetitive
/// <see cref="StringArray"/> columns. Builds a deduplicated dictionary of
/// distinct values and a per-row codes array; reader does
/// <c>output[i] = values[codes[i]]</c>.
///
/// <para>Wire shape: 0 buffers, 2 children (codes, values), metadata
/// <c>DictMetadata { codes_ptype, values_len }</c>. Same vtable as vortex.list
/// (slots 0+1+2, with optional slot 4 for stats). Codes child is encoded as
/// vortex.primitive (smallest fitting unsigned width); values child is
/// vortex.varbin.</para>
///
/// <para>Scope: non-nullable Arrow <see cref="StringArray"/>. Other types and
/// nullable inputs are deferred — <see cref="EngineeredWood.Vortex.Layouts.DictReconstructor"/>
/// doesn't propagate validity from codes back to the output, so a nullable
/// dict file would silently drop nulls on read.</para>
/// </summary>
internal static class DictArrayEncoder
{
    /// <summary>
    /// Returns true iff the column is a non-null, non-sliced StringArray AND
    /// the distinct count is small enough that the codes + dict-values payload
    /// is meaningfully smaller than the raw varbin encoding. Heuristic: at
    /// least 4× repetition (<c>K × 4 ≤ n</c>) AND at least 8 rows.
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is not StringArray s) return false;
        var data = s.Data;
        if (data.Offset != 0) return false; // dict over sliced — defer
        if (data.GetNullCount() > 0) return false;
        int n = s.Length;
        if (n < 8) return false;

        // Probe distinct count, capped early: if we exceed n / 4 we'd reject
        // anyway, so abort the scan.
        var seen = new HashSet<string>(StringComparer.Ordinal);
        int cap = n / 4;
        for (int i = 0; i < n; i++)
        {
            seen.Add(s.GetString(i));
            if (seen.Count > cap) return false;
        }
        return true;
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is not StringArray s)
            throw new NotSupportedException(
                $"vortex.dict writer requires StringArray, got {array.GetType().Name}.");
        var data = s.Data;
        if (data.Offset != 0)
            throw new NotSupportedException("vortex.dict writer doesn't yet support sliced inputs.");
        if (data.GetNullCount() > 0)
            throw new NotSupportedException("vortex.dict writer doesn't yet support nullable inputs.");

        int n = s.Length;

        // 1. Build dictionary in input order — first occurrence assigns the index.
        var lookup = new Dictionary<string, int>(StringComparer.Ordinal);
        var distinct = new List<string>();
        var codes = new int[n];
        for (int i = 0; i < n; i++)
        {
            var v = s.GetString(i);
            if (!lookup.TryGetValue(v, out int idx_))
            {
                idx_ = distinct.Count;
                lookup.Add(v, idx_);
                distinct.Add(v);
            }
            codes[i] = idx_;
        }
        int k = distinct.Count;

        // 2. Pick the smallest unsigned codes width. PType enum: U8=0, U16=1, U32=2, U64=3.
        IArrowArray codesArray;
        byte codesPtype;
        if (k <= byte.MaxValue + 1) // K up to 256 fits in u8.
        {
            codesArray = BuildCodesU8(codes);
            codesPtype = 0;
        }
        else if (k <= ushort.MaxValue + 1) // up to 65536 fits in u16.
        {
            codesArray = BuildCodesU16(codes);
            codesPtype = 1;
        }
        else
        {
            codesArray = BuildCodesU32(codes);
            codesPtype = 2;
        }

        // 3. Build the values StringArray (dictionary itself).
        var valuesArray = BuildStringArray(distinct);

        // 4. Encode children recursively. Codes go through dispatch with
        //    compress=false so they don't accidentally pick up another encoding
        //    layer; same for values.
        int codesNodeTicket = ArrayEncoderDispatch.Emit(sb, codesArray, idx);
        int valuesNodeTicket = ArrayEncoderDispatch.Emit(sb, valuesArray, idx);

        // 5. Metadata.
        var metadataBytes = SerializeDictMetadata(codesPtype, (uint)k);
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var childTickets = new[] { codesNodeTicket, valuesNodeTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, idx.Dict, metadataTicket, childTickets)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, idx.Dict, metadataTicket, childTickets, statsTicket.Value);
    }

    private static UInt8Array BuildCodesU8(int[] codes)
    {
        var bytes = new byte[codes.Length];
        for (int i = 0; i < codes.Length; i++) bytes[i] = checked((byte)codes[i]);
        return new UInt8Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, codes.Length, 0, 0);
    }

    private static UInt16Array BuildCodesU16(int[] codes)
    {
        var bytes = new byte[codes.Length * 2];
        for (int i = 0; i < codes.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(i * 2, 2), checked((ushort)codes[i]));
        return new UInt16Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, codes.Length, 0, 0);
    }

    private static UInt32Array BuildCodesU32(int[] codes)
    {
        var bytes = new byte[codes.Length * 4];
        for (int i = 0; i < codes.Length; i++)
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(i * 4, 4), checked((uint)codes[i]));
        return new UInt32Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, codes.Length, 0, 0);
    }

    private static StringArray BuildStringArray(List<string> values)
    {
        // offsets[k+1] + concatenated UTF-8 bytes; no validity (non-nullable).
        int total = 0;
        var encodedSegments = new byte[values.Count][];
        for (int i = 0; i < values.Count; i++)
        {
            encodedSegments[i] = System.Text.Encoding.UTF8.GetBytes(values[i]);
            total += encodedSegments[i].Length;
        }
        var offsetBytes = new byte[(values.Count + 1) * 4];
        var dataBytes = new byte[total];
        int pos = 0;
        for (int i = 0; i < values.Count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4, 4), pos);
            encodedSegments[i].CopyTo(dataBytes, pos);
            pos += encodedSegments[i].Length;
        }
        BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(values.Count * 4, 4), pos);

        return new StringArray(
            values.Count,
            new ArrowBuffer(offsetBytes),
            new ArrowBuffer(dataBytes),
            ArrowBuffer.Empty,
            nullCount: 0,
            offset: 0);
    }

    /// <summary>
    /// Inline DictMetadata proto bytes (per vortex-array's <c>arrays/dict/array.rs</c>):
    ///   field 1 (varint, u32): values_len
    ///   field 2 (varint, PType enum): codes_ptype
    /// Fields 3 (is_nullable_codes) and 4 (all_values_referenced) are absent —
    /// proto3 defaults (false) suffice for our non-nullable, possibly-some-unreferenced
    /// values case.
    /// </summary>
    private static byte[] SerializeDictMetadata(byte codesPtype, uint valuesLen)
    {
        // Worst case: 1 + 5 + 1 + 1 = 8 bytes.
        Span<byte> tmp = stackalloc byte[8];
        int pos = 0;
        tmp[pos++] = 0x08; // tag: field 1, wire-type 0 — values_len
        pos += Varint.WriteUnsigned(tmp.Slice(pos), valuesLen);
        tmp[pos++] = 0x10; // tag: field 2, wire-type 0 — codes_ptype
        tmp[pos++] = codesPtype;
        return tmp.Slice(0, pos).ToArray();
    }
}
