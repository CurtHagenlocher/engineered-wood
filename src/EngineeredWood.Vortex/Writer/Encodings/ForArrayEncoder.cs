// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.ForArrayDecoder"/>:
/// emits a <c>fastlanes.for</c> (Frame of Reference) ArrayNode subtree.
/// Subtracts a reference scalar (the column's min) from every value; the
/// (always non-negative) residuals are then encoded as a child via
/// <see cref="BitPackedArrayEncoder"/>.
///
/// <para>Wire shape: 0 buffers, 1 child (the residuals — same Arrow dtype as
/// the parent — typically <c>fastlanes.bitpacked</c>), metadata = vortex
/// <c>ScalarValue</c> protobuf of the reference. Reader: <c>output[i] = ref + residuals[i]</c>.</para>
///
/// <para>Scope: non-nullable, non-sliced integer columns (Int8..Int64,
/// UInt8..UInt64). FoR is profitable when EITHER (a) the column has negative
/// values that <see cref="BitPackedArrayEncoder"/> alone can't handle, OR (b)
/// <c>min != 0</c> AND <c>MaxBits(values - min) &lt; MaxBits(values)</c>, i.e.
/// shifting tightens the bit width. When neither holds, plain bitpacked wins
/// and dispatch falls through to it.</para>
/// </summary>
internal static class ForArrayEncoder
{
    /// <summary>
    /// True iff FoR can encode <paramref name="array"/> AND would compress
    /// strictly better than plain bitpacked (or where plain bitpacked doesn't
    /// even apply because of negative values).
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is null) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        if (data.Offset != 0) return false;
        if (data.GetNullCount() > 0) return false;
        if (array.Length == 0) return false;
        int? nativeBits = NativeBits(array);
        if (nativeBits is not int native) return false;

        int residualBits = ComputeResidualBits(array);
        if (residualBits >= native) return false; // FoR child wouldn't compress.

        // FoR is profitable when either:
        //   (a) bitpacked alone can't apply (signed column with any negative).
        //   (b) min != 0 — residuals strictly fewer bits than direct values.
        if (IsSigned(array) && HasNegative(array)) return true;
        return MinIsNonZero(array);
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array,
        ushort forEncodingIdx, ushort bitpackedEncodingIdx, ushort boolEncodingIdx,
        int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        var data = ((Apache.Arrow.Array)array).Data;
        if (data.Offset != 0)
            throw new NotSupportedException("fastlanes.for writer doesn't yet support sliced inputs.");
        if (data.GetNullCount() > 0)
            throw new NotSupportedException("fastlanes.for writer doesn't yet support nullable inputs.");

        // 1. Build residuals array (same Arrow dtype as parent) + serialize the
        //    reference scalar's protobuf bytes for the metadata field.
        var (residualArray, metadataBytes) = BuildResidualsAndMetadata(array);

        // 2. Emit residuals as a child via bitpacked. Child gets no stats —
        //    statsTicket only attaches at the top of the column.
        int residualNodeTicket = BitPackedArrayEncoder.Emit(
            sb, residualArray, bitpackedEncodingIdx, boolEncodingIdx);

        // 3. Emit FoR metadata as a byte vector (ScalarValue protobuf bytes).
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        // 4. Emit FoR ArrayNode: same vtable shape as vortex.list (0 buffers,
        //    metadata + children).
        var children = new[] { residualNodeTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, forEncodingIdx, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, forEncodingIdx, metadataTicket, children, statsTicket.Value);
    }

    /// <summary>Convenience: encode one column's segment in isolation.</summary>
    public static byte[] Encode(
        IArrowArray array, ushort forEncodingIdx, ushort bitpackedEncodingIdx, ushort boolEncodingIdx)
    {
        var sb = new SegmentBuilder();
        var rootTicket = Emit(sb, array, forEncodingIdx, bitpackedEncodingIdx, boolEncodingIdx);
        return sb.FinishSegment(rootTicket);
    }

    private static int? NativeBits(IArrowArray array) => array switch
    {
        UInt8Array or Int8Array => 8,
        UInt16Array or Int16Array => 16,
        UInt32Array or Int32Array => 32,
        UInt64Array or Int64Array => 64,
        _ => null,
    };

    private static bool IsSigned(IArrowArray array) => array switch
    {
        Int8Array or Int16Array or Int32Array or Int64Array => true,
        _ => false,
    };

    private static bool HasNegative(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        return array switch
        {
            Int8Array => HasAnyNegative(MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(0, n))),
            Int16Array => HasAnyNegative(MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(0, n * 2))),
            Int32Array => HasAnyNegative(MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(0, n * 4))),
            Int64Array => HasAnyNegative(MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(0, n * 8))),
            _ => false,
        };
    }

    private static bool HasAnyNegative<T>(ReadOnlySpan<T> span) where T : unmanaged, IComparable<T>
    {
        T zero = default;
        for (int i = 0; i < span.Length; i++)
            if (span[i].CompareTo(zero) < 0) return true;
        return false;
    }

    /// <summary>Returns true if the column's minimum value is non-zero (non-empty assumed).</summary>
    private static bool MinIsNonZero(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        return array switch
        {
            Int8Array => ComputeMinSigned(MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(0, n))) != 0,
            Int16Array => ComputeMinSigned(MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(0, n * 2))) != 0,
            Int32Array => ComputeMinSigned(MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(0, n * 4))) != 0,
            Int64Array => ComputeMinSigned(MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(0, n * 8))) != 0,
            UInt8Array => ComputeMinUnsigned(data.Buffers[1].Span.Slice(0, n)) != 0,
            UInt16Array => ComputeMinUnsigned(MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(0, n * 2))) != 0,
            UInt32Array => ComputeMinUnsigned(MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(0, n * 4))) != 0,
            UInt64Array => ComputeMinUnsigned(MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(0, n * 8))) != 0,
            _ => false,
        };
    }

    private static long ComputeMinSigned<T>(ReadOnlySpan<T> span) where T : unmanaged, IComparable<T>
    {
        // Generic min via IComparable (works for sbyte/short/int/long).
        T min = span[0];
        for (int i = 1; i < span.Length; i++)
            if (span[i].CompareTo(min) < 0) min = span[i];
        // Box once and convert to long.
        return Convert.ToInt64(min);
    }

    private static ulong ComputeMinUnsigned<T>(ReadOnlySpan<T> span) where T : unmanaged, IComparable<T>
    {
        T min = span[0];
        for (int i = 1; i < span.Length; i++)
            if (span[i].CompareTo(min) < 0) min = span[i];
        return Convert.ToUInt64(min);
    }

    /// <summary>
    /// Computes <c>MaxBits(values - min)</c> — the bit width required by the
    /// FoR-shifted residuals. Residuals are always non-negative (≥ 0 ≤ T_max -
    /// T_min) so we promote to <c>ulong</c> uniformly for the bit-count.
    /// </summary>
    private static int ComputeResidualBits(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        ulong maxResidual;
        switch (array)
        {
            case Int8Array:
                {
                    var s = MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(0, n));
                    sbyte min = s[0]; for (int i = 1; i < n; i++) if (s[i] < min) min = s[i];
                    maxResidual = 0;
                    for (int i = 0; i < n; i++) { ulong r = (ulong)(s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case Int16Array:
                {
                    var s = MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(0, n * 2));
                    short min = s[0]; for (int i = 1; i < n; i++) if (s[i] < min) min = s[i];
                    maxResidual = 0;
                    for (int i = 0; i < n; i++) { ulong r = (ulong)(s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case Int32Array:
                {
                    var s = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(0, n * 4));
                    int min = s[0]; for (int i = 1; i < n; i++) if (s[i] < min) min = s[i];
                    maxResidual = 0;
                    for (int i = 0; i < n; i++) { ulong r = (ulong)((long)s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case Int64Array:
                {
                    var s = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(0, n * 8));
                    long min = s[0]; for (int i = 1; i < n; i++) if (s[i] < min) min = s[i];
                    maxResidual = 0;
                    for (int i = 0; i < n; i++) { ulong r = unchecked((ulong)(s[i] - min)); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case UInt8Array:
                {
                    var s = data.Buffers[1].Span.Slice(0, n);
                    byte min = s[0]; for (int i = 1; i < n; i++) if (s[i] < min) min = s[i];
                    maxResidual = 0;
                    for (int i = 0; i < n; i++) { ulong r = (ulong)(s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case UInt16Array:
                {
                    var s = MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(0, n * 2));
                    ushort min = s[0]; for (int i = 1; i < n; i++) if (s[i] < min) min = s[i];
                    maxResidual = 0;
                    for (int i = 0; i < n; i++) { ulong r = (ulong)(s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case UInt32Array:
                {
                    var s = MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(0, n * 4));
                    uint min = s[0]; for (int i = 1; i < n; i++) if (s[i] < min) min = s[i];
                    maxResidual = 0;
                    for (int i = 0; i < n; i++) { ulong r = s[i] - min; if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case UInt64Array:
                {
                    var s = MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(0, n * 8));
                    ulong min = s[0]; for (int i = 1; i < n; i++) if (s[i] < min) min = s[i];
                    maxResidual = 0;
                    for (int i = 0; i < n; i++) { ulong r = s[i] - min; if (r > maxResidual) maxResidual = r; }
                    break;
                }
            default: throw new NotSupportedException();
        }
        return maxResidual == 0 ? 0 : 64 - System.Numerics.BitOperations.LeadingZeroCount(maxResidual);
    }

    /// <summary>
    /// Constructs the residuals Arrow array (same dtype as parent) and the
    /// reference-scalar's ScalarValue protobuf bytes.
    /// </summary>
    private static (IArrowArray Residuals, byte[] MetadataBytes) BuildResidualsAndMetadata(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        switch (array)
        {
            case Int8Array:
                {
                    var src = MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(0, n));
                    sbyte min = src[0]; for (int i = 1; i < n; i++) if (src[i] < min) min = src[i];
                    var bytes = new byte[n];
                    var dst = MemoryMarshal.Cast<byte, sbyte>(bytes.AsSpan());
                    for (int i = 0; i < n; i++) dst[i] = (sbyte)(src[i] - min);
                    return (new Int8Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0),
                            ScalarValueSerializer.FromSignedInt(min));
                }
            case Int16Array:
                {
                    var src = MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(0, n * 2));
                    short min = src[0]; for (int i = 1; i < n; i++) if (src[i] < min) min = src[i];
                    var bytes = new byte[n * 2];
                    var dst = MemoryMarshal.Cast<byte, short>(bytes.AsSpan());
                    for (int i = 0; i < n; i++) dst[i] = (short)(src[i] - min);
                    return (new Int16Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0),
                            ScalarValueSerializer.FromSignedInt(min));
                }
            case Int32Array:
                {
                    var src = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(0, n * 4));
                    int min = src[0]; for (int i = 1; i < n; i++) if (src[i] < min) min = src[i];
                    var bytes = new byte[n * 4];
                    var dst = MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
                    for (int i = 0; i < n; i++) dst[i] = src[i] - min;
                    return (new Int32Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0),
                            ScalarValueSerializer.FromSignedInt(min));
                }
            case Int64Array:
                {
                    var src = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(0, n * 8));
                    long min = src[0]; for (int i = 1; i < n; i++) if (src[i] < min) min = src[i];
                    var bytes = new byte[n * 8];
                    var dst = MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
                    for (int i = 0; i < n; i++) dst[i] = src[i] - min;
                    return (new Int64Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0),
                            ScalarValueSerializer.FromSignedInt(min));
                }
            case UInt8Array:
                {
                    var src = data.Buffers[1].Span.Slice(0, n);
                    byte min = src[0]; for (int i = 1; i < n; i++) if (src[i] < min) min = src[i];
                    var bytes = new byte[n];
                    for (int i = 0; i < n; i++) bytes[i] = (byte)(src[i] - min);
                    return (new UInt8Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0),
                            ScalarValueSerializer.FromUnsignedInt(min));
                }
            case UInt16Array:
                {
                    var src = MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(0, n * 2));
                    ushort min = src[0]; for (int i = 1; i < n; i++) if (src[i] < min) min = src[i];
                    var bytes = new byte[n * 2];
                    var dst = MemoryMarshal.Cast<byte, ushort>(bytes.AsSpan());
                    for (int i = 0; i < n; i++) dst[i] = (ushort)(src[i] - min);
                    return (new UInt16Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0),
                            ScalarValueSerializer.FromUnsignedInt(min));
                }
            case UInt32Array:
                {
                    var src = MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(0, n * 4));
                    uint min = src[0]; for (int i = 1; i < n; i++) if (src[i] < min) min = src[i];
                    var bytes = new byte[n * 4];
                    var dst = MemoryMarshal.Cast<byte, uint>(bytes.AsSpan());
                    for (int i = 0; i < n; i++) dst[i] = src[i] - min;
                    return (new UInt32Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0),
                            ScalarValueSerializer.FromUnsignedInt(min));
                }
            case UInt64Array:
                {
                    var src = MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(0, n * 8));
                    ulong min = src[0]; for (int i = 1; i < n; i++) if (src[i] < min) min = src[i];
                    var bytes = new byte[n * 8];
                    var dst = MemoryMarshal.Cast<byte, ulong>(bytes.AsSpan());
                    for (int i = 0; i < n; i++) dst[i] = src[i] - min;
                    return (new UInt64Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0),
                            ScalarValueSerializer.FromUnsignedInt(min));
                }
            default:
                throw new NotSupportedException(
                    $"fastlanes.for doesn't support Arrow {array.GetType().Name}.");
        }
    }
}
