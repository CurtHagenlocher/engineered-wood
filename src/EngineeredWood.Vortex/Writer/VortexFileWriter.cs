// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Layouts;
using EngineeredWood.Vortex.Writer.Encodings;

namespace EngineeredWood.Vortex.Writer;

/// <summary>
/// Vortex writer. Produces a single Vortex file from one or more Arrow
/// <see cref="RecordBatch"/>es. Single-batch files use a
/// <c>vortex.struct(vortex.flat × N)</c> layout; multi-batch files use
/// <c>vortex.struct(vortex.chunked(vortex.flat × M) × N)</c>.
///
/// <para>Usage:
/// <code>
/// using var w = new VortexFileWriter(stream, schema);
/// w.WriteBatch(batch1);
/// w.WriteBatch(batch2);
/// w.Close(); // (Dispose also closes if not already)
/// </code></para>
///
/// <para>File layout:
/// <c>[VTXF magic][batch_0 segments][batch_1 segments]...[DType FB][Layout FB][Footer FB][Postscript FB][EndOfFile struct]</c>
/// where EndOfFile = <c>version:u16 | postscript_len:u16 | "VTXF"</c>.</para>
///
/// <para>Phase 2 scope: primitive (Int8..Int64, UInt8..UInt64, Float32/64, Bool)
/// and Utf8/Binary columns, nullable + non-nullable, sliced inputs. Multi-batch
/// streaming. Compressing encodings, lists, decimals, dicts, and zone-pruning
/// stats are deferred to later phases.</para>
/// </summary>
public sealed class VortexFileWriter : IDisposable
{
    // Array-spec registry constants.
    private const ushort PrimitiveEncodingIdx = 0;
    private const ushort BoolEncodingIdx = 1;
    private const ushort VarBinEncodingIdx = 2;
    private const ushort ListEncodingIdx = 3;
    private const ushort FixedSizeListEncodingIdx = 4;
    private const ushort BitPackedEncodingIdx = 5;
    private const ushort DecimalEncodingIdx = 6;
    private const ushort ConstantEncodingIdx = 7;
    private const ushort ForEncodingIdx = 8;
    private const ushort DeltaEncodingIdx = 9;
    private const ushort DictEncodingIdx = 10;
    private const ushort RleEncodingIdx = 11;
    private const ushort StructEncodingIdx = 12;
    private const ushort AlpEncodingIdx = 13;
    private const ushort RunEndEncodingIdx = 14;
    private const ushort SparseEncodingIdx = 15;
    private const ushort FsstStringEncodingIdx = 16;
    private const ushort AlpRdEncodingIdx = 17;
    private const ushort VarBinViewEncodingIdx = 18;
    private static readonly EncodingIndices Indices = new(
        Primitive: PrimitiveEncodingIdx,
        Bool: BoolEncodingIdx,
        VarBin: VarBinEncodingIdx,
        List: ListEncodingIdx,
        FixedSizeList: FixedSizeListEncodingIdx,
        BitPacked: BitPackedEncodingIdx,
        Decimal: DecimalEncodingIdx,
        Constant: ConstantEncodingIdx,
        For: ForEncodingIdx,
        Delta: DeltaEncodingIdx,
        Dict: DictEncodingIdx,
        Rle: RleEncodingIdx,
        Struct_: StructEncodingIdx,
        Alp: AlpEncodingIdx,
        RunEnd: RunEndEncodingIdx,
        Sparse: SparseEncodingIdx,
        FsstString: FsstStringEncodingIdx,
        AlpRd: AlpRdEncodingIdx,
        VarBinView: VarBinViewEncodingIdx);

    // Layout-spec registry constants.
    private const ushort FlatLayoutIdx = 0;
    private const ushort StructLayoutIdx = 1;
    private const ushort ChunkedLayoutIdx = 2;
    private const ushort StatsLayoutIdx = 3;

    private readonly Stream _stream;
    private readonly Apache.Arrow.Schema _schema;
    private readonly SegmentWriter _sw;
    /// <summary>One per column; each entry collects (segment_idx) per WriteBatch call.</summary>
    private readonly List<uint>[] _columnSegmentsByBatch;
    /// <summary>One per column; per-batch null counts for the zoned-stats layout.</summary>
    private readonly List<ulong>[] _columnNullCountsByBatch;
    /// <summary>One per column; the zones-table schema scheme for this column's type.</summary>
    private readonly ZoneStatScheme[] _columnStatScheme;
    /// <summary>For IntMinMaxNull columns: per-batch min/max bytes (column-width raw bytes)
    /// plus a has-value flag (false when the batch was all-null).</summary>
    private readonly List<byte[]?>[] _columnMinByBatch;
    private readonly List<byte[]?>[] _columnMaxByBatch;
    private readonly List<ulong> _batchRowCounts = new();
    private readonly bool _compress;
    private readonly bool _preferVarBinView;
    private readonly bool _preserveStats;
    private bool _closed;

    /// <summary>
    /// Stat schema for a column's per-zone table. Determines the column set
    /// in the auxiliary zones array AND the bitset that lands in the
    /// vortex.stats layout's metadata. Per upstream's `present_stats` rules
    /// the order is sorted ascending by Stat enum value.
    /// </summary>
    private enum ZoneStatScheme
    {
        /// <summary>
        /// bitset = [0x40, 0x00] — bit 6 (NullCount).
        /// zones struct = { null_count: u64 }.
        /// </summary>
        NullCountOnly,

        /// <summary>
        /// bitset = [0x58, 0x00] — bits 3 (Max), 4 (Min), 6 (NullCount).
        /// zones struct = { max: T?, max_is_truncated: bool,
        ///                  min: T?, min_is_truncated: bool,
        ///                  null_count: u64 }.
        /// Used for non-nullable / nullable integer columns where Min/Max
        /// are well-defined and cheap to capture per batch.
        /// </summary>
        IntMinMaxNull,
    }

    /// <summary>
    /// Begins a Vortex file. The <paramref name="schema"/> is fixed for the
    /// lifetime of the writer; subsequent <see cref="WriteBatch"/> calls must
    /// pass batches with a structurally-equal schema.
    /// </summary>
    /// <param name="compress">When true, opt eligible columns into compressing
    /// encodings (bitpacked, FoR, dict, ALP, FSST, runend, sparse, etc.).
    /// Default <c>false</c>.</param>
    /// <param name="preferVarBinView">When true, string columns that fall
    /// through the compressing-encoding chain (no constant / dict / FSST hit)
    /// land on <c>vortex.varbinview</c> instead of <c>vortex.varbin</c>.
    /// Useful for cross-tool interop with consumers that prefer Arrow's
    /// BinaryView shape; for short-string columns vortex.varbin is more
    /// compact (4 + len bytes/row vs varbinview's 16 + len bytes/row).</param>
    /// <param name="preserveStats">When true, wrap each column in a
    /// <c>vortex.stats</c> layout that carries a per-zone stats table
    /// (currently just <c>null_count</c> per zone). Lets pruning-aware
    /// readers skip whole zones without decoding them. Falls back to the
    /// non-zoned chunked layout when batch row counts aren't uniform (vortex
    /// requires all zones except the last to share <c>zone_len</c>).</param>
    public VortexFileWriter(
        Stream stream, Apache.Arrow.Schema schema,
        bool compress = false, bool preferVarBinView = false,
        bool preserveStats = false)
    {
        _compress = compress;
        _preferVarBinView = preferVarBinView;
        _preserveStats = preserveStats;
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _sw = new SegmentWriter(_stream);
        int nFields = schema.FieldsList.Count;
        _columnSegmentsByBatch = new List<uint>[nFields];
        _columnNullCountsByBatch = new List<ulong>[nFields];
        _columnMinByBatch = new List<byte[]?>[nFields];
        _columnMaxByBatch = new List<byte[]?>[nFields];
        _columnStatScheme = new ZoneStatScheme[nFields];
        for (int i = 0; i < nFields; i++)
        {
            _columnSegmentsByBatch[i] = new List<uint>();
            _columnNullCountsByBatch[i] = new List<ulong>();
            _columnMinByBatch[i] = new List<byte[]?>();
            _columnMaxByBatch[i] = new List<byte[]?>();
            _columnStatScheme[i] = SchemeForType(schema.FieldsList[i].DataType);
        }

        // Leading VTXF magic.
        var magicBytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(magicBytes, VortexFileFormat.MagicLE);
        _sw.WriteRaw(magicBytes);
    }

    /// <summary>Encodes <paramref name="batch"/> into one segment per column and appends them.</summary>
    public void WriteBatch(RecordBatch batch)
    {
        if (batch is null) throw new ArgumentNullException(nameof(batch));
        if (_closed) throw new InvalidOperationException("Writer has been closed.");
        if (batch.Schema.FieldsList.Count != _schema.FieldsList.Count)
            throw new ArgumentException(
                $"Batch has {batch.Schema.FieldsList.Count} columns but writer was opened with {_schema.FieldsList.Count}.");

        for (int i = 0; i < _schema.FieldsList.Count; i++)
        {
            var col = batch.Column(i);
            var sb = new SegmentBuilder();

            // Compute and emit per-column stats at the top-level ArrayNode.
            // Recursive child encoders skip stats (statsTicket=null) — adding
            // stats at every level would be wasteful and semantically wrong
            // for unrelated children (e.g., the offsets array's null_count
            // isn't the parent's).
            var statsValues = ArrayStatsComputer.Compute(col);
            int? statsTicket = ArrayStatsEmitter.Emit(sb.Builder, statsValues);

            int rootTicket = ArrayEncoderDispatch.Emit(
                sb, col, Indices, statsTicket, _compress, statsValues, _preferVarBinView);
            byte[] bytes = sb.FinishSegment(rootTicket);
            uint segIdx = _sw.AppendSegment(bytes, alignmentExponent: 0);
            _columnSegmentsByBatch[i].Add(segIdx);
            // Capture this batch's null_count so we can build a per-zone stats
            // table at Close. Apache.Arrow's typed array exposes NullCount via
            // the underlying Data; using GetNullCount handles the lazy-count
            // case for sliced columns even though we don't slice at the
            // top-level here.
            _columnNullCountsByBatch[i].Add((ulong)((Apache.Arrow.Array)col).Data.GetNullCount());

            // For integer columns, also capture per-batch min/max for the
            // zones table. Phase B emits these alongside null_count so the
            // reader (when it grows pruning) can skip whole zones via
            // predicate evaluation against the min/max bounds.
            if (_columnStatScheme[i] == ZoneStatScheme.IntMinMaxNull)
            {
                var (minBytes, maxBytes) = ComputeIntMinMax(col);
                _columnMinByBatch[i].Add(minBytes);
                _columnMaxByBatch[i].Add(maxBytes);
            }
        }
        _batchRowCounts.Add(checked((ulong)batch.Length));
    }

    /// <summary>
    /// Returns true iff the buffered batches form a valid zoned layout —
    /// every non-final batch has the same row count and the final batch's
    /// row count is &lt;= that. Vortex's zoned-layout spec requires zones
    /// of identical length except for the trailing partial zone.
    /// </summary>
    private bool CanZoneBatches()
    {
        int n = _batchRowCounts.Count;
        if (n == 0) return false;
        if (n == 1) return true;
        ulong first = _batchRowCounts[0];
        for (int b = 1; b < n - 1; b++)
            if (_batchRowCounts[b] != first) return false;
        return _batchRowCounts[n - 1] <= first;
    }

    private static ZoneStatScheme SchemeForType(Apache.Arrow.Types.IArrowType type) => type switch
    {
        Apache.Arrow.Types.Int8Type or Apache.Arrow.Types.Int16Type
            or Apache.Arrow.Types.Int32Type or Apache.Arrow.Types.Int64Type
            or Apache.Arrow.Types.UInt8Type or Apache.Arrow.Types.UInt16Type
            or Apache.Arrow.Types.UInt32Type or Apache.Arrow.Types.UInt64Type
            => ZoneStatScheme.IntMinMaxNull,
        _ => ZoneStatScheme.NullCountOnly,
    };

    /// <summary>
    /// Returns the bitset bytes for the layout-metadata's `present_stats`
    /// field. Layout matches upstream's `as_stat_bitset_bytes`: 9-bit
    /// packed field (Stat enum values 0..8), 2 bytes total.
    /// </summary>
    private static byte[] BitsetForScheme(ZoneStatScheme scheme) => scheme switch
    {
        // Bit 6 (NullCount) only.
        ZoneStatScheme.NullCountOnly => new byte[] { 0x40, 0x00 },
        // Bits 3 (Max) | 4 (Min) | 6 (NullCount) = 0x58 in byte 0.
        ZoneStatScheme.IntMinMaxNull => new byte[] { 0x58, 0x00 },
        _ => throw new NotSupportedException(),
    };

    private static int ByteWidthForIntType(Apache.Arrow.Types.IArrowType type) => type switch
    {
        Apache.Arrow.Types.Int8Type or Apache.Arrow.Types.UInt8Type => 1,
        Apache.Arrow.Types.Int16Type or Apache.Arrow.Types.UInt16Type => 2,
        Apache.Arrow.Types.Int32Type or Apache.Arrow.Types.UInt32Type => 4,
        Apache.Arrow.Types.Int64Type or Apache.Arrow.Types.UInt64Type => 8,
        _ => throw new NotSupportedException(),
    };

    /// <summary>
    /// Computes (min, max) over an integer column's non-null rows. Returns
    /// the raw bytes (column-width LE) for each, or <c>null</c> when the
    /// column is empty / all-null in this batch — the zones-table cell
    /// for that batch then has its validity bit cleared.
    /// </summary>
    private static (byte[]? Min, byte[]? Max) ComputeIntMinMax(Apache.Arrow.IArrowArray col)
    {
        var data = ((Apache.Arrow.Array)col).Data;
        int n = col.Length;
        if (n == 0) return (null, null);
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        int off = data.Offset;

        bool isSigned = col is Apache.Arrow.Int8Array or Apache.Arrow.Int16Array
            or Apache.Arrow.Int32Array or Apache.Arrow.Int64Array;
        int byteWidth = ByteWidthForIntType(((Apache.Arrow.Array)col).Data.DataType);
        var src = data.Buffers[1].Span.Slice(off * byteWidth, n * byteWidth);

        bool any = false;
        long sMin = long.MaxValue, sMax = long.MinValue;
        ulong uMin = ulong.MaxValue, uMax = ulong.MinValue;

        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = off + i;
                if ((validity[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            int p = i * byteWidth;
            if (isSigned)
            {
                long v = byteWidth switch
                {
                    1 => (sbyte)src[p],
                    2 => System.Buffers.Binary.BinaryPrimitives.ReadInt16LittleEndian(src.Slice(p, 2)),
                    4 => System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(src.Slice(p, 4)),
                    8 => System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(src.Slice(p, 8)),
                    _ => throw new NotSupportedException(),
                };
                if (!any) { sMin = sMax = v; any = true; }
                else { if (v < sMin) sMin = v; if (v > sMax) sMax = v; }
            }
            else
            {
                ulong v = byteWidth switch
                {
                    1 => src[p],
                    2 => System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(p, 2)),
                    4 => System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(p, 4)),
                    8 => System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(src.Slice(p, 8)),
                    _ => throw new NotSupportedException(),
                };
                if (!any) { uMin = uMax = v; any = true; }
                else { if (v < uMin) uMin = v; if (v > uMax) uMax = v; }
            }
        }

        if (!any) return (null, null);

        var minBytes = new byte[byteWidth];
        var maxBytes = new byte[byteWidth];
        if (isSigned)
        {
            WriteIntLE(minBytes, sMin, byteWidth);
            WriteIntLE(maxBytes, sMax, byteWidth);
        }
        else
        {
            WriteIntLE(minBytes, unchecked((long)uMin), byteWidth);
            WriteIntLE(maxBytes, unchecked((long)uMax), byteWidth);
        }
        return (minBytes, maxBytes);
    }

    private static void WriteIntLE(Span<byte> dest, long value, int byteWidth)
    {
        switch (byteWidth)
        {
            case 1: dest[0] = (byte)value; break;
            case 2: System.Buffers.Binary.BinaryPrimitives.WriteInt16LittleEndian(dest, (short)value); break;
            case 4: System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(dest, (int)value); break;
            case 8: System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(dest, value); break;
            default: throw new NotSupportedException();
        }
    }

    /// <summary>
    /// Builds and appends a per-column zones segment. Dispatches on the
    /// column's <see cref="ZoneStatScheme"/> — NullCountOnly emits a
    /// 1-field struct, IntMinMaxNull emits 5 fields.
    /// </summary>
    private uint EmitZonesSegment(int columnIdx)
    {
        return _columnStatScheme[columnIdx] switch
        {
            ZoneStatScheme.NullCountOnly => EmitZonesSegmentNullCountOnly(columnIdx),
            ZoneStatScheme.IntMinMaxNull => EmitZonesSegmentIntMinMaxNull(columnIdx),
            _ => throw new NotSupportedException(),
        };
    }

    private uint EmitZonesSegmentNullCountOnly(int columnIdx)
    {
        var counts = _columnNullCountsByBatch[columnIdx];
        int numZones = counts.Count;
        var bytes = new byte[(long)numZones * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(bytes.AsSpan());
        for (int i = 0; i < numZones; i++) span[i] = counts[i];

        var sb = new SegmentBuilder();
        ushort u64BufIdx = sb.AddBuffer(bytes, alignmentExponent: 3);
        int u64Ticket = ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, PrimitiveEncodingIdx, u64BufIdx);
        int structTicket = ArrayNodeEmitter.EmitWithChildrenOnly(
            sb.Builder, StructEncodingIdx, new[] { u64Ticket });
        byte[] segBytes = sb.FinishSegment(structTicket);
        return _sw.AppendSegment(segBytes, alignmentExponent: 0);
    }

    /// <summary>
    /// Builds the zones segment for an integer column carrying
    /// <c>{ max: T?, max_is_truncated: bool, min: T?, min_is_truncated: bool,
    /// null_count: u64 }</c>. Field order matches upstream's
    /// <c>stats_table_dtype</c> for present_stats = [Max, Min, NullCount].
    /// </summary>
    private uint EmitZonesSegmentIntMinMaxNull(int columnIdx)
    {
        var nullCounts = _columnNullCountsByBatch[columnIdx];
        var minByBatch = _columnMinByBatch[columnIdx];
        var maxByBatch = _columnMaxByBatch[columnIdx];
        int numZones = nullCounts.Count;
        int byteWidth = ByteWidthForIntType(_schema.FieldsList[columnIdx].DataType);

        // Build min/max buffers with validity (null when batch was all-null
        // → has-value flag is false → minByBatch[i] / maxByBatch[i] is null).
        var minBytes = new byte[(long)numZones * byteWidth];
        var maxBytes = new byte[(long)numZones * byteWidth];
        int validityByteCount = (numZones + 7) / 8;
        var minMaxValidity = new byte[validityByteCount];
        // Pre-fill validity to all-1s (most batches have data); clear for null entries.
        for (int i = 0; i < validityByteCount; i++) minMaxValidity[i] = 0xFF;
        // Mask trailing garbage bits in the last byte.
        if ((numZones & 7) != 0)
            minMaxValidity[validityByteCount - 1] &= (byte)((1 << (numZones & 7)) - 1);
        int minMaxNullCount = 0;
        for (int i = 0; i < numZones; i++)
        {
            if (minByBatch[i] is null)
            {
                minMaxValidity[i >> 3] &= (byte)~(1 << (i & 7));
                minMaxNullCount++;
            }
            else
            {
                minByBatch[i].AsSpan().CopyTo(minBytes.AsSpan(i * byteWidth, byteWidth));
                maxByBatch[i]!.AsSpan().CopyTo(maxBytes.AsSpan(i * byteWidth, byteWidth));
            }
        }

        // null_count column (always non-null).
        var ncBytes = new byte[(long)numZones * 8];
        var ncSpan = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(ncBytes.AsSpan());
        for (int i = 0; i < numZones; i++) ncSpan[i] = nullCounts[i];

        // *_is_truncated columns are always all-false here — we never
        // truncate. Build a numZones-bit zero bitmap.
        var truncatedBytes = new byte[(numZones + 7) / 8];

        // Now emit the 5-field struct: max, max_is_truncated, min,
        // min_is_truncated, null_count. Each field is its own ArrayNode.
        var sb = new SegmentBuilder();

        // 1. max: vortex.primitive with value buffer + validity child.
        int maxTicket = EmitNullablePrimitive(sb, maxBytes, byteWidth, minMaxValidity, minMaxNullCount);

        // 2. max_is_truncated: vortex.bool with the all-zero bitmap.
        int maxTruncTicket = EmitBoolBitmap(sb, truncatedBytes);

        // 3. min: same shape as max (shares the same min/max validity).
        int minTicket = EmitNullablePrimitive(sb, minBytes, byteWidth, minMaxValidity, minMaxNullCount);

        // 4. min_is_truncated: bool bitmap (all-zero).
        int minTruncTicket = EmitBoolBitmap(sb, truncatedBytes);

        // 5. null_count: vortex.primitive (non-nullable u64).
        ushort ncBufIdx = sb.AddBuffer(ncBytes, alignmentExponent: 3);
        int ncTicket = ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, PrimitiveEncodingIdx, ncBufIdx);

        int structTicket = ArrayNodeEmitter.EmitWithChildrenOnly(
            sb.Builder, StructEncodingIdx,
            new[] { maxTicket, maxTruncTicket, minTicket, minTruncTicket, ncTicket });
        byte[] segBytes = sb.FinishSegment(structTicket);
        return _sw.AppendSegment(segBytes, alignmentExponent: 0);
    }

    private int EmitNullablePrimitive(
        SegmentBuilder sb, byte[] valueBytes, int byteWidth,
        byte[] validity, int nullCount)
    {
        // Pick alignment per width: 8 → 3, 4 → 2, 2 → 1, 1 → 0.
        byte alignExp = byteWidth switch { 1 => 0, 2 => 1, 4 => 2, 8 => 3, _ => 0 };
        ushort valBuf = sb.AddBuffer(valueBytes, alignmentExponent: alignExp);
        if (nullCount == 0)
        {
            return ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, PrimitiveEncodingIdx, valBuf);
        }
        ushort validityBuf = sb.AddBuffer(validity, alignmentExponent: 0);
        int validityNode = ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, BoolEncodingIdx, validityBuf);
        return ArrayNodeEmitter.EmitWithBufferAndChildren(
            sb.Builder, PrimitiveEncodingIdx, valBuf, new[] { validityNode });
    }

    private int EmitBoolBitmap(SegmentBuilder sb, byte[] bitmap)
    {
        ushort bufIdx = sb.AddBuffer(bitmap, alignmentExponent: 0);
        return ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, BoolEncodingIdx, bufIdx);
    }

    /// <summary>
    /// Finalizes the file: emits DType, Layout, Footer, Postscript, EndOfFile.
    /// Idempotent — calling twice is a no-op. Disposing also calls Close.
    /// </summary>
    public void Close()
    {
        if (_closed) return;
        _closed = true;

        ulong totalRows = 0;
        for (int b = 0; b < _batchRowCounts.Count; b++) totalRows += _batchRowCounts[b];

        // 1. DType.
        var dtypeBytes = DTypeSerializer.SerializeSchema(_schema);
        var dtypeBlock = _sw.AppendPostscriptBlock(dtypeBytes);

        // 2. Layout.
        // Strategy:
        //   - Single batch: vortex.struct(vortex.flat × N).
        //   - Multi batch:  vortex.struct(vortex.chunked(vortex.flat × M) × N).
        //   - With preserveStats AND zone-eligible batch sizes: each column is
        //     additionally wrapped in vortex.stats(data, zones) so a
        //     pruning-aware reader can skip whole zones via the zones table.
        // preserveStats falls back to the non-zoned chunked layout when
        // batch row counts aren't uniform (vortex requires all zones except
        // the last to share zone_len).
        bool zoned = _preserveStats && _columnSegmentsByBatch.Length > 0 && CanZoneBatches();
        byte[] layoutBytes;
        if (_batchRowCounts.Count == 0)
        {
            throw new InvalidOperationException(
                "Cannot finalize a Vortex file with zero batches written; write at least one batch first.");
        }

        if (zoned)
        {
            // Build per-column zones segment (one row per batch with the
            // batch's null_count). Returns the segment index for each column.
            var perColumnZonesSeg = new uint[_columnSegmentsByBatch.Length];
            for (int c = 0; c < perColumnZonesSeg.Length; c++)
                perColumnZonesSeg[c] = EmitZonesSegment(c);

            int zoneLen = checked((int)_batchRowCounts[0]);
            int numZones = _batchRowCounts.Count;
            var perColumnSegByBatch = new uint[_columnSegmentsByBatch.Length][];
            var perColumnBitset = new byte[_columnSegmentsByBatch.Length][];
            for (int i = 0; i < perColumnSegByBatch.Length; i++)
            {
                perColumnSegByBatch[i] = _columnSegmentsByBatch[i].ToArray();
                perColumnBitset[i] = BitsetForScheme(_columnStatScheme[i]);
            }
            layoutBytes = LayoutSerializer.SerializeStructStatsChunked(
                StructLayoutIdx, StatsLayoutIdx, ChunkedLayoutIdx, FlatLayoutIdx,
                totalRows, zoneLen, numZones,
                _batchRowCounts, perColumnSegByBatch, perColumnZonesSeg, perColumnBitset);
        }
        else if (_batchRowCounts.Count == 1)
        {
            var perColumnSeg = new uint[_columnSegmentsByBatch.Length];
            for (int i = 0; i < perColumnSeg.Length; i++)
                perColumnSeg[i] = _columnSegmentsByBatch[i][0];
            layoutBytes = LayoutSerializer.SerializeStructFlat(
                StructLayoutIdx, FlatLayoutIdx, totalRows, perColumnSeg);
        }
        else
        {
            var perColumnSegByBatch = new uint[_columnSegmentsByBatch.Length][];
            for (int i = 0; i < perColumnSegByBatch.Length; i++)
                perColumnSegByBatch[i] = _columnSegmentsByBatch[i].ToArray();
            layoutBytes = LayoutSerializer.SerializeStructChunked(
                StructLayoutIdx, ChunkedLayoutIdx, FlatLayoutIdx,
                totalRows, _batchRowCounts, perColumnSegByBatch);
        }
        var layoutBlock = _sw.AppendPostscriptBlock(layoutBytes);

        // 3. Footer.
        var footerBytes = FooterSerializer.Serialize(
            arraySpecs: new[]
            {
                VortexArrayEncodings.Primitive,
                VortexArrayEncodings.Bool,
                VortexArrayEncodings.VarBin,
                VortexArrayEncodings.List,
                VortexArrayEncodings.FixedSizeList,
                VortexArrayEncodings.FastlanesBitPacked,
                VortexArrayEncodings.Decimal,
                VortexArrayEncodings.Constant,
                VortexArrayEncodings.FastlanesFor,
                VortexArrayEncodings.FastlanesDelta,
                VortexArrayEncodings.Dict,
                VortexArrayEncodings.FastlanesRle,
                VortexArrayEncodings.Struct_,
                VortexArrayEncodings.Alp,
                VortexArrayEncodings.RunEnd,
                VortexArrayEncodings.Sparse,
                VortexArrayEncodings.FsstString,
                VortexArrayEncodings.AlpRD,
                VortexArrayEncodings.VarBinView,
            },
            layoutSpecs: new[] { VortexLayoutEncodings.Flat, VortexLayoutEncodings.Struct, VortexLayoutEncodings.Chunked, VortexLayoutEncodings.Stats },
            segmentSpecs: _sw.SegmentSpecs);
        var footerBlock = _sw.AppendPostscriptBlock(footerBytes);

        // 4. Postscript.
        var postscriptBytes = PostscriptSerializer.Serialize(dtypeBlock, layoutBlock, footerBlock);
        if (postscriptBytes.Length > VortexFileFormat.MaxPostscriptLen)
            throw new InvalidOperationException(
                $"Postscript size {postscriptBytes.Length} exceeds the format maximum ({VortexFileFormat.MaxPostscriptLen}).");
        _sw.WriteRaw(postscriptBytes);

        // 5. EndOfFile struct.
        var eof = new byte[VortexFileFormat.EndOfFileSize];
        BinaryPrimitives.WriteUInt16LittleEndian(eof.AsSpan(0), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(eof.AsSpan(2), (ushort)postscriptBytes.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(eof.AsSpan(4), VortexFileFormat.MagicLE);
        _sw.WriteRaw(eof);
    }

    public void Dispose()
    {
        // Don't let a finalize-time exception (e.g., "no batches") mask the
        // original exception that caused the using-block to unwind. Swallow
        // anything from Close so the caller sees the root cause.
        try { Close(); }
        catch { _closed = true; }
    }

    /// <summary>One-shot convenience: writes <paramref name="batch"/> as a single-batch file.</summary>
    public static void Write(
        Stream stream, RecordBatch batch,
        bool compress = false, bool preferVarBinView = false,
        bool preserveStats = false)
    {
        if (batch is null) throw new ArgumentNullException(nameof(batch));
        using var writer = new VortexFileWriter(
            stream, batch.Schema, compress, preferVarBinView, preserveStats);
        writer.WriteBatch(batch);
        writer.Close();
    }
}
