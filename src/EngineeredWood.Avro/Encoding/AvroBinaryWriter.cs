using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using EngineeredWood.Buffers;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Encoding;

/// <summary>
/// Avro binary encoder that writes to a <see cref="GrowableBuffer"/>.
/// Uses the Avro binary encoding specification.
/// </summary>
internal sealed class AvroBinaryWriter
{
    private readonly GrowableBuffer _buffer;

    public AvroBinaryWriter(GrowableBuffer buffer)
    {
        _buffer = buffer;
    }

    public int Length => _buffer.Length;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteNull() { } // Avro null is zero bytes

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBoolean(bool value) => _buffer.WriteByte(value ? (byte)1 : (byte)0);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt(int value) => WriteLong(value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteLong(long value)
    {
        var span = _buffer.GetSpan(10);
        _buffer.Advance(Varint.WriteSigned(span, value));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteFloat(float value)
    {
        var span = _buffer.GetSpan(4);
        BinaryPrimitives.WriteSingleLittleEndian(span, value);
        _buffer.Advance(4);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteDouble(double value)
    {
        var span = _buffer.GetSpan(8);
        BinaryPrimitives.WriteDoubleLittleEndian(span, value);
        _buffer.Advance(8);
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteLong(value.Length);
        _buffer.Write(value);
    }

    public void WriteString(string value)
    {
        int byteCount = System.Text.Encoding.UTF8.GetByteCount(value);
        WriteLong(byteCount);
        var span = _buffer.GetSpan(byteCount);
        System.Text.Encoding.UTF8.GetBytes(value, span);
        _buffer.Advance(byteCount);
    }

    public void WriteString(ReadOnlySpan<byte> utf8Bytes)
    {
        WriteLong(utf8Bytes.Length);
        _buffer.Write(utf8Bytes);
    }

    public void WriteFixed(ReadOnlySpan<byte> value)
    {
        _buffer.Write(value);
    }

    /// <summary>Writes a union branch index (0-based).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUnionIndex(int index) => WriteLong(index);

    /// <summary>Returns the underlying buffer's written span.</summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.WrittenSpan;

    /// <summary>Returns the underlying buffer's written memory.</summary>
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.WrittenMemory;

    public void Reset() => _buffer.Reset();
}
