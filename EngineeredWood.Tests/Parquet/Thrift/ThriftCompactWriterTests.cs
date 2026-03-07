using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Tests.Parquet.Thrift;

public class ThriftCompactWriterTests
{
    [Fact]
    public void WriteByte_SingleByte()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteByte(0x42);
        Assert.Equal(new byte[] { 0x42 }, writer.ToArray());
    }

    [Fact]
    public void WriteVarint_SingleByte()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteVarint(42);
        Assert.Equal(new byte[] { 0x2A }, writer.ToArray());
    }

    [Fact]
    public void WriteVarint_MultiByte()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteVarint(300);
        Assert.Equal(new byte[] { 0xAC, 0x02 }, writer.ToArray());
    }

    [Fact]
    public void WriteVarint_LargeValue()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteVarint(624485);
        Assert.Equal(new byte[] { 0xE5, 0x8E, 0x26 }, writer.ToArray());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(42)]
    [InlineData(-42)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void ZigZagInt32_RoundTrips(int value)
    {
        var writer = new ThriftCompactWriter();
        writer.WriteZigZagInt32(value);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(value, reader.ReadZigZagInt32());
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(42L)]
    [InlineData(-42L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void ZigZagInt64_RoundTrips(long value)
    {
        var writer = new ThriftCompactWriter();
        writer.WriteZigZagInt64(value);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(value, reader.ReadZigZagInt64());
    }

    [Fact]
    public void WriteDouble_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteDouble(3.14159265358979);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(3.14159265358979, reader.ReadDouble());
    }

    [Fact]
    public void WriteBinary_RoundTrips()
    {
        var data = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var writer = new ThriftCompactWriter();
        writer.WriteBinary(data);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(data, reader.ReadBinary().ToArray());
    }

    [Fact]
    public void WriteString_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteString("Hello, Thrift!");

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal("Hello, Thrift!", reader.ReadString());
    }

    [Fact]
    public void WriteString_Unicode_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteString("日本語テスト🎉");

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal("日本語テスト🎉", reader.ReadString());
    }

    [Fact]
    public void WriteBool_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteBool(true);
        writer.WriteBool(false);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.True(reader.ReadBool());
        Assert.False(reader.ReadBool());
    }

    [Fact]
    public void FieldHeader_ShortForm_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        // Field IDs 1, 2, 3 — small deltas → short form
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteFieldHeader(ThriftType.Binary, 2);
        writer.WriteFieldHeader(ThriftType.I64, 3);
        writer.WriteFieldStop();

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (t1, id1) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.I32, t1);
        Assert.Equal(1, id1);

        var (t2, id2) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Binary, t2);
        Assert.Equal(2, id2);

        var (t3, id3) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.I64, t3);
        Assert.Equal(3, id3);

        var (t4, _) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Stop, t4);
    }

    [Fact]
    public void FieldHeader_LongForm_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        // Large jump → long form
        writer.WriteFieldHeader(ThriftType.I32, 100);
        writer.WriteFieldStop();

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (t1, id1) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.I32, t1);
        Assert.Equal(100, id1);
    }

    [Fact]
    public void BoolField_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteBoolField(1, true);
        writer.WriteBoolField(2, false);
        writer.WriteFieldStop();

        var reader = new ThriftCompactReader(writer.WrittenSpan);

        var (t1, id1) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.BooleanTrue, t1);
        Assert.Equal(1, id1);
        Assert.True(reader.ReadBool());

        var (t2, id2) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.BooleanFalse, t2);
        Assert.Equal(2, id2);
        Assert.False(reader.ReadBool());
    }

    [Fact]
    public void ListHeader_SmallCount_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteListHeader(ThriftType.I32, 5);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (elemType, count) = reader.ReadListHeader();
        Assert.Equal(ThriftType.I32, elemType);
        Assert.Equal(5, count);
    }

    [Fact]
    public void ListHeader_LargeCount_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteListHeader(ThriftType.Struct, 200);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (elemType, count) = reader.ReadListHeader();
        Assert.Equal(ThriftType.Struct, elemType);
        Assert.Equal(200, count);
    }

    [Fact]
    public void MapHeader_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteMapHeader(ThriftType.Binary, ThriftType.I64, 10);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (keyType, valueType, count) = reader.ReadMapHeader();
        Assert.Equal(ThriftType.Binary, keyType);
        Assert.Equal(ThriftType.I64, valueType);
        Assert.Equal(10, count);
    }

    [Fact]
    public void MapHeader_Empty_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteMapHeader(ThriftType.Binary, ThriftType.I64, 0);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (_, _, count) = reader.ReadMapHeader();
        Assert.Equal(0, count);
    }

    [Fact]
    public void NestedStruct_RoundTrips()
    {
        var writer = new ThriftCompactWriter();
        writer.PushStruct();
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(42);
        writer.WriteFieldHeader(ThriftType.Struct, 2);
        {
            writer.PushStruct();
            writer.WriteFieldHeader(ThriftType.Binary, 1);
            writer.WriteString("inner");
            writer.WriteFieldStop();
            writer.PopStruct();
        }
        writer.WriteFieldHeader(ThriftType.I64, 3);
        writer.WriteZigZagInt64(999);
        writer.WriteFieldStop();
        writer.PopStruct();

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        reader.PushStruct();

        var (t1, id1) = reader.ReadFieldHeader();
        Assert.Equal(1, id1);
        Assert.Equal(42, reader.ReadZigZagInt32());

        var (t2, id2) = reader.ReadFieldHeader();
        Assert.Equal(2, id2);
        reader.PushStruct();
        var (t2a, _) = reader.ReadFieldHeader();
        Assert.Equal("inner", reader.ReadString());
        var (tStop, _) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Stop, tStop);
        reader.PopStruct();

        var (t3, id3) = reader.ReadFieldHeader();
        Assert.Equal(3, id3);
        Assert.Equal(999, reader.ReadZigZagInt64());

        var (t4, _) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Stop, t4);
        reader.PopStruct();
    }

    [Fact]
    public void Reset_AllowsReuse()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteZigZagInt32(42);
        Assert.True(writer.Position > 0);

        writer.Reset();
        Assert.Equal(0, writer.Position);

        writer.WriteZigZagInt32(99);
        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(99, reader.ReadZigZagInt32());
    }
}
