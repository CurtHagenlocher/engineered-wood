using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class ArrowArrayDecomposerTests
{
    [Fact]
    public void Int32_NonNullable_AllValuesCopied()
    {
        var builder = new Int32Array.Builder();
        builder.Append(1);
        builder.Append(2);
        builder.Append(3);
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.Int32, maxDefLevel: 0);

        Assert.Null(result.DefLevels);
        Assert.Equal(3, result.NonNullCount);
        Assert.NotNull(result.ValueBytes);
        Assert.Equal(12, result.ValueBytes!.Length); // 3 * 4 bytes

        var values = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(result.ValueBytes);
        Assert.Equal(1, values[0]);
        Assert.Equal(2, values[1]);
        Assert.Equal(3, values[2]);
    }

    [Fact]
    public void Int32_Nullable_ExtractsDenseValues()
    {
        var builder = new Int32Array.Builder();
        builder.Append(10);
        builder.AppendNull();
        builder.Append(30);
        builder.AppendNull();
        builder.Append(50);
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.Int32, maxDefLevel: 1);

        Assert.NotNull(result.DefLevels);
        Assert.Equal(new byte[] { 1, 0, 1, 0, 1 }, result.DefLevels);
        Assert.Equal(3, result.NonNullCount);

        var values = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(result.ValueBytes);
        Assert.Equal(3, values.Length);
        Assert.Equal(10, values[0]);
        Assert.Equal(30, values[1]);
        Assert.Equal(50, values[2]);
    }

    [Fact]
    public void Int64_Nullable_RoundTrips()
    {
        var builder = new Int64Array.Builder();
        builder.Append(100L);
        builder.Append(200L);
        builder.AppendNull();
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.Int64, maxDefLevel: 1);

        Assert.Equal(new byte[] { 1, 1, 0 }, result.DefLevels);
        Assert.Equal(2, result.NonNullCount);

        var values = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(result.ValueBytes);
        Assert.Equal(100L, values[0]);
        Assert.Equal(200L, values[1]);
    }

    [Fact]
    public void Float_NonNullable()
    {
        var builder = new FloatArray.Builder();
        builder.Append(1.5f);
        builder.Append(2.5f);
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.Float, maxDefLevel: 0);

        Assert.Null(result.DefLevels);
        var values = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, float>(result.ValueBytes);
        Assert.Equal(1.5f, values[0]);
        Assert.Equal(2.5f, values[1]);
    }

    [Fact]
    public void Double_Nullable_AllNull()
    {
        var builder = new DoubleArray.Builder();
        builder.AppendNull();
        builder.AppendNull();
        builder.AppendNull();
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.Double, maxDefLevel: 1);

        Assert.Equal(new byte[] { 0, 0, 0 }, result.DefLevels);
        Assert.Equal(0, result.NonNullCount);
        Assert.NotNull(result.ValueBytes);
        Assert.Empty(result.ValueBytes!);
    }

    [Fact]
    public void Boolean_Nullable()
    {
        var builder = new BooleanArray.Builder();
        builder.Append(true);
        builder.AppendNull();
        builder.Append(false);
        builder.Append(true);
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.Boolean, maxDefLevel: 1);

        Assert.Equal(new byte[] { 1, 0, 1, 1 }, result.DefLevels);
        Assert.Equal(3, result.NonNullCount);
        Assert.NotNull(result.BoolValues);
        Assert.Equal(new[] { true, false, true }, result.BoolValues);
    }

    [Fact]
    public void Boolean_NonNullable()
    {
        var builder = new BooleanArray.Builder();
        builder.Append(true);
        builder.Append(false);
        builder.Append(true);
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.Boolean, maxDefLevel: 0);

        Assert.Null(result.DefLevels);
        Assert.Equal(3, result.NonNullCount);
        Assert.Equal(new[] { true, false, true }, result.BoolValues);
    }

    [Fact]
    public void String_Nullable()
    {
        var builder = new StringArray.Builder();
        builder.Append("hello");
        builder.AppendNull();
        builder.Append("world");
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.ByteArray, maxDefLevel: 1);

        Assert.Equal(new byte[] { 1, 0, 1 }, result.DefLevels);
        Assert.Equal(2, result.NonNullCount);
        Assert.NotNull(result.ByteArrayData);
        Assert.NotNull(result.ByteArrayOffsets);

        // Offsets: [0, 5, 10]
        Assert.Equal(3, result.ByteArrayOffsets!.Length); // nonNullCount + 1
        Assert.Equal(0, result.ByteArrayOffsets[0]);
        Assert.Equal(5, result.ByteArrayOffsets[1]);
        Assert.Equal(10, result.ByteArrayOffsets[2]);

        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(result.ByteArrayData!.AsSpan(0, 5)));
        Assert.Equal("world", System.Text.Encoding.UTF8.GetString(result.ByteArrayData!.AsSpan(5, 5)));
    }

    [Fact]
    public void String_NonNullable()
    {
        var builder = new StringArray.Builder();
        builder.Append("foo");
        builder.Append("bar");
        builder.Append("baz");
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.ByteArray, maxDefLevel: 0);

        Assert.Null(result.DefLevels);
        Assert.Equal(3, result.NonNullCount);
        Assert.Equal(4, result.ByteArrayOffsets!.Length); // 3 + 1
        Assert.Equal(9, result.ByteArrayData!.Length); // 3 * 3
    }

    [Fact]
    public void Binary_Nullable()
    {
        var builder = new BinaryArray.Builder();
        builder.Append(new byte[] { 1, 2, 3 });
        builder.AppendNull();
        builder.Append(new byte[] { 4, 5 });
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.ByteArray, maxDefLevel: 1);

        Assert.Equal(2, result.NonNullCount);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, result.ByteArrayData);
        Assert.Equal(new[] { 0, 3, 5 }, result.ByteArrayOffsets);
    }

    [Fact]
    public void FixedSizeBinary_Nullable()
    {
        int typeLength = 4;
        var type = new FixedSizeBinaryType(typeLength);
        var validityBuilder = new ArrowBuffer.BitmapBuilder();
        validityBuilder.Append(true);
        validityBuilder.Append(false);
        validityBuilder.Append(true);

        var valueBytes = new byte[] { 1, 2, 3, 4, 0, 0, 0, 0, 5, 6, 7, 8 };
        var valueBuffer = new ArrowBuffer(valueBytes);
        var validityBuffer = validityBuilder.Build();

        var arrayData = new ArrayData(type, 3, 1, 0,
            new[] { validityBuffer, valueBuffer });
        var array = ArrowArrayFactory.BuildArray(arrayData);

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.FixedLenByteArray, maxDefLevel: 1, typeLength: typeLength);

        Assert.Equal(new byte[] { 1, 0, 1 }, result.DefLevels);
        Assert.Equal(2, result.NonNullCount);
        // Dense values: [1,2,3,4] + [5,6,7,8]
        Assert.Equal(8, result.ValueBytes!.Length);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, result.ValueBytes);
    }

    [Fact]
    public void Empty_Array()
    {
        var builder = new Int32Array.Builder();
        var array = builder.Build();

        var result = ArrowArrayDecomposer.Decompose(array, PhysicalType.Int32, maxDefLevel: 1);

        Assert.NotNull(result.DefLevels);
        Assert.Empty(result.DefLevels!);
        Assert.Equal(0, result.NonNullCount);
    }
}
