using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Data.Encoders;

namespace EngineeredWood.Tests.Parquet.Data.Encoders;

public class DictionaryEncoderTests
{
    [Fact]
    public void Int32_RoundTrips()
    {
        var encoder = new DictionaryEncoder(PhysicalType.Int32);
        var values = new[] { 10, 20, 10, 30, 20, 10 };
        foreach (int v in values)
            encoder.AddInt32(v);

        Assert.Equal(3, encoder.DictionarySize);
        Assert.Equal(6, encoder.ValueCount);

        var dictPage = encoder.EncodeDictionaryPage();
        var indexData = encoder.EncodeIndices();

        var dict = new DictionaryDecoder(PhysicalType.Int32);
        dict.Load(dictPage, encoder.DictionarySize, 0);

        int bitWidth = encoder.GetIndexBitWidth();
        var rleDecoder = new RleBitPackedDecoder(indexData, bitWidth);
        var indices = new int[values.Length];
        rleDecoder.ReadBatch(indices);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], dict.GetInt32(indices[i]));
    }

    [Fact]
    public void Int64_RoundTrips()
    {
        var encoder = new DictionaryEncoder(PhysicalType.Int64);
        var values = new[] { 100L, 200L, 100L, 300L };
        foreach (long v in values)
            encoder.AddInt64(v);

        Assert.Equal(3, encoder.DictionarySize);

        var dictPage = encoder.EncodeDictionaryPage();
        var indexData = encoder.EncodeIndices();

        var dict = new DictionaryDecoder(PhysicalType.Int64);
        dict.Load(dictPage, encoder.DictionarySize, 0);
        int bitWidth = encoder.GetIndexBitWidth();
        var rleDecoder = new RleBitPackedDecoder(indexData, bitWidth);
        var indices = new int[values.Length];
        rleDecoder.ReadBatch(indices);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], dict.GetInt64(indices[i]));
    }

    [Fact]
    public void Float_RoundTrips()
    {
        var encoder = new DictionaryEncoder(PhysicalType.Float);
        var values = new[] { 1.5f, 2.5f, 1.5f, 3.5f };
        foreach (float v in values)
            encoder.AddFloat(v);

        Assert.Equal(3, encoder.DictionarySize);

        var dictPage = encoder.EncodeDictionaryPage();
        var indexData = encoder.EncodeIndices();

        var dict = new DictionaryDecoder(PhysicalType.Float);
        dict.Load(dictPage, encoder.DictionarySize, 0);
        int bitWidth = encoder.GetIndexBitWidth();
        var rleDecoder = new RleBitPackedDecoder(indexData, bitWidth);
        var indices = new int[values.Length];
        rleDecoder.ReadBatch(indices);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], dict.GetFloat(indices[i]));
    }

    [Fact]
    public void Double_RoundTrips()
    {
        var encoder = new DictionaryEncoder(PhysicalType.Double);
        var values = new[] { 3.14, 2.71, 3.14 };
        foreach (double v in values)
            encoder.AddDouble(v);

        Assert.Equal(2, encoder.DictionarySize);

        var dictPage = encoder.EncodeDictionaryPage();
        var indexData = encoder.EncodeIndices();

        var dict = new DictionaryDecoder(PhysicalType.Double);
        dict.Load(dictPage, encoder.DictionarySize, 0);
        int bitWidth = encoder.GetIndexBitWidth();
        var rleDecoder = new RleBitPackedDecoder(indexData, bitWidth);
        var indices = new int[values.Length];
        rleDecoder.ReadBatch(indices);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], dict.GetDouble(indices[i]));
    }

    [Fact]
    public void ByteArray_RoundTrips()
    {
        var encoder = new DictionaryEncoder(PhysicalType.ByteArray);
        var strings = new[] { "hello"u8.ToArray(), "world"u8.ToArray(), "hello"u8.ToArray() };
        foreach (var s in strings)
            encoder.AddByteArray(s);

        Assert.Equal(2, encoder.DictionarySize);

        var dictPage = encoder.EncodeDictionaryPage();
        var indexData = encoder.EncodeIndices();

        var dict = new DictionaryDecoder(PhysicalType.ByteArray);
        dict.Load(dictPage, encoder.DictionarySize, 0);
        int bitWidth = encoder.GetIndexBitWidth();
        var rleDecoder = new RleBitPackedDecoder(indexData, bitWidth);
        var indices = new int[strings.Length];
        rleDecoder.ReadBatch(indices);

        for (int i = 0; i < strings.Length; i++)
            Assert.Equal(strings[i], dict.GetByteArray(indices[i]).ToArray());
    }

    [Fact]
    public void SingleDistinctValue_Works()
    {
        var encoder = new DictionaryEncoder(PhysicalType.Int32);
        for (int i = 0; i < 100; i++)
            encoder.AddInt32(42);

        Assert.Equal(1, encoder.DictionarySize);
        Assert.Equal(1, encoder.GetIndexBitWidth());

        var dictPage = encoder.EncodeDictionaryPage();
        var indexData = encoder.EncodeIndices();

        var dict = new DictionaryDecoder(PhysicalType.Int32);
        dict.Load(dictPage, encoder.DictionarySize, 0);
        int bitWidth = encoder.GetIndexBitWidth();
        var rleDecoder = new RleBitPackedDecoder(indexData, bitWidth);
        var indices = new int[100];
        rleDecoder.ReadBatch(indices);

        Assert.All(indices, idx => Assert.Equal(0, idx));
        Assert.Equal(42, dict.GetInt32(0));
    }

    [Fact]
    public void HighCardinality_RoundTrips()
    {
        var encoder = new DictionaryEncoder(PhysicalType.Int32);
        var values = Enumerable.Range(0, 1000).ToArray();
        foreach (int v in values)
            encoder.AddInt32(v);

        Assert.Equal(1000, encoder.DictionarySize);

        var dictPage = encoder.EncodeDictionaryPage();
        var indexData = encoder.EncodeIndices();

        var dict = new DictionaryDecoder(PhysicalType.Int32);
        dict.Load(dictPage, encoder.DictionarySize, 0);
        int bitWidth = encoder.GetIndexBitWidth();
        var rleDecoder = new RleBitPackedDecoder(indexData, bitWidth);
        var indices = new int[values.Length];
        rleDecoder.ReadBatch(indices);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], dict.GetInt32(indices[i]));
    }
}
