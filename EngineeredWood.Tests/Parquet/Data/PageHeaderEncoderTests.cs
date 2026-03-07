using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class PageHeaderEncoderTests
{
    [Fact]
    public void DataPageV1_RoundTrips()
    {
        var original = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = 4096,
            CompressedPageSize = 3200,
            DataPageHeader = new DataPageHeader
            {
                NumValues = 1000,
                Encoding = Encoding.Plain,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            }
        };

        var encoded = PageHeaderEncoder.Encode(original);
        var decoded = PageHeaderDecoder.Decode(encoded, out int bytesRead);

        Assert.Equal(encoded.Length, bytesRead);
        Assert.Equal(PageType.DataPage, decoded.Type);
        Assert.Equal(4096, decoded.UncompressedPageSize);
        Assert.Equal(3200, decoded.CompressedPageSize);
        Assert.NotNull(decoded.DataPageHeader);
        Assert.Equal(1000, decoded.DataPageHeader!.NumValues);
        Assert.Equal(Encoding.Plain, decoded.DataPageHeader.Encoding);
        Assert.Equal(Encoding.Rle, decoded.DataPageHeader.DefinitionLevelEncoding);
        Assert.Equal(Encoding.Rle, decoded.DataPageHeader.RepetitionLevelEncoding);
    }

    [Fact]
    public void DictionaryPage_RoundTrips()
    {
        var original = new PageHeader
        {
            Type = PageType.DictionaryPage,
            UncompressedPageSize = 2048,
            CompressedPageSize = 1500,
            DictionaryPageHeader = new DictionaryPageHeader
            {
                NumValues = 256,
                Encoding = Encoding.PlainDictionary,
            }
        };

        var encoded = PageHeaderEncoder.Encode(original);
        var decoded = PageHeaderDecoder.Decode(encoded, out _);

        Assert.Equal(PageType.DictionaryPage, decoded.Type);
        Assert.Equal(2048, decoded.UncompressedPageSize);
        Assert.Equal(1500, decoded.CompressedPageSize);
        Assert.NotNull(decoded.DictionaryPageHeader);
        Assert.Equal(256, decoded.DictionaryPageHeader!.NumValues);
        Assert.Equal(Encoding.PlainDictionary, decoded.DictionaryPageHeader.Encoding);
    }

    [Fact]
    public void DataPageV2_RoundTrips()
    {
        var original = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = 8192,
            CompressedPageSize = 6000,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = 2000,
                NumNulls = 50,
                NumRows = 2000,
                Encoding = Encoding.DeltaBinaryPacked,
                DefinitionLevelsByteLength = 256,
                RepetitionLevelsByteLength = 0,
                IsCompressed = true,
            }
        };

        var encoded = PageHeaderEncoder.Encode(original);
        var decoded = PageHeaderDecoder.Decode(encoded, out _);

        Assert.Equal(PageType.DataPageV2, decoded.Type);
        Assert.Equal(8192, decoded.UncompressedPageSize);
        Assert.Equal(6000, decoded.CompressedPageSize);
        Assert.NotNull(decoded.DataPageHeaderV2);
        var v2 = decoded.DataPageHeaderV2!;
        Assert.Equal(2000, v2.NumValues);
        Assert.Equal(50, v2.NumNulls);
        Assert.Equal(2000, v2.NumRows);
        Assert.Equal(Encoding.DeltaBinaryPacked, v2.Encoding);
        Assert.Equal(256, v2.DefinitionLevelsByteLength);
        Assert.Equal(0, v2.RepetitionLevelsByteLength);
        Assert.True(v2.IsCompressed);
    }

    [Fact]
    public void DataPageV2_NotCompressed_RoundTrips()
    {
        var original = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = 1000,
            CompressedPageSize = 1000,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = 100,
                NumNulls = 0,
                NumRows = 100,
                Encoding = Encoding.Plain,
                DefinitionLevelsByteLength = 0,
                RepetitionLevelsByteLength = 0,
                IsCompressed = false,
            }
        };

        var encoded = PageHeaderEncoder.Encode(original);
        var decoded = PageHeaderDecoder.Decode(encoded, out _);

        Assert.False(decoded.DataPageHeaderV2!.IsCompressed);
    }

    [Fact]
    public void AllEncodings_RoundTrip()
    {
        foreach (Encoding enc in Enum.GetValues<Encoding>())
        {
            var original = new PageHeader
            {
                Type = PageType.DataPage,
                UncompressedPageSize = 100,
                CompressedPageSize = 100,
                DataPageHeader = new DataPageHeader
                {
                    NumValues = 10,
                    Encoding = enc,
                    DefinitionLevelEncoding = Encoding.Rle,
                    RepetitionLevelEncoding = Encoding.Rle,
                }
            };

            var encoded = PageHeaderEncoder.Encode(original);
            var decoded = PageHeaderDecoder.Decode(encoded, out _);

            Assert.Equal(enc, decoded.DataPageHeader!.Encoding);
        }
    }

    [Fact]
    public void LargeValues_RoundTrips()
    {
        var original = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = int.MaxValue,
            CompressedPageSize = int.MaxValue / 2,
            DataPageHeader = new DataPageHeader
            {
                NumValues = int.MaxValue,
                Encoding = Encoding.Plain,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            }
        };

        var encoded = PageHeaderEncoder.Encode(original);
        var decoded = PageHeaderDecoder.Decode(encoded, out _);

        Assert.Equal(int.MaxValue, decoded.UncompressedPageSize);
        Assert.Equal(int.MaxValue / 2, decoded.CompressedPageSize);
        Assert.Equal(int.MaxValue, decoded.DataPageHeader!.NumValues);
    }
}
