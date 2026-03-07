using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes Parquet page headers (DataPageHeader V1, DataPageHeaderV2, DictionaryPageHeader)
/// using the Thrift compact protocol. Mirror of <see cref="PageHeaderDecoder"/>.
/// </summary>
internal static class PageHeaderEncoder
{
    /// <summary>
    /// Encodes a complete <see cref="PageHeader"/> to Thrift bytes.
    /// </summary>
    public static byte[] Encode(PageHeader header)
    {
        var writer = new ThriftCompactWriter(64);
        writer.PushStruct();

        // Field 1: type (i32)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32((int)header.Type);

        // Field 2: uncompressed_page_size (i32)
        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32(header.UncompressedPageSize);

        // Field 3: compressed_page_size (i32)
        writer.WriteFieldHeader(ThriftType.I32, 3);
        writer.WriteZigZagInt32(header.CompressedPageSize);

        // Field 5: data_page_header (struct) — V1
        if (header.DataPageHeader is { } dph)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 5);
            EncodeDataPageHeader(writer, dph);
        }

        // Field 7: dictionary_page_header (struct)
        if (header.DictionaryPageHeader is { } dict)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 7);
            EncodeDictionaryPageHeader(writer, dict);
        }

        // Field 8: data_page_header_v2 (struct)
        if (header.DataPageHeaderV2 is { } v2)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 8);
            EncodeDataPageHeaderV2(writer, v2);
        }

        writer.WriteFieldStop();
        writer.PopStruct();
        return writer.ToArray();
    }

    private static void EncodeDataPageHeader(ThriftCompactWriter writer, DataPageHeader dph)
    {
        writer.PushStruct();

        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(dph.NumValues);

        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32((int)dph.Encoding);

        writer.WriteFieldHeader(ThriftType.I32, 3);
        writer.WriteZigZagInt32((int)dph.DefinitionLevelEncoding);

        writer.WriteFieldHeader(ThriftType.I32, 4);
        writer.WriteZigZagInt32((int)dph.RepetitionLevelEncoding);

        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void EncodeDictionaryPageHeader(ThriftCompactWriter writer, DictionaryPageHeader dict)
    {
        writer.PushStruct();

        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(dict.NumValues);

        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32((int)dict.Encoding);

        writer.WriteFieldStop();
        writer.PopStruct();
    }

    private static void EncodeDataPageHeaderV2(ThriftCompactWriter writer, DataPageHeaderV2 v2)
    {
        writer.PushStruct();

        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(v2.NumValues);

        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32(v2.NumNulls);

        writer.WriteFieldHeader(ThriftType.I32, 3);
        writer.WriteZigZagInt32(v2.NumRows);

        writer.WriteFieldHeader(ThriftType.I32, 4);
        writer.WriteZigZagInt32((int)v2.Encoding);

        writer.WriteFieldHeader(ThriftType.I32, 5);
        writer.WriteZigZagInt32(v2.DefinitionLevelsByteLength);

        writer.WriteFieldHeader(ThriftType.I32, 6);
        writer.WriteZigZagInt32(v2.RepetitionLevelsByteLength);

        // Field 7: is_compressed (bool) — only write if false (default is true)
        if (!v2.IsCompressed)
        {
            writer.WriteBoolField(7, false);
        }

        writer.WriteFieldStop();
        writer.PopStruct();
    }
}
