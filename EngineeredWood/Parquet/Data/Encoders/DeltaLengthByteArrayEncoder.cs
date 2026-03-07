namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes byte arrays using DELTA_LENGTH_BYTE_ARRAY encoding.
/// Format: [DELTA_BINARY_PACKED lengths] [concatenated raw byte data]
/// </summary>
internal sealed class DeltaLengthByteArrayEncoder
{
    private readonly List<byte[]> _values = new();

    public void AddValue(ReadOnlySpan<byte> value) => _values.Add(value.ToArray());

    public void AddValues(ReadOnlySpan<byte> flatData, ReadOnlySpan<int> offsets)
    {
        int count = offsets.Length - 1;
        for (int i = 0; i < count; i++)
        {
            int start = offsets[i];
            int length = offsets[i + 1] - start;
            _values.Add(flatData.Slice(start, length).ToArray());
        }
    }

    public byte[] Finish()
    {
        // Encode lengths using DELTA_BINARY_PACKED
        var lengthEncoder = new DeltaBinaryPackedEncoder();
        foreach (var value in _values)
            lengthEncoder.AddValue(value.Length);

        var lengthBytes = lengthEncoder.Finish();

        // Concatenate raw data
        int totalDataLen = 0;
        foreach (var value in _values)
            totalDataLen += value.Length;

        var result = new byte[lengthBytes.Length + totalDataLen];
        lengthBytes.CopyTo(result, 0);

        int pos = lengthBytes.Length;
        foreach (var value in _values)
        {
            value.CopyTo(result, pos);
            pos += value.Length;
        }

        _values.Clear();
        return result;
    }
}
