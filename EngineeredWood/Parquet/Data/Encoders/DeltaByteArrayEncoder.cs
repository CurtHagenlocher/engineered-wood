namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes byte arrays using DELTA_BYTE_ARRAY (incremental/prefix) encoding.
/// Format: [DELTA_BINARY_PACKED prefix lengths] [DELTA_LENGTH_BYTE_ARRAY suffixes]
/// </summary>
internal sealed class DeltaByteArrayEncoder
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
        if (_values.Count == 0)
        {
            // Empty: just two empty delta-encoded blocks
            var emptyDelta = new DeltaBinaryPackedEncoder();
            var emptyBytes = emptyDelta.Finish();
            var emptyDlba = new DeltaLengthByteArrayEncoder();
            var emptyDlbaBytes = emptyDlba.Finish();
            var result = new byte[emptyBytes.Length + emptyDlbaBytes.Length];
            emptyBytes.CopyTo(result, 0);
            emptyDlbaBytes.CopyTo(result, emptyBytes.Length);
            _values.Clear();
            return result;
        }

        // Compute prefix lengths and suffixes
        var prefixLengths = new int[_values.Count];
        var suffixes = new byte[_values.Count][];

        // First value has no prefix
        prefixLengths[0] = 0;
        suffixes[0] = _values[0];

        for (int i = 1; i < _values.Count; i++)
        {
            var prev = _values[i - 1];
            var curr = _values[i];
            int commonLen = CommonPrefixLength(prev, curr);
            prefixLengths[i] = commonLen;
            suffixes[i] = curr.AsSpan(commonLen).ToArray();
        }

        // Encode prefix lengths with DELTA_BINARY_PACKED
        var prefixEncoder = new DeltaBinaryPackedEncoder();
        foreach (int pl in prefixLengths)
            prefixEncoder.AddValue(pl);
        var prefixBytes = prefixEncoder.Finish();

        // Encode suffixes with DELTA_LENGTH_BYTE_ARRAY
        var suffixEncoder = new DeltaLengthByteArrayEncoder();
        foreach (var suffix in suffixes)
            suffixEncoder.AddValue(suffix);
        var suffixBytes = suffixEncoder.Finish();

        var output = new byte[prefixBytes.Length + suffixBytes.Length];
        prefixBytes.CopyTo(output, 0);
        suffixBytes.CopyTo(output, prefixBytes.Length);

        _values.Clear();
        return output;
    }

    private static int CommonPrefixLength(byte[] a, byte[] b)
    {
        int minLen = Math.Min(a.Length, b.Length);
        for (int i = 0; i < minLen; i++)
        {
            if (a[i] != b[i]) return i;
        }
        return minLen;
    }
}
