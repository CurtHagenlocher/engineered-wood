using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Generates in-memory test data for write benchmarks.
/// Provides Arrow RecordBatches (for EW) and raw typed arrays (for ParquetSharp/Parquet.NET).
/// Data matches the same profiles as the read benchmarks for fair comparison.
/// </summary>
public static class WriteTestDataGenerator
{
    public const string WideFlat = "wide_flat";
    public const string TallNarrow = "tall_narrow";
    public const string Snappy = "snappy";

    public const int WideFlatColumnCount = 50;
    public const int WideFlatRowCount = 100_000;
    public const int TallNarrowRowCount = 200_000; // single row group for benchmark
    private const double NullRate = 0.10;
    private const int StringPoolSize = 10_000;
    private const int StringMinLen = 5;
    private const int StringMaxLen = 100;

    /// <summary>Raw arrays for ParquetSharp/Parquet.NET writers.</summary>
    public sealed class RawData
    {
        public int[] Int32Required = null!;
        public int?[] Int32Nullable = null!;
        public long[] Int64Required = null!;
        public long?[] Int64Nullable = null!;
        public double[] DoubleRequired = null!;
        public double?[] DoubleNullable = null!;
        public byte[][] BytesRequired = null!;
        public byte[]?[] BytesNullable = null!;
        public string[] StringRequired = null!;
        public string?[] StringNullable = null!;
    }

    public sealed class WriteData
    {
        public RecordBatch Batch = null!;
        public RawData Raw = null!;
        public int RowCount;
        public int ColumnCount;
    }

    public static WriteData GenerateWideFlat(int rowCount = WideFlatRowCount)
    {
        var rng = new Random(42);
        int halfCols = WideFlatColumnCount / 2;
        var raw = GenerateRawArrays(rng, rowCount);

        var fields = new List<Field>();
        var arrays = new List<IArrowArray>();

        for (int i = 0; i < WideFlatColumnCount; i++)
        {
            bool nullable = i >= halfCols;
            string name = $"c{i}";

            switch (i % 4)
            {
                case 0: // int
                    fields.Add(new Field(name, Int32Type.Default, nullable));
                    arrays.Add(nullable
                        ? BuildNullableInt32(raw.Int32Nullable, rowCount, rng)
                        : BuildInt32(raw.Int32Required, rowCount));
                    break;
                case 1: // long
                    fields.Add(new Field(name, Int64Type.Default, nullable));
                    arrays.Add(nullable
                        ? BuildNullableInt64(raw.Int64Nullable, rowCount, rng)
                        : BuildInt64(raw.Int64Required, rowCount));
                    break;
                case 2: // double
                    fields.Add(new Field(name, DoubleType.Default, nullable));
                    arrays.Add(nullable
                        ? BuildNullableDouble(raw.DoubleNullable, rowCount, rng)
                        : BuildDouble(raw.DoubleRequired, rowCount));
                    break;
                default: // string (byte[])
                    fields.Add(new Field(name, Apache.Arrow.Types.StringType.Default, nullable));
                    arrays.Add(nullable
                        ? BuildNullableString(raw.StringNullable, rowCount, rng)
                        : BuildString(raw.StringRequired, rowCount));
                    break;
            }
        }

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields) schema.Field(f);

        return new WriteData
        {
            Batch = new RecordBatch(schema.Build(), arrays, rowCount),
            Raw = raw,
            RowCount = rowCount,
            ColumnCount = WideFlatColumnCount,
        };
    }

    public static WriteData GenerateTallNarrow(int rowCount = TallNarrowRowCount)
    {
        var rng = new Random(123);
        var raw = GenerateRawArrays(rng, rowCount);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("timestamp", Int64Type.Default, nullable: false))
            .Field(new Field("value", DoubleType.Default, nullable: true))
            .Field(new Field("tag", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Build();

        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            BuildInt32(raw.Int32Required, rowCount),
            BuildInt64(raw.Int64Required, rowCount),
            BuildNullableDouble(raw.DoubleNullable, rowCount, rng),
            BuildNullableString(raw.StringNullable, rowCount, rng),
        }, rowCount);

        return new WriteData
        {
            Batch = batch,
            Raw = raw,
            RowCount = rowCount,
            ColumnCount = 4,
        };
    }

    private static RawData GenerateRawArrays(Random rng, int rowCount)
    {
        var stringPool = new string[StringPoolSize];
        for (int i = 0; i < StringPoolSize; i++)
        {
            int len = rng.Next(StringMinLen, StringMaxLen + 1);
            var chars = new char[len];
            for (int j = 0; j < len; j++)
                chars[j] = (char)rng.Next(0x20, 0x7F);
            stringPool[i] = new string(chars);
        }

        var bytesPool = new byte[StringPoolSize][];
        for (int i = 0; i < StringPoolSize; i++)
            bytesPool[i] = System.Text.Encoding.UTF8.GetBytes(stringPool[i]);

        var data = new RawData
        {
            Int32Required = new int[rowCount],
            Int32Nullable = new int?[rowCount],
            Int64Required = new long[rowCount],
            Int64Nullable = new long?[rowCount],
            DoubleRequired = new double[rowCount],
            DoubleNullable = new double?[rowCount],
            BytesRequired = new byte[rowCount][],
            BytesNullable = new byte[]?[rowCount],
            StringRequired = new string[rowCount],
            StringNullable = new string?[rowCount],
        };

        for (int i = 0; i < rowCount; i++)
        {
            data.Int32Required[i] = rng.Next();
            data.Int32Nullable[i] = rng.NextDouble() < NullRate ? null : rng.Next();
            data.Int64Required[i] = rng.NextInt64();
            data.Int64Nullable[i] = rng.NextDouble() < NullRate ? null : rng.NextInt64();
            data.DoubleRequired[i] = rng.NextDouble() * 1_000_000.0 - 500_000.0;
            data.DoubleNullable[i] = rng.NextDouble() < NullRate ? null : rng.NextDouble() * 1_000_000.0 - 500_000.0;
            int idx = rng.Next(StringPoolSize);
            data.BytesRequired[i] = bytesPool[idx];
            data.StringRequired[i] = stringPool[idx];
            if (rng.NextDouble() < NullRate)
            {
                data.BytesNullable[i] = null;
                data.StringNullable[i] = null;
            }
            else
            {
                int idx2 = rng.Next(StringPoolSize);
                data.BytesNullable[i] = bytesPool[idx2];
                data.StringNullable[i] = stringPool[idx2];
            }
        }

        return data;
    }

    // Arrow array builders

    private static Int32Array BuildInt32(int[] values, int count)
    {
        var b = new Int32Array.Builder();
        for (int i = 0; i < count; i++) b.Append(values[i]);
        return b.Build();
    }

    private static Int32Array BuildNullableInt32(int?[] values, int count, Random _)
    {
        var b = new Int32Array.Builder();
        for (int i = 0; i < count; i++)
        {
            if (values[i] == null) b.AppendNull();
            else b.Append(values[i]!.Value);
        }
        return b.Build();
    }

    private static Int64Array BuildInt64(long[] values, int count)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < count; i++) b.Append(values[i]);
        return b.Build();
    }

    private static Int64Array BuildNullableInt64(long?[] values, int count, Random _)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
        {
            if (values[i] == null) b.AppendNull();
            else b.Append(values[i]!.Value);
        }
        return b.Build();
    }

    private static DoubleArray BuildDouble(double[] values, int count)
    {
        var b = new DoubleArray.Builder();
        for (int i = 0; i < count; i++) b.Append(values[i]);
        return b.Build();
    }

    private static DoubleArray BuildNullableDouble(double?[] values, int count, Random _)
    {
        var b = new DoubleArray.Builder();
        for (int i = 0; i < count; i++)
        {
            if (values[i] == null) b.AppendNull();
            else b.Append(values[i]!.Value);
        }
        return b.Build();
    }

    private static StringArray BuildString(string[] values, int count)
    {
        var b = new StringArray.Builder();
        for (int i = 0; i < count; i++) b.Append(values[i]);
        return b.Build();
    }

    private static StringArray BuildNullableString(string?[] values, int count, Random _)
    {
        var b = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            if (values[i] == null) b.AppendNull();
            else b.Append(values[i]!);
        }
        return b.Build();
    }
}
