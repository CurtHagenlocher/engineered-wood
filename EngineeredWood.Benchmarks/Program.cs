using BenchmarkDotNet.Running;
using EngineeredWood.Benchmarks;

BenchmarkSwitcher.FromTypes([
    typeof(MetadataReadBenchmarks),
    typeof(RowGroupReadBenchmarks),
    typeof(DeltaBinaryPackedBenchmarks),
    typeof(DeltaByteArrayBenchmarks),
    typeof(ByteStreamSplitBenchmarks),
    typeof(EncodingReadBenchmarks),
]).Run(args);
