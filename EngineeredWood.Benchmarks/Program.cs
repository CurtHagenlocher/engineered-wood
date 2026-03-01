using BenchmarkDotNet.Running;
using EngineeredWood.Benchmarks;

BenchmarkSwitcher.FromTypes([
    typeof(MetadataReadBenchmarks),
    typeof(RowGroupReadBenchmarks),
    typeof(DeltaBinaryPackedBenchmarks),
]).Run(args);
