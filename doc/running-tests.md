# Running Tests

## Prerequisites

### .NET SDK

The solution requires **.NET 10 SDK** (or later). The test projects multi-target:

- `net10.0` — primary target
- `net8.0` — LTS target
- `net472` — .NET Framework (Windows only)

### Git Submodules

The Parquet test suite reads files from the `parquet-testing` submodule:

```
git submodule update --init
```

### Python (optional — for cross-validation tests)

Some ORC and Avro tests validate interoperability by invoking Python
libraries as subprocesses. These tests are **skipped** automatically if the
required Python packages are not installed — they will appear as "Skipped"
in the test output.

To enable them, install Python 3.8+ and the following packages:

```
pip install pyarrow fastavro tzdata
```

The `tzdata` package is required on Windows — PyArrow's Arrow C++ ORC
reader needs IANA timezone data that isn't available natively on Windows.
The test harness automatically detects the `tzdata` package and sets
the `TZDIR` environment variable for the Python subprocess. If you still
see timezone-related errors, you can set `TZDIR` manually:

```
# PowerShell (session)
$env:TZDIR = "$(python -c "import os, tzdata; print(os.path.join(os.path.dirname(tzdata.__file__), 'zoneinfo'))")"

# Or set permanently via System Properties → Environment Variables
# Value: C:\Users\<you>\AppData\Local\Programs\Python\Python3XX\Lib\site-packages\tzdata\zoneinfo
```

**Python discovery:** The tests try `python3` and `python` on PATH,
then fall back to scanning `%LOCALAPPDATA%\Programs\Python\Python*\`.
On Windows, you may need to disable the Microsoft Store "python.exe"
app execution alias (Settings → Apps → Advanced app settings → App
execution aliases → turn off "python.exe" and "python3.exe").

| Test suite | Python package | Tests enabled | What they validate |
|---|---|---|---|
| ORC | `pyarrow` | 10 cross-validation tests | EngineeredWood writes → PyArrow reads |
| Avro | `fastavro` | 7 cross-validation tests | EngineeredWood writes → fastavro reads |
| Parquet | *(none — uses ParquetSharp NuGet)* | all tests always run | Bidirectional with ParquetSharp |

### Regenerating Avro Test Data

The Avro test suite includes pre-generated `.avro` files in
`test/EngineeredWood.Avro.Tests/TestData/`. To regenerate them:

```
cd test/EngineeredWood.Avro.Tests/TestData
python generate_test_data.py
```

This requires `fastavro` to be installed.

## Running Tests

### All tests (all targets)

```
dotnet test
```

Or per project:

```
dotnet test test/EngineeredWood.Parquet.Tests
dotnet test test/EngineeredWood.Orc.Tests
dotnet test test/EngineeredWood.Avro.Tests
```

### Single target framework

```
dotnet test --framework net10.0
dotnet test --framework net8.0
dotnet test --framework net472
```

### Filtered

```
dotnet test --filter "FullyQualifiedName~CrossValidat"
dotnet test --filter "FullyQualifiedName~BatchedRead"
```

## Understanding Test Output

### Skipped tests

Tests that depend on optional Python libraries show as "Skipped" with a
reason:

```
Skipped EngineeredWood.Orc.Tests.CrossValidationTests.CrossValidate_Integers [1 ms]
...
Passed!  - Failed: 0, Passed: 194, Skipped: 10, Total: 204
```

If you see `Skipped: 0` for ORC/Avro, the Python tests **are running**
(they passed). If you see `Skipped: 10` (ORC) or `Skipped: 7` (Avro),
the Python packages are not installed.

### Expected test counts

| Suite | Total | Always run | Python-dependent |
|---|---|---|---|
| **Parquet** | 481 | 481 | 0 |
| **ORC** | 204 | 194 | 10 (PyArrow) |
| **Avro** | 272 | 265 | 7 (fastavro) |

## Parquet Compatibility Tool

A separate CLI tool validates the Parquet reader against a corpus of
real-world files from multiple implementations:

```
dotnet run --project test/EngineeredWood.Parquet.Compatibility
```

This downloads ~138 Parquet files on first run (cached in a temp directory)
and validates that the reader can parse metadata, decompress, and decode
each file. It does not require Python or any external tools.

## Benchmarks

```
dotnet run -c Release --project test/EngineeredWood.Parquet.Benchmarks -- --filter "*RowGroupRead*"
dotnet run -c Release --project test/EngineeredWood.Orc.Benchmarks
dotnet run -c Release --project test/EngineeredWood.Avro.Benchmarks
```

Add `--framework net472` to benchmark on .NET Framework.

The Parquet benchmarks also include a cloud benchmark for Azure Blob Storage:

```
dotnet run -c Release --project test/EngineeredWood.Parquet.Benchmarks -- cloud
```

This prompts interactively for an Azure Blob URL and account key.
