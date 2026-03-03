namespace EngineeredWood.Compatibility;

enum ExpectedOutcome
{
    Pass,
    Skip,
}

record FileEntry(string Url, string LocalName, string Source, ExpectedOutcome Expected, string? SkipReason = null);

static class FileManifest
{
    private const string Fp = "https://raw.githubusercontent.com/dask/fastparquet/main/test-data/";
    private const string Pd = "https://raw.githubusercontent.com/aloneguid/parquet-dotnet/master/src/Parquet.Test/data/";
    private const string PdS = Pd + "special/";
    private const string PdI = Pd + "issues/";
    private const string Pt = "https://raw.githubusercontent.com/hangxie/parquet-tools/main/testdata/";
    private const string Hf = "https://huggingface.co/datasets/";

    public static IReadOnlyList<FileEntry> Files { get; } =
    [
        // ── dask/fastparquet (~18 files) ──────────────────────────────────
        new(Fp + "nation.plain.parquet",         "fp-nation.plain.parquet",         "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "nation.dict.parquet",          "fp-nation.dict.parquet",          "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "nation.impala.parquet",        "fp-nation.impala.parquet",        "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "gzip-nation.impala.parquet",   "fp-gzip-nation.impala.parquet",   "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "snappy-nation.impala.parquet", "fp-snappy-nation.impala.parquet", "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "datapage_v2.snappy.parquet",   "fp-datapage_v2.snappy.parquet",   "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "decimals.parquet",             "fp-decimals.parquet",             "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "map-test.snappy.parquet",      "fp-map-test.snappy.parquet",      "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "nested.parq",                  "fp-nested.parq",                  "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "nested1.parquet",              "fp-nested1.parquet",              "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "repeated_no_annotation.parquet","fp-repeated_no_annotation.parquet","fastparquet", ExpectedOutcome.Pass),
        new(Fp + "test-null.parquet",            "fp-test-null.parquet",            "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "test-null-dictionary.parquet", "fp-test-null-dictionary.parquet", "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "test-converted-type-null.parquet","fp-test-converted-type-null.parquet","fastparquet", ExpectedOutcome.Pass),
        new(Fp + "empty.parquet",                "fp-empty.parquet",                "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "no_columns.parquet",           "fp-no_columns.parquet",           "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "test-timezone.parquet",        "fp-test-timezone.parquet",        "fastparquet", ExpectedOutcome.Pass),
        new(Fp + "foo.parquet",                  "fp-foo.parquet",                  "fastparquet", ExpectedOutcome.Pass),

        // ── aloneguid/parquet-dotnet (~38 files) ──────────────────────────
        new(Pd + "all_var1.parquet",             "pd-all_var1.parquet",             "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "all_var1.v2.parquet",          "pd-all_var1.v2.parquet",          "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "bigdecimal.parquet",           "pd-bigdecimal.parquet",           "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "bss_with_nulls_double.parquet","pd-bss_with_nulls_double.parquet","parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "bss_with_nulls_float.parquet", "pd-bss_with_nulls_float.parquet", "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "byte_array_decimal.parquet",   "pd-byte_array_decimal.parquet",   "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "byte_stream_split_256.parquet","pd-byte_stream_split_256.parquet","parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "complex-primitives.parquet",   "pd-complex-primitives.parquet",   "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "dates.parquet",                "pd-dates.parquet",                "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "datetime_other_system.parquet","pd-datetime_other_system.parquet","parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "delta_binary_packed.parquet",  "pd-delta_binary_packed.parquet",  "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "delta_byte_array.parquet",     "pd-delta_byte_array.parquet",     "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "delta_length_byte_array.parquet","pd-delta_length_byte_array.parquet","parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "emptycolumn.parquet",          "pd-emptycolumn.parquet",          "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "fixed_len_byte_array_with_dict.parquet","pd-fixed_len_byte_array_with_dict.parquet","parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "fixedlenbytearray.parquet",    "pd-fixedlenbytearray.parquet",    "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "list_simple.parquet",          "pd-list_simple.parquet",          "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "list_empty.parquet",           "pd-list_empty.parquet",           "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "list_empty_and_null.parquet",  "pd-list_empty_and_null.parquet",  "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "list_structs.parquet",         "pd-list_structs.parquet",         "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "legacy-list-onearray.parquet", "pd-legacy-list-onearray.parquet", "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "map_simple.parquet",           "pd-map_simple.parquet",           "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "mapinstruct.parquet",          "pd-mapinstruct.parquet",          "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "mixed-dictionary-plain.parquet","pd-mixed-dictionary-plain.parquet","parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "multi.page.parquet",           "pd-multi.page.parquet",           "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "multi.page.v2.parquet",        "pd-multi.page.v2.parquet",        "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "postcodes.plain.parquet",      "pd-postcodes.plain.parquet",      "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "required-strings.parquet",     "pd-required-strings.parquet",     "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "rle_dictionary_encoded_columns.parquet","pd-rle_dictionary_encoded_columns.parquet","parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "simplenested.parquet",         "pd-simplenested.parquet",         "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "struct_plain.parquet",         "pd-struct_plain.parquet",         "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "table.parquet",                "pd-table.parquet",                "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "test-types-with-decimal.parquet","pd-test-types-with-decimal.parquet","parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "timestamp_micros.parquet",     "pd-timestamp_micros.parquet",     "parquet-dotnet", ExpectedOutcome.Pass),
        new(Pd + "641_rle_pagesize_oob.parquet", "pd-641_rle_pagesize_oob.parquet", "parquet-dotnet", ExpectedOutcome.Pass),
        new(PdS + "multi_data_page.parquet",     "pd-multi_data_page.parquet",      "parquet-dotnet", ExpectedOutcome.Pass),
        new(PdS + "multi_page_dictionary_with_nulls.parquet","pd-multi_page_dictionary_with_nulls.parquet","parquet-dotnet", ExpectedOutcome.Pass),
        new(PdS + "all_nulls.parquet",           "pd-all_nulls.parquet",            "parquet-dotnet", ExpectedOutcome.Pass),
        new(PdS + "decimallegacy.parquet",       "pd-decimallegacy.parquet",        "parquet-dotnet", ExpectedOutcome.Pass),
        new(PdS + "decimalnulls.parquet",        "pd-decimalnulls.parquet",         "parquet-dotnet", ExpectedOutcome.Pass),
        new(PdS + "legacy-list.parquet",         "pd-legacy-list.parquet",          "parquet-dotnet", ExpectedOutcome.Pass),
        new(PdS + "wide.parquet",                "pd-wide.parquet",                 "parquet-dotnet", ExpectedOutcome.Pass),
        new(PdI + "637-duckdb.parquet",          "pd-637-duckdb.parquet",           "parquet-dotnet", ExpectedOutcome.Pass),

        // ── hangxie/parquet-tools (~25 files) ─────────────────────────────
        new(Pt + "all-types.parquet",            "pt-all-types.parquet",            "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "good.parquet",                 "pt-good.parquet",                 "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "good-snappy.parquet",          "pt-good-snappy.parquet",          "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "nan.parquet",                  "pt-nan.parquet",                  "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "data-page-v2.parquet",         "pt-data-page-v2.parquet",         "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "dict-page.parquet",            "pt-dict-page.parquet",            "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "bloom-filter.parquet",         "pt-bloom-filter.parquet",         "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "nil-statistics.parquet",       "pt-nil-statistics.parquet",       "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "int96-nil-min-max.parquet",    "pt-int96-nil-min-max.parquet",    "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "row-group.parquet",            "pt-row-group.parquet",            "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "empty.parquet",                "pt-empty.parquet",                "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "optional-fields.parquet",      "pt-optional-fields.parquet",      "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "csv-nested.parquet",           "pt-csv-nested.parquet",           "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "gostruct-list.parquet",        "pt-gostruct-list.parquet",        "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "old-style-list.parquet",       "pt-old-style-list.parquet",       "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "csv-repeated.parquet",         "pt-csv-repeated.parquet",         "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "map-composite-value.parquet",  "pt-map-composite-value.parquet",  "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "geospatial.parquet",           "pt-geospatial.parquet",           "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "csv-good.parquet",             "pt-csv-good.parquet",             "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "csv-optional.parquet",         "pt-csv-optional.parquet",         "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "pargo-prefix-flat.parquet",    "pt-pargo-prefix-flat.parquet",    "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "pargo-prefix-nested.parquet",  "pt-pargo-prefix-nested.parquet",  "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "ARROW-GH-41317.parquet",       "pt-ARROW-GH-41317.parquet",      "parquet-tools", ExpectedOutcome.Skip, "corrupted PageType byte in timestamp_us_no_tz column (fuzzer artifact)"),
        new(Pt + "ARROW-GH-41321.parquet",       "pt-ARROW-GH-41321.parquet",      "parquet-tools", ExpectedOutcome.Skip, "Arrow issue file — truncated RLE data"),
        new(Pt + "PARQUET-1481.parquet",         "pt-PARQUET-1481.parquet",         "parquet-tools", ExpectedOutcome.Skip, "uses unknown physical type"),

        // Deeply nested (maxRepLevel > 1) — nesting is now supported
        new(Pt + "list-of-list.parquet",         "pt-list-of-list.parquet",         "parquet-tools", ExpectedOutcome.Pass),
        new(Pt + "map-value-map.parquet",        "pt-map-value-map.parquet",        "parquet-tools", ExpectedOutcome.Pass),

        // UNKNOWN logical type (NullType) — supported
        new(Pt + "unknown-type.parquet",         "pt-unknown-type.parquet",         "parquet-tools", ExpectedOutcome.Pass),

        // ── Hugging Face (3 files) ────────────────────────────────────────
        new(Hf + "scikit-learn/iris/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet",
            "hf-iris.parquet",                   "huggingface",  ExpectedOutcome.Pass),
        new(Hf + "nyu-mll/glue/resolve/refs%2Fconvert%2Fparquet/stsb/validation/0000.parquet",
            "hf-glue-stsb.parquet",              "huggingface",  ExpectedOutcome.Pass),
        new(Hf + "cais/mmlu/resolve/refs%2Fconvert%2Fparquet/abstract_algebra/test/0000.parquet",
            "hf-mmlu-abstract-algebra.parquet",  "huggingface",  ExpectedOutcome.Pass),
    ];
}
