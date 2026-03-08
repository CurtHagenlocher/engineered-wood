"""
Generates Avro test data files using fastavro for cross-validation with EngineeredWood.Avro.
Run: python generate_test_data.py
"""
import os
import fastavro

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

def write_avro(filename, schema, records, codec='null'):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, 'wb') as f:
        fastavro.writer(f, schema, records, codec=codec)
    print(f"  Written: {filename} ({len(records)} records, codec={codec})")

# --- Primitives ---
primitives_schema = {
    "type": "record",
    "name": "Primitives",
    "fields": [
        {"name": "bool_col", "type": "boolean"},
        {"name": "int_col", "type": "int"},
        {"name": "long_col", "type": "long"},
        {"name": "float_col", "type": "float"},
        {"name": "double_col", "type": "double"},
        {"name": "string_col", "type": "string"},
        {"name": "bytes_col", "type": "bytes"},
    ]
}

primitives_records = []
for i in range(100):
    primitives_records.append({
        "bool_col": i % 2 == 0,
        "int_col": i * 7 - 50,
        "long_col": i * 100000 - 500000,
        "float_col": float(i) * 0.5,
        "double_col": float(i) * 1.23456789,
        "string_col": f"row_{i}",
        "bytes_col": bytes([i % 256, (i * 3) % 256]),
    })

print("Generating primitive type files...")
write_avro("primitives_null.avro", primitives_schema, primitives_records, codec='null')
write_avro("primitives_deflate.avro", primitives_schema, primitives_records, codec='deflate')
write_avro("primitives_snappy.avro", primitives_schema, primitives_records, codec='snappy')

# --- Nullable fields ---
nullable_schema = {
    "type": "record",
    "name": "Nullable",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "nullable_int", "type": ["null", "int"]},
        {"name": "nullable_string", "type": ["null", "string"]},
    ]
}

nullable_records = []
for i in range(50):
    nullable_records.append({
        "id": i,
        "nullable_int": None if i % 3 == 0 else i * 10,
        "nullable_string": None if i % 5 == 0 else f"value_{i}",
    })

print("Generating nullable field files...")
write_avro("nullable_null.avro", nullable_schema, nullable_records, codec='null')

# --- Edge cases ---
edge_schema = {
    "type": "record",
    "name": "EdgeCases",
    "fields": [
        {"name": "int_col", "type": "int"},
        {"name": "long_col", "type": "long"},
        {"name": "string_col", "type": "string"},
    ]
}

edge_records = [
    {"int_col": 0, "long_col": 0, "string_col": ""},
    {"int_col": 2147483647, "long_col": 9223372036854775807, "string_col": "max"},
    {"int_col": -2147483648, "long_col": -9223372036854775808, "string_col": "min"},
    {"int_col": 1, "long_col": -1, "string_col": "hello 🌍"},
]

print("Generating edge case files...")
write_avro("edge_cases.avro", edge_schema, edge_records, codec='null')

# --- Empty file ---
empty_schema = {
    "type": "record",
    "name": "Empty",
    "fields": [
        {"name": "x", "type": "int"},
    ]
}

print("Generating empty file...")
write_avro("empty.avro", empty_schema, [], codec='null')

print("Done!")
