# Encryption Support Design Document

**Status:** Not yet implemented. No encryption code exists in
`EngineeredWood.Parquet` or `EngineeredWood.Orc` as of this writing — this
document is forward-looking design only. Encrypted Parquet test files are
currently skipped by the test sweeps.

## Overview

Both Parquet and ORC support column-level encryption, but with fundamentally different designs. This document captures the spec details and open design decisions for implementing encryption in EngineeredWood.

## Parquet Modular Encryption (PARQUET-1375)

### Two Footer Modes

| Mode | Magic | Privacy | Compat | Use Case |
|------|-------|---------|--------|----------|
| **Encrypted footer** | `PARE` | Schema, row counts, column names all hidden | Breaks legacy readers | Maximum confidentiality |
| **Plaintext footer** | `PAR1` | Schema visible, stats encrypted separately, footer signed via GCM | Legacy readers can open unencrypted columns | Backward compatibility |

**Decision needed:** Support both modes, or start with plaintext footer only?

### Per-Column Encryption

Each column can independently be:
- Encrypted with the **footer key** (simplest)
- Encrypted with a **column-specific key** (fine-grained access control)
- Left **unencrypted** (non-sensitive data)

Column metadata (statistics) is encrypted separately when using column-specific keys.

### Cipher Modes

| Mode | Pages | Headers/Metadata | Overhead | Auth |
|------|-------|-----------------|----------|------|
| **AES_GCM_V1** | AES-GCM (authenticated) | AES-GCM (authenticated) | ~15% | Full |
| **AES_GCM_CTR_V1** | AES-CTR (fast, no auth) | AES-GCM (authenticated) | ~4-5% | Metadata only |

**Decision needed:** Support both? The hybrid mode (GCM_CTR) is more practical for large datasets. Both use 128-bit or 256-bit keys.

### AAD (Additional Authenticated Data)

AAD prevents module-swapping attacks by binding each encrypted block to its identity:

```
AAD = user_prefix || module_type || row_group_ordinal || column_ordinal || page_ordinal
```

This ensures a page from one position can't be replayed at another position.

### What Gets Encrypted

1. Data pages and dictionary pages (values)
2. Page headers (compression, encoding, statistics)
3. Column indexes and offset indexes
4. Bloom filters (headers + bitsets)
5. ColumnMetaData (when using column-specific keys)
6. FileMetaData (encrypted footer mode only)

### Key Management

The spec is deliberately KMS-agnostic. Keys are referenced via an opaque `key_metadata` binary field. The caller provides keys; the file format doesn't dictate how they're stored.

**Pattern:** Envelope encryption (DEK/MEK):
- A random Data Encryption Key (DEK) encrypts the data
- The DEK is encrypted by a Master Encryption Key (MEK) from the KMS
- The encrypted DEK is stored in the file metadata

### Implementation Scope Options

| Phase | Scope | Effort |
|-------|-------|--------|
| **Phase 1** | Read encrypted files (uniform key, plaintext footer) | Medium |
| **Phase 2** | Write encrypted files (uniform key, plaintext footer) | Medium |
| **Phase 3** | Per-column keys (read + write) | Medium |
| **Phase 4** | Encrypted footer mode | Small |
| **Phase 5** | Key Management Tools API (envelope encryption, KMS integration) | Large |

---

## ORC Encryption

### Design Philosophy

ORC prioritizes performance and seeking over integrity verification. All encryption uses AES-CTR (no authentication tags).

### Dual-Variant Storage

Unique to ORC: each encrypted column stores **two versions**:
- **Encrypted unmasked**: Original data, encrypted — for authorized users
- **Unencrypted masked**: Sanitized data, plaintext — for unauthorized users

This means readers without keys still get meaningful (but redacted) data rather than errors.

### Masking Options (Applied Before Encryption)

| Mask | Effect | Example |
|------|--------|---------|
| **Nullify** | All values → null | `"John"` → `null` |
| **Redact** | Replace chars with constants | `"John"` → `"XXXX"` |
| **SHA256** | One-way hash | `"John"` → `"a8cfcd..."` |

### Cipher: AES-CTR Only

- No authentication tags (no GCM) — tampering is undetectable
- Deterministic IVs from `(column_id, stream_kind, stripe_id, block_counter)`
- Random local key per column per file
- Local keys encrypted by KMS, stored in StripeInformation
- <2% performance overhead

### What Gets Encrypted

1. Data streams (column values)
2. Index streams (row group statistics)
3. File and stripe statistics
4. Local keys themselves (encrypted by KMS)

### IV Construction (16 bytes)

```
Bytes 0-2:   Column ID
Bytes 3-4:   Stream kind (DATA, INDEX, etc.)
Bytes 5-7:   Stripe ID
Bytes 8-15:  Block counter
```

### Implementation Scope Options

| Phase | Scope | Effort |
|-------|-------|--------|
| **Phase 1** | Read encrypted columns (skip if no key) | Medium |
| **Phase 2** | Write encrypted columns (single key) | Medium |
| **Phase 3** | Per-column keys + masking | Large |
| **Phase 4** | KMS integration | Large |

---

## Key Differences: Parquet vs ORC

| Aspect | Parquet | ORC |
|--------|---------|-----|
| **Cipher** | AES-GCM and/or AES-CTR | AES-CTR only |
| **Integrity** | Optional auth tags (GCM) | None |
| **No-key behavior** | Error (or read unencrypted columns) | Read masked/redacted data |
| **Performance** | 4-15% overhead | <2% overhead |
| **IV** | Random (GCM) | Deterministic from metadata |
| **Masking** | Not built-in | Built-in (nullify/redact/sha256) |

## Cross-Cutting Design Decisions

### 1. Crypto Provider

.NET has built-in AES-GCM (`System.Security.Cryptography.AesGcm`) since .NET 5 and AES in CTR mode can be built from `Aes.CreateEncryptor` with manual counter management. No external crypto library needed.

**However:** `AesGcm` is not available on netstandard2.0. Options:
- Drop netstandard2.0 for the encryption module (separate project, net8.0+ only)
- Use a managed AES-GCM implementation for netstandard2.0 (e.g., BouncyCastle)
- Encryption as a separate NuGet package with higher TFM requirement

**Recommendation:** Separate `EngineeredWood.Parquet.Encryption` project targeting net8.0+ only, keeping the core library compatible with netstandard2.0.

### 2. Key Provider Interface

```csharp
public interface IKeyProvider
{
    /// <summary>
    /// Retrieves the decryption key for the given key metadata.
    /// </summary>
    byte[] GetKey(byte[] keyMetadata);

    /// <summary>
    /// Retrieves the decryption key for a named column.
    /// Returns null if no key is available (column should be skipped or read as masked).
    /// </summary>
    byte[]? GetColumnKey(string columnPath, byte[]? keyMetadata);
}
```

### 3. Encryption Options (Write)

```csharp
public class ParquetEncryptionOptions
{
    /// <summary>Footer encryption mode.</summary>
    public FooterEncryptionMode FooterMode { get; init; } = FooterEncryptionMode.PlaintextSigned;

    /// <summary>Cipher algorithm.</summary>
    public ParquetCipher Cipher { get; init; } = ParquetCipher.AesGcmCtrV1;

    /// <summary>Footer encryption key (required).</summary>
    public required byte[] FooterKey { get; init; }

    /// <summary>Per-column key overrides. Columns not listed use the footer key.</summary>
    public IReadOnlyDictionary<string, byte[]>? ColumnKeys { get; init; }

    /// <summary>AAD prefix for anti-replay protection.</summary>
    public string? AadPrefix { get; init; }
}
```

### 4. Testing Strategy

- Round-trip tests with our own writer/reader
- Cross-validation with ParquetSharp (which supports encryption)
- Test files from parquet-testing submodule (encrypted test files exist)
- For ORC: cross-validation with PyArrow if it supports ORC encryption

### 5. Phasing Recommendation

**Start with Parquet read-only, plaintext footer mode, uniform key.** This gives us the ability to open encrypted files that others produce, with minimum complexity. The existing `parquet-testing` submodule has encrypted test files we can validate against.
