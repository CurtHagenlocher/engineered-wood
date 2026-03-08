namespace EngineeredWood.Avro;

/// <summary>
/// A registry that maps schema fingerprints to <see cref="AvroSchema"/> instances.
/// Used to resolve schemas during deserialization.
/// </summary>
public sealed class SchemaStore
{
    private readonly Dictionary<SchemaFingerprint, AvroSchema> _schemas = new();

    /// <summary>
    /// Creates a new <see cref="SchemaStore"/> using the specified fingerprint algorithm for auto-registration.
    /// </summary>
    public SchemaStore(FingerprintAlgorithm algorithm = FingerprintAlgorithm.Rabin)
    {
        Algorithm = algorithm;
    }

    /// <summary>The fingerprint algorithm used when registering schemas via <see cref="Register"/>.</summary>
    public FingerprintAlgorithm Algorithm { get; }

    /// <summary>
    /// Registers a schema by computing its fingerprint using <see cref="Algorithm"/>.
    /// Returns the computed fingerprint.
    /// </summary>
    public SchemaFingerprint Register(AvroSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var fingerprint = schema.ComputeFingerprint(Algorithm);
        _schemas[fingerprint] = schema;
        return fingerprint;
    }

    /// <summary>
    /// Registers a schema with an explicit fingerprint (e.g., a Confluent Schema Registry ID).
    /// </summary>
    public void Set(SchemaFingerprint fingerprint, AvroSchema schema)
    {
        ArgumentNullException.ThrowIfNull(fingerprint);
        ArgumentNullException.ThrowIfNull(schema);
        _schemas[fingerprint] = schema;
    }

    /// <summary>
    /// Looks up a schema by its fingerprint.
    /// </summary>
    /// <returns>The schema if found; otherwise <c>null</c>.</returns>
    public AvroSchema? Lookup(SchemaFingerprint fingerprint)
    {
        ArgumentNullException.ThrowIfNull(fingerprint);
        return _schemas.GetValueOrDefault(fingerprint);
    }

    /// <summary>
    /// All registered fingerprints.
    /// </summary>
    public IReadOnlyList<SchemaFingerprint> Fingerprints => _schemas.Keys.ToList();
}
