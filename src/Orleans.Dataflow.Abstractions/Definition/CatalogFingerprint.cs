using System.Globalization;
using System.Security.Cryptography;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// The SHA-256 identity of one canonical stage catalog byte form.
/// </summary>
/// <remarks>
/// <para>
/// A catalog has exactly one canonical byte form, so the digest of those bytes is an identity rather than
/// merely a checksum: two catalogs share a fingerprint when and only when they register the same declared
/// shapes, and a fingerprint computed on one silo is the same number computed anywhere else. Two silos
/// can therefore agree that they are running the same catalog by exchanging 32 bytes instead of the
/// catalog.
/// </para>
/// <para>
/// This is deliberately not a <see cref="GraphFingerprint"/>, though the two are computed the same way.
/// They identify different things: a graph fingerprint answers "is this the same document", a catalog
/// fingerprint answers "is this the same set of registered stages". A run is reproducible only when both
/// agree, and a single type would let one be passed where the other is meant and make that confusion
/// compile. Two identity domains, two types, and no conversion between them.
/// </para>
/// <para>
/// The fingerprint covers the declared shape only. A parameter validator is behavior, is never
/// serialized, and therefore never changes this value: two catalogs whose specifications agree but whose
/// validators differ share a fingerprint. Validator behavior is a deployment concern, and this limit is
/// stated rather than hidden.
/// </para>
/// <para>
/// The text form is <c>sha256:</c> followed by 64 lowercase hexadecimal digits. Parsing accepts exactly
/// that form: an uppercase digit, a missing prefix, or any other length is rejected rather than
/// normalized, because a fingerprint that two spellings could produce would stop being an identity.
/// </para>
/// <para>
/// The default value carries no digest: <see cref="IsDefault"/> reports it, <see cref="Hash"/> throws for
/// it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct CatalogFingerprint
{
    /// <summary>The prefix that names the digest algorithm in the text form.</summary>
    private const string AlgorithmPrefix = "sha256:";

    /// <summary>The size, in bytes, of a SHA-256 digest.</summary>
    private const int HashByteCount = 32;

    /// <summary>The number of hexadecimal digits that render <see cref="HashByteCount"/> bytes.</summary>
    private const int HashDigitCount = HashByteCount * 2;

    /// <summary>The diagnostic text <see cref="ToString"/> renders for the default value.</summary>
    private const string DefaultText = "(default CatalogFingerprint)";

    private readonly byte[]? _hash;

    /// <summary>
    /// Initializes a new instance of the <see cref="CatalogFingerprint"/> struct.
    /// </summary>
    /// <param name="hash">
    /// The digest bytes, exactly <see cref="HashByteCount"/> of them, in an array this value owns and
    /// never hands to anyone who could mutate it.
    /// </param>
    private CatalogFingerprint(byte[] hash) => _hash = hash;

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    public bool IsDefault => _hash is null;

    /// <summary>
    /// Gets the digest bytes.
    /// </summary>
    /// <value>Exactly 32 bytes, most significant byte first, as SHA-256 produces them.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no digest.
    /// </exception>
    /// <remarks>
    /// The memory is a read-only view over the value's own storage, so reading it allocates nothing and
    /// no caller can reach the underlying array to change it.
    /// </remarks>
    public ReadOnlyMemory<byte> Hash => HashBytes;

    /// <summary>Gets the digest bytes, or throws for the default value.</summary>
    private byte[] HashBytes =>
        _hash ?? throw new InvalidOperationException(
            IdentifierGrammar.DescribeDefaultAccess(nameof(CatalogFingerprint)));

    /// <summary>
    /// Computes the fingerprint of a serialized stage catalog.
    /// </summary>
    /// <param name="canonicalEnvelope">The serialized catalog bytes.</param>
    /// <returns>The SHA-256 digest of <paramref name="canonicalEnvelope"/>.</returns>
    /// <remarks>
    /// The bytes are hashed as given and are deliberately not validated here. A fingerprint is a function
    /// of bytes; making it a partial function would only mean a second way to say a rejection that the
    /// serializer already owns.
    /// </remarks>
    public static CatalogFingerprint OfSerialized(ReadOnlySpan<byte> canonicalEnvelope) =>
        new(SHA256.HashData(canonicalEnvelope));

    /// <summary>
    /// Parses the canonical text form of a fingerprint.
    /// </summary>
    /// <param name="text">Text of the form <c>sha256:</c> followed by 64 lowercase hexadecimal digits.</param>
    /// <returns>The parsed fingerprint.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="text"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="text"/> is not the canonical text form. The message names the rule it breaks.
    /// </exception>
    public static CatalogFingerprint Parse(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        string? violation = DescribeTextViolation(text);

        if (violation is not null)
        {
            throw new ArgumentException(FormatTextError(text, violation), nameof(text));
        }

        return new CatalogFingerprint(Convert.FromHexString(text.AsSpan(AlgorithmPrefix.Length)));
    }

    /// <summary>
    /// Attempts to parse the canonical text form of a fingerprint.
    /// </summary>
    /// <param name="text">The candidate text, which may be <see langword="null"/>.</param>
    /// <param name="fingerprint">
    /// When this method returns <see langword="true"/>, the parsed fingerprint; otherwise the default
    /// value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="text"/> is the canonical text form; otherwise
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>This method never throws, including for a <see langword="null"/> input.</remarks>
    public static bool TryParse(string? text, out CatalogFingerprint fingerprint)
    {
        if (text is not null && DescribeTextViolation(text) is null)
        {
            fingerprint = new CatalogFingerprint(Convert.FromHexString(text.AsSpan(AlgorithmPrefix.Length)));
            return true;
        }

        fingerprint = default;
        return false;
    }

    /// <summary>
    /// Determines whether this fingerprint has the same digest bytes as <paramref name="other"/>.
    /// </summary>
    /// <param name="other">The fingerprint to compare with.</param>
    /// <returns><see langword="true"/> when the digests are equal; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Comparison is over the byte content, not the array reference, which is what makes a fingerprint
    /// computed here equal to the same fingerprint parsed from text. The default value equals only the
    /// default value.
    /// </remarks>
    public bool Equals(CatalogFingerprint other)
    {
        if (ReferenceEquals(_hash, other._hash))
        {
            return true;
        }

        return _hash is not null &&
            other._hash is not null &&
            _hash.AsSpan().SequenceEqual(other._hash);
    }

    /// <summary>
    /// Returns a hash code over the digest bytes.
    /// </summary>
    /// <returns>A hash code consistent with <see cref="Equals(CatalogFingerprint)"/>.</returns>
    /// <remarks>
    /// This is a hash-table hash, not the fingerprint itself: <see cref="HashCode"/> is seeded per
    /// process, so the same fingerprint produces a different number in a different process. The durable
    /// identity is <see cref="Hash"/>, never this value.
    /// </remarks>
    public override int GetHashCode()
    {
        if (_hash is null)
        {
            return 0;
        }

        HashCode hash = default;
        hash.AddBytes(_hash);
        return hash.ToHashCode();
    }

    /// <summary>
    /// Returns the canonical text form, or a diagnostic literal when this instance is the default value.
    /// </summary>
    /// <returns>
    /// Text of the form <c>sha256:9f86d081...</c>, or <c>"(default CatalogFingerprint)"</c> when
    /// <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// The hexadecimal digits are lowercase, which is the only spelling <see cref="Parse"/> accepts, so
    /// the text form round-trips. This method never throws, so logging and debugger display stay safe for
    /// every instance including the default one.
    /// </remarks>
    public override string ToString() =>
        _hash is null ? DefaultText : AlgorithmPrefix + Convert.ToHexStringLower(_hash);

    /// <summary>
    /// Describes the first rule <paramref name="text"/> breaks as a text-form fingerprint.
    /// </summary>
    /// <param name="text">The candidate text.</param>
    /// <returns>A lower-case sentence fragment, or <see langword="null"/> when the text is canonical.</returns>
    /// <remarks>
    /// Hexadecimal digits are classified explicitly rather than through
    /// <see cref="Convert.FromHexString(string)"/>, which also accepts uppercase digits. Two spellings of
    /// one digest would defeat the point of a fingerprint, so the uppercase form is a rejection with its
    /// own diagnostic rather than an accepted alias.
    /// </remarks>
    private static string? DescribeTextViolation(string text)
    {
        if (!text.StartsWith(AlgorithmPrefix, StringComparison.Ordinal))
        {
            return $"it does not start with the algorithm prefix '{AlgorithmPrefix}'";
        }

        ReadOnlySpan<char> digits = text.AsSpan(AlgorithmPrefix.Length);

        if (digits.Length != HashDigitCount)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"it carries {digits.Length} digits after the prefix rather than {HashDigitCount}");
        }

        for (int index = 0; index < digits.Length; index++)
        {
            char digit = digits[index];

            if (digit is (< '0' or > '9') and (< 'a' or > 'f'))
            {
                return string.Create(
                    CultureInfo.InvariantCulture,
                    $"the character '{digit}' at index {index} is not a lowercase hexadecimal digit");
            }
        }

        return null;
    }

    /// <summary>
    /// Builds the message for rejected fingerprint text.
    /// </summary>
    /// <param name="text">The rejected text, quoted into the message.</param>
    /// <param name="violation">The violated rule, as returned by <see cref="DescribeTextViolation"/>.</param>
    /// <returns>A message naming the offending value and the rule it breaks.</returns>
    private static string FormatTextError(string text, string violation) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"'{text}' is not a valid {nameof(CatalogFingerprint)}: {violation}. The text form is '{AlgorithmPrefix}' followed by {HashDigitCount} lowercase hexadecimal digits.");
}
