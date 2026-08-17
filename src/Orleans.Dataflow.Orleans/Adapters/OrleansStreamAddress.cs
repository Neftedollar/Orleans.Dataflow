using System.Globalization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// Which Orleans stream an adapter attaches to: a provider by name and one stream identity within it.
/// </summary>
/// <remarks>
/// <para>
/// Three strings and nothing else, because that is exactly what a document can carry honestly. The provider
/// name is a deployment's own registration name — the value passed to <c>AddMemoryStreams</c> or to any
/// other provider registration — and the namespace and key are the two halves of an Orleans
/// <see cref="Orleans.Runtime.StreamId"/>. Nothing here names a CLR type: what the stream carries is the
/// element declaration the adapter's payload names, and which CLR type carries that contract in a given
/// silo is that silo's registration to make.
/// </para>
/// <para>
/// <b>Guid keys.</b> Orleans builds one identity from a <see cref="Guid"/> and from that guid's
/// thirty-two-digit unpadded text, which was probed rather than assumed: <c>StreamId.Create(ns, guid)</c>
/// and <c>StreamId.Create(ns, guid.ToString("N"))</c> compare equal. So a guid-keyed address is the string
/// address of that text, one representation reaches a document, and a deployment that writes guids and a
/// deployment that writes strings address the same streams.
/// </para>
/// <para>
/// The type is a readonly record struct because equality over the three strings is its whole contract, and
/// the default instance addresses nothing and says so.
/// </para>
/// </remarks>
public readonly record struct OrleansStreamAddress
{
    /// <summary>The diagnostic text <see cref="ToString"/> renders for the default value.</summary>
    private const string DefaultText = "(default OrleansStreamAddress)";

    private readonly string? _provider;
    private readonly string? _namespace;
    private readonly string? _key;

    /// <summary>Initializes a new instance of the <see cref="OrleansStreamAddress"/> struct.</summary>
    /// <param name="provider">The validated provider name.</param>
    /// <param name="streamNamespace">The validated stream namespace.</param>
    /// <param name="key">The validated stream key.</param>
    private OrleansStreamAddress(string provider, string streamNamespace, string key)
    {
        _provider = provider;
        _namespace = streamNamespace;
        _key = key;
    }

    /// <summary>Gets the name the deployment registered the stream provider under.</summary>
    /// <value>A non-empty name.</value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    public string Provider => _provider ?? throw DefaultAccess();

    /// <summary>Gets the stream namespace.</summary>
    /// <value>A non-empty namespace.</value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    public string Namespace => _namespace ?? throw DefaultAccess();

    /// <summary>Gets the stream key within its namespace.</summary>
    /// <value>A non-empty key.</value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    public string Key => _key ?? throw DefaultAccess();

    /// <summary>Gets a value indicating whether this instance is the uninitialized default.</summary>
    /// <value><see langword="true"/> when the instance addresses no stream.</value>
    public bool IsDefault => _provider is null;

    /// <summary>Addresses one stream by provider name, namespace, and string key.</summary>
    /// <param name="provider">The provider's registration name.</param>
    /// <param name="streamNamespace">The stream namespace.</param>
    /// <param name="key">The stream key.</param>
    /// <returns>The address.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">Any argument is empty or white space.</exception>
    /// <remarks>
    /// The three parts are checked for emptiness and for nothing else. Orleans imposes no grammar on a
    /// stream namespace or key beyond their being text, and inventing one here would refuse addresses a
    /// working deployment already uses.
    /// </remarks>
    public static OrleansStreamAddress Create(string provider, string streamNamespace, string key)
    {
        Require(provider, nameof(provider));
        Require(streamNamespace, nameof(streamNamespace));
        Require(key, nameof(key));

        return new OrleansStreamAddress(provider, streamNamespace, key);
    }

    /// <summary>Addresses one stream by provider name, namespace, and guid key.</summary>
    /// <param name="provider">The provider's registration name.</param>
    /// <param name="streamNamespace">The stream namespace.</param>
    /// <param name="key">The stream key.</param>
    /// <returns>The address, whose <see cref="Key"/> is the guid's thirty-two-digit text.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="provider"/> or <paramref name="streamNamespace"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="provider"/> or <paramref name="streamNamespace"/> is empty or white space.
    /// </exception>
    /// <remarks>
    /// The guid is rendered with the invariant culture and the <c>N</c> format, which is the representation
    /// Orleans itself builds a stream identity from: the two spellings were probed and produce equal
    /// identities, so a guid-keyed address and the string address of the same text address one stream.
    /// </remarks>
    public static OrleansStreamAddress Create(string provider, string streamNamespace, Guid key) =>
        Create(provider, streamNamespace, key.ToString("N", CultureInfo.InvariantCulture));

    /// <summary>Returns a one-line diagnostic summary of this address.</summary>
    /// <returns>
    /// Text of the form <c>memory-streams/orders/17</c>, or <c>"(default OrleansStreamAddress)"</c> for the
    /// default value.
    /// </returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() =>
        IsDefault ? DefaultText : $"{_provider}/{_namespace}/{_key}";

    /// <summary>Refuses a part that is null, empty, or white space.</summary>
    /// <param name="value">The part.</param>
    /// <param name="parameter">The parameter name to report it under.</param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="value"/> is empty or white space.</exception>
    private static void Require(string value, string parameter)
    {
        ArgumentNullException.ThrowIfNull(value, parameter);

        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException(
                "A stream address names a provider, a namespace, and a key, and none of the three is empty.",
                parameter);
        }
    }

    /// <summary>Builds the exception for reading a part of the default instance.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException DefaultAccess() =>
        new($"The default {nameof(OrleansStreamAddress)} addresses no stream. Build one with {nameof(OrleansStreamAddress)}.{nameof(Create)}.");
}
