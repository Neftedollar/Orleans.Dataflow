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
    /// <exception cref="ArgumentException">
    /// Any argument is empty, is white space, or is not well-formed Unicode text. The message names which
    /// of the three parts was wrong.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The three parts are checked for emptiness and for being well-formed text, and for nothing else.
    /// Orleans imposes no grammar on a stream namespace or key beyond their being text, and inventing one
    /// here would refuse addresses a working deployment already uses.
    /// </para>
    /// <para>
    /// <b>"Empty" is .NET's own reading of white space, and that reading is wider than a space bar.</b>
    /// <see cref="string.IsNullOrWhiteSpace(string)"/> classifies the Unicode separators <c>U+2028</c> and
    /// <c>U+2029</c> as white space along with tabs and newlines, so a part made only of them is refused
    /// here as an empty one. That is the intended answer and not a side effect worth removing: an address
    /// whose key is invisible is an address nobody can read back out of a log line, a directory listing, or
    /// a failure message, and the three parts of this value exist to be read. What is deliberately not
    /// refused is a part that merely <em>contains</em> such a character beside visible text, because that is
    /// a key with an odd character in it rather than a key that is not there.
    /// </para>
    /// <para>
    /// <b>Well-formedness is not a grammar; it is the condition under which the address means one thing.</b>
    /// A string carrying an unpaired surrogate has no exact UTF-8 form, and the JSON writer that puts this
    /// address into a graph document substitutes <c>U+FFFD</c> for each one. Two distinct ill-formed keys
    /// therefore used to collapse to the same payload bytes — two keys aliasing one stream — and the
    /// document written named a key that is not the string the caller is holding. Refusing before the run
    /// exists is this library's standing answer to that shape of problem, and it is why the check is here
    /// and not at the writer, which can no longer tell which of three arguments was at fault.
    /// </para>
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

    /// <summary>Refuses a part that is null, empty, white space, or not well-formed text.</summary>
    /// <param name="value">The part.</param>
    /// <param name="parameter">The parameter name to report it under.</param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="value"/> is empty, is white space, or carries an unpaired surrogate.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The well-formedness scanner is the core package's, shared with the JSON string writer that would
    /// otherwise have substituted for the same characters. One implementation of "is this text" is what
    /// keeps the refusal here and the writing there from ever disagreeing about which strings are whole.
    /// </para>
    /// <para>
    /// The emptiness test is <see cref="string.IsNullOrWhiteSpace(string)"/> as it stands, separators
    /// <c>U+2028</c> and <c>U+2029</c> included. Narrowing it to the space bar was considered and refused:
    /// the platform's definition is the one every other .NET reader of these strings will apply, and a part
    /// this method admitted that every log and every listing then rendered as nothing would be a name only
    /// this library believes in.
    /// </para>
    /// </remarks>
    private static void Require(string value, string parameter)
    {
        ArgumentNullException.ThrowIfNull(value, parameter);

        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException(
                "A stream address names a provider, a namespace, and a key, and none of the three is empty.",
                parameter);
        }

        if (!JsonText.IsWellFormed(value))
        {
            throw new ArgumentException(
                $"The {parameter} of a stream address carries an unpaired surrogate, so it is not well-formed text and has no exact form on the wire. A document written from it would substitute the replacement character for that character, which would let two different addresses name one stream and would name a key that is not the one given here.",
                parameter);
        }
    }

    /// <summary>Builds the exception for reading a part of the default instance.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException DefaultAccess() =>
        new($"The default {nameof(OrleansStreamAddress)} addresses no stream. Build one with {nameof(OrleansStreamAddress)}.{nameof(Create)}.");
}
