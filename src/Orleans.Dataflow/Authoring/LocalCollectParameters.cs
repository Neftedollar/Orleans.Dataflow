using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a collecting sink's element bound is written into a document and read back
/// out of one.
/// </summary>
/// <remarks>
/// <para>
/// A collecting sink is the second shape whose memory grows with the data rather than with the graph, after
/// deduplication, and the bound on it is the same kind of statement: it is what makes the sink bounded at
/// all, it changes what the graph observably does when the bound is reached, and it therefore belongs in
/// the payload under the contract <see cref="LocalVocabulary.CollectParameterContract"/>. The element type
/// is not in the payload, because a local document never names one.
/// </para>
/// <para>
/// The payload is a JSON object with exactly one member: <c>maxElements</c>, an integer of at least one.
/// One is a legal bound — it collects a single-element stream and fails on the second element — where zero
/// would describe a sink that fails on every stream that has anything in it.
/// </para>
/// <para>
/// A contract of its own rather than the deduplication contract with a different member name, because the
/// two bound different things and a document has to say which: a key bound and an element bound are not
/// interchangeable, and reusing one identity for both would make a payload readable by the wrong stage.
/// </para>
/// </remarks>
internal static class LocalCollectParameters
{
    /// <summary>The payload member holding the greatest number of elements the sink collects.</summary>
    internal const string MaxElementsMember = "maxElements";

    /// <summary>Gets the check the <c>collect</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one collecting sink's options as the payload its node carries.</summary>
    /// <param name="options">The validated options.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(CollectOptions options) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{MaxElementsMember}\":{options.MaxElements}}}"));

    /// <summary>Reads a payload back into the options it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="options">
    /// When this method returns <see langword="true"/>, the options the payload describes; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid collect payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out CollectOptions? options,
        out IReadOnlyList<string> violations)
    {
        options = null;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = LocalParameterPayload.TryReadPositiveInteger(
            payload,
            MaxElementsMember,
            found,
            out int maxElements);

        LocalParameterPayload.ReportUnknownMembers(payload, [MaxElementsMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        options = new CollectOptions { MaxElements = maxElements };

        return true;
    }

    /// <summary>The parameter check of the <c>collect</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out CollectOptions? _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
