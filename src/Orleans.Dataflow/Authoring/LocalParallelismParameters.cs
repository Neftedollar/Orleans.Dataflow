using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how an asynchronous stage's parallelism is written into a document and read
/// back out of one.
/// </summary>
/// <remarks>
/// <para>
/// The concurrency bound is configuration and belongs in the document under the contract
/// <see cref="LocalVocabulary.ParallelismParameterContract"/>; the callback is behavior and stays in the
/// authoring-side binding table, where every delegate stays. Two graphs that differ only in their bound
/// admit different numbers of callbacks at once, which is observable, so they differ in their fingerprints
/// as well.
/// </para>
/// <para>
/// The payload is a JSON object with exactly one member: <c>maxConcurrency</c>, an integer of at least
/// one. Both asynchronous stages carry the same payload shape, because ordering is which stage was placed
/// and not a parameter of one stage.
/// </para>
/// </remarks>
internal static class LocalParallelismParameters
{
    /// <summary>The payload member holding the greatest number of callbacks in flight at once.</summary>
    internal const string ConcurrencyMember = "maxConcurrency";

    /// <summary>Gets the check the asynchronous stages apply to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one stage's parallelism as the payload its node carries.</summary>
    /// <param name="options">The validated options.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(ParallelismOptions options) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{ConcurrencyMember}\":{options.MaxConcurrency}}}"));

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
    /// <returns><see langword="true"/> when the payload is a valid parallelism payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out ParallelismOptions? options,
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

        bool read = LocalParameterPayload.TryReadPositiveInteger(payload, ConcurrencyMember, found, out int concurrency);

        LocalParameterPayload.ReportUnknownMembers(payload, [ConcurrencyMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        options = new ParallelismOptions { MaxConcurrency = concurrency };

        return true;
    }

    /// <summary>The parameter check of the two asynchronous stages.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out ParallelismOptions? _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
