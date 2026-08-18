using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a valve writes the state it starts in into a document and reads it back out
/// of one.
/// </summary>
/// <remarks>
/// <para>
/// A valve's own switch is runtime and never reaches a document — what an author does to a running graph is
/// not topology — but the state it <i>starts</i> in is: a graph whose valve starts closed produces nothing
/// until something opens it, and that is a different graph from one whose valve starts open.
/// </para>
/// <para>
/// The payload is a JSON object with exactly one member: <c>mode</c>, one of two kebab-case names. A name
/// rather than a boolean, for the reason a policy is a name: the document says which state it means instead
/// of leaving a reader to work out which way round <c>true</c> was meant.
/// </para>
/// </remarks>
internal static class LocalValveParameters
{
    /// <summary>The payload member holding the state the valve starts in.</summary>
    internal const string ModeMember = "mode";

    /// <summary>Gets the check the <c>valve</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one valve's initial state as the payload its node carries.</summary>
    /// <param name="mode">The validated initial state.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(ValveMode mode) =>
        CanonicalJsonValue.Parse($"{{\"{ModeMember}\":\"{Spell(mode)}\"}}");

    /// <summary>Reads a payload back into the state it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="mode">
    /// When this method returns <see langword="true"/>, the state the payload declares; otherwise
    /// <see cref="ValveMode.Open"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid valve payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out ValveMode mode,
        out IReadOnlyList<string> violations)
    {
        mode = ValveMode.Open;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        if (!payload.TryGetProperty(ModeMember, out JsonElement declared))
        {
            found.Add(LocalParameterPayload.DescribeMissing(ModeMember));
        }
        else if (declared.ValueKind is not JsonValueKind.String)
        {
            found.Add(LocalParameterPayload.DescribeWrongKind(ModeMember, declared, "one of two state names"));
        }
        else if (declared.GetString() is "open")
        {
            mode = ValveMode.Open;
        }
        else if (declared.GetString() is "closed")
        {
            mode = ValveMode.Closed;
        }
        else
        {
            found.Add(
                $"the member '{ModeMember}' is '{declared.GetString()}', and a valve starts in one of 'open' and 'closed'");
        }

        LocalParameterPayload.ReportUnknownMembers(payload, [ModeMember], found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];

        return true;
    }

    /// <summary>Renders one valve state the way a payload spells it.</summary>
    /// <param name="mode">The state, which may be a value no member declares.</param>
    /// <returns>The kebab-case text, or <see langword="null"/> when the value is not a declared member.</returns>
    internal static string? Spell(ValveMode mode) => mode switch
    {
        ValveMode.Open => "open",
        ValveMode.Closed => "closed",
        _ => null,
    };

    /// <summary>The parameter check of a valve.</summary>
    /// <remarks>
    /// The check is <see cref="TryRead"/> and nothing else, so a payload the catalog accepts is exactly a
    /// payload the run planner can execute.
    /// </remarks>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out ValveMode _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
