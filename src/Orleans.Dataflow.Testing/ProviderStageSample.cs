using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// One stage of a provider's vocabulary, and a payload that stage's own reader accepts.
/// </summary>
/// <remarks>
/// <para>
/// This is the whole of what <see cref="ProviderConformance"/> asks a provider author to write, and it is
/// deliberately one valid example rather than a description: everything the payload checks need — which
/// members exist, what kind each one is, what a reader must say when one is missing or mistyped — is
/// derived from the example by mutating it, so a provider that adds a member to a payload does not also
/// have to remember to add it to a list somewhere.
/// </para>
/// <para>
/// <b>Every member is required unless it is named optional.</b> The two are checked differently and both
/// are checked: removing a required member must be refused, and removing an optional one must be accepted.
/// A member that is merely undeclared in the sample is not checked at all, which is why the sample should
/// be the fullest payload the stage accepts rather than the smallest.
/// </para>
/// <para>
/// The payload is written by the provider's own typed parameter builder wherever it has one — the pattern
/// REGISTERED-STAGES.md describes — so that the sample is the same value an author would write and not a
/// second spelling of it maintained beside the first.
/// </para>
/// </remarks>
public sealed class ProviderStageSample
{
    /// <summary>Initializes a new instance of the <see cref="ProviderStageSample"/> class.</summary>
    /// <param name="stage">The stage this sample is of.</param>
    /// <param name="parameters">The payload.</param>
    /// <param name="optionalMembers">The payload members a reader must accept the absence of.</param>
    private ProviderStageSample(
        StageRef stage,
        CanonicalJsonValue parameters,
        IReadOnlyList<string> optionalMembers)
    {
        Stage = stage;
        Parameters = parameters;
        OptionalMembers = optionalMembers;
    }

    /// <summary>Gets the stage this sample is of.</summary>
    /// <value>A created <see cref="StageRef"/> the provider's catalog declares.</value>
    public StageRef Stage { get; }

    /// <summary>Gets a payload the stage's own reader accepts.</summary>
    /// <value>A canonical JSON object, never the default value.</value>
    public CanonicalJsonValue Parameters { get; }

    /// <summary>Gets the payload members whose absence the reader has to accept.</summary>
    /// <value>
    /// A read-only list of member names in ordinal order, each of which is a member of
    /// <see cref="Parameters"/>; empty when every member is required.
    /// </value>
    public IReadOnlyList<string> OptionalMembers { get; }

    /// <summary>Declares a sample whose every member is required.</summary>
    /// <param name="stage">The stage this sample is of; must not be the default value.</param>
    /// <param name="parameters">A payload the stage's reader accepts; must be a JSON object.</param>
    /// <returns>The sample.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> is the default value, or <paramref name="parameters"/> is the default value
    /// or is not a JSON object.
    /// </exception>
    public static ProviderStageSample Create(StageRef stage, CanonicalJsonValue parameters) =>
        Create(stage, parameters, []);

    /// <summary>Declares a sample some of whose members a reader may do without.</summary>
    /// <param name="stage">The stage this sample is of; must not be the default value.</param>
    /// <param name="parameters">A payload the stage's reader accepts; must be a JSON object.</param>
    /// <param name="optionalMembers">
    /// The members of <paramref name="parameters"/> a reader must accept the absence of.
    /// </param>
    /// <returns>The sample.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="optionalMembers"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> is the default value, <paramref name="parameters"/> is the default value or
    /// is not a JSON object, or a named optional member is not a member of the payload. The message is a
    /// numbered list of every violation found.
    /// </exception>
    public static ProviderStageSample Create(
        StageRef stage,
        CanonicalJsonValue parameters,
        IEnumerable<string> optionalMembers)
    {
        ArgumentNullException.ThrowIfNull(optionalMembers);

        string[] optional = [.. optionalMembers];
        List<string> violations = [];

        if (stage.IsDefault)
        {
            violations.Add($"the stage reference is the default {nameof(StageRef)}, which names no stage");
        }

        if (parameters.IsDefault)
        {
            violations.Add(
                $"the payload is the default {nameof(CanonicalJsonValue)}, and a sample is a payload a reader accepts");
        }
        else if (parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations.Add("the payload is not a JSON object, and a stage's parameters are a JSON object");
        }
        else
        {
            JsonElement payload = parameters.ToElement();

            for (int index = 0; index < optional.Length; index++)
            {
                if (string.IsNullOrEmpty(optional[index]))
                {
                    violations.Add(string.Create(
                        CultureInfo.InvariantCulture,
                        $"optionalMembers[{index}] names nothing, and an optional member is named"));
                }
                else if (!payload.TryGetProperty(optional[index], out JsonElement _))
                {
                    violations.Add(
                        $"optionalMembers names '{optional[index]}', which the payload does not carry, so nothing could be removed to check it");
                }
            }
        }

        if (violations.Count > 0)
        {
            throw new ArgumentException(ProviderConformance.FormatViolations("stage sample", violations));
        }

        Array.Sort(optional, static (left, right) => string.CompareOrdinal(left, right));

        return new ProviderStageSample(stage, parameters, Array.AsReadOnly(optional));
    }

    /// <summary>Returns a one-line diagnostic summary of this sample.</summary>
    /// <returns>Text of the form <c>orleans/stream-source@v1 (6 members, 1 optional)</c>.</returns>
    public override string ToString()
    {
        int members = Parameters.ToElement().EnumerateObject().Count();

        return string.Create(
            CultureInfo.InvariantCulture,
            $"{Stage} ({members} member{(members == 1 ? string.Empty : "s")}, {OptionalMembers.Count} optional)");
    }
}
