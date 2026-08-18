using System.Globalization;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a supervision scope's policy and the chain it answers for are written into
/// a document and read back out of one.
/// </summary>
/// <remarks>
/// <para>
/// A policy that the definition plane cannot see is a policy a cluster cannot honor (ADR 0007), so all of it
/// is here: which form the scope takes, and — for the retrying form alone — how many attempts one element
/// gets, how long the scope waits before each re-offer, and what it does when the attempts run out. What is
/// <em>not</em> here is the fallback a recovering scope emits, because it is a value of an element type no
/// local document names, exactly as the element a <c>single</c> source emits is; ADR 0007's other half of
/// that split — a canonical constant, deployable by construction — is the registered vocabulary's, where
/// element contracts are real.
/// </para>
/// <para>
/// <b>The retry members are present only for the retrying form.</b> A fixed shape would be easier to read
/// and would be a lie: an attempt count on a scope that resumes is a number nothing reads, and a reader who
/// found one would have to guess whether the graph was generated wrong or the engine was ignoring it. So the
/// admitted member list is a function of the form, and the unknown-member report is what says a resuming
/// scope carried a ladder.
/// </para>
/// <para>
/// <b>A rung of zero is admitted</b>, which is the one place this vocabulary's duration rule bends. Every
/// other duration it carries is refused at zero because a delay of no time and a window of no duration
/// describe operators that should have been left out; "try again now" is the ordinary shape of a first rung,
/// so the ladder reads its own ticks rather than the shared duration reader.
/// </para>
/// <para>
/// The chain is <see cref="LocalInnerChain"/>'s array, written and read exactly as a keyed stage's group
/// flow is, and for the same reason: two scopes over different chains are two graphs, and a payload that
/// left the chain out would give them one fingerprint.
/// </para>
/// </remarks>
internal static class LocalSupervisionParameters
{
    /// <summary>The payload member holding which form the scope takes.</summary>
    internal const string FormMember = "form";

    /// <summary>The payload member holding how many attempts one element gets.</summary>
    internal const string MaxAttemptsMember = "maxAttempts";

    /// <summary>The payload member holding how long the scope waits before each re-offer, in ticks.</summary>
    internal const string BackoffMember = "backoffTicks";

    /// <summary>The payload member holding what an element that used every attempt costs.</summary>
    internal const string ExhaustionMember = "onExhaustion";

    /// <summary>The payload member holding the stages the scope answers for.</summary>
    internal const string ScopeMember = "scope";

    /// <summary>The members every scope carries, whatever its form.</summary>
    private static readonly string[] AlwaysDeclared = [FormMember, ScopeMember];

    /// <summary>The members a retrying scope carries in addition to those.</summary>
    private static readonly string[] RetryDeclared =
        [FormMember, ScopeMember, MaxAttemptsMember, BackoffMember, ExhaustionMember];

    /// <summary>Gets the check the <c>supervised</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one scope's policy and chain as the payload its node carries.</summary>
    /// <param name="options">The validated options.</param>
    /// <param name="scope">The validated stages of the scope's chain, in flow order.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(
        SupervisionOptions options,
        IReadOnlyList<LocalStageDescriptor> scope)
    {
        StringBuilder text = new();

        _ = text.Append(CultureInfo.InvariantCulture, $"{{\"{FormMember}\":\"{Spell(options.Form)}\"");

        if (options.Form is SupervisionForm.Retry)
        {
            _ = text.Append(CultureInfo.InvariantCulture, $",\"{MaxAttemptsMember}\":{options.MaxAttempts}")
                .Append(CultureInfo.InvariantCulture, $",\"{ExhaustionMember}\":\"{Spell(options.OnExhaustion)}\"")
                .Append(CultureInfo.InvariantCulture, $",\"{BackoffMember}\":[");

            for (int rung = 0; rung < options.Backoff.Count; rung++)
            {
                _ = text.Append(rung == 0 ? string.Empty : ",")
                    .Append(CultureInfo.InvariantCulture, $"{options.Backoff[rung].Ticks}");
            }

            _ = text.Append(']');
        }

        _ = text.Append(CultureInfo.InvariantCulture, $",\"{ScopeMember}\":");

        return CanonicalJsonValue.Parse(LocalInnerChain.Write(text, scope).Append('}').ToString());
    }

    /// <summary>Renders one supervision form the way a payload spells it.</summary>
    /// <param name="form">The form, which may be a value no member declares.</param>
    /// <returns>The name, or <see langword="null"/> when no member declares that value.</returns>
    internal static string? Spell(SupervisionForm form) => form switch
    {
        SupervisionForm.Resume => "resume",
        SupervisionForm.RestartStage => "restart-stage",
        SupervisionForm.Retry => "retry",
        SupervisionForm.Recover => "recover",
        _ => null,
    };

    /// <summary>Renders one exhaustion answer the way a payload spells it.</summary>
    /// <param name="answer">The answer, which may be a value no member declares.</param>
    /// <returns>The name, or <see langword="null"/> when no member declares that value.</returns>
    internal static string? Spell(RetryExhaustion answer) => answer switch
    {
        RetryExhaustion.Fail => "fail",
        RetryExhaustion.Resume => "resume",
        RetryExhaustion.RestartStage => "restart-stage",
        _ => null,
    };

    /// <summary>Reads a payload back into the policy and the chain it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="options">
    /// When this method returns <see langword="true"/>, the policy the payload describes; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="scope">
    /// When this method returns <see langword="true"/>, the stages of the chain in flow order; otherwise
    /// empty.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid scope payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out SupervisionOptions? options,
        out IReadOnlyList<LocalInnerStage> scope,
        out IReadOnlyList<string> violations)
    {
        options = null;
        scope = [];

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        if (!TryReadForm(payload, found, out SupervisionForm form))
        {
            violations = found;

            return false;
        }

        bool read = LocalInnerChain.TryRead(
            payload,
            LocalInnerChain.Words.Scope,
            found,
            out IReadOnlyList<LocalInnerStage> stages);

        int attempts = 1;
        RetryExhaustion exhaustion = RetryExhaustion.Fail;
        IReadOnlyList<TimeSpan> backoff = [];

        if (form is SupervisionForm.Retry)
        {
            read &= LocalParameterPayload.TryReadPositiveInteger(payload, MaxAttemptsMember, found, out attempts);
            read &= TryReadExhaustion(payload, found, out exhaustion);
            read &= TryReadBackoff(payload, found, out backoff);
        }

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            form is SupervisionForm.Retry ? RetryDeclared : AlwaysDeclared,
            found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        scope = stages;
        options = new SupervisionOptions
        {
            Form = form,
            MaxAttempts = attempts,
            Backoff = backoff,
            OnExhaustion = exhaustion,
        };

        return true;
    }

    /// <summary>Reads the member that has to name one of the four supervision forms.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="form">
    /// When this method returns <see langword="true"/>, the form; otherwise
    /// <see cref="SupervisionForm.Resume"/>.
    /// </param>
    /// <returns><see langword="true"/> when the member is present and names a declared form.</returns>
    /// <remarks>
    /// Read before anything else and reported alone when it is wrong, because the form is what decides
    /// which other members the payload may carry: a report that also complained about a missing attempt
    /// count would be describing a shape nobody can know is the intended one.
    /// </remarks>
    private static bool TryReadForm(JsonElement payload, List<string> violations, out SupervisionForm form)
    {
        form = SupervisionForm.Resume;

        if (!payload.TryGetProperty(FormMember, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(FormMember));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(FormMember, declared, "one of four form names"));

            return false;
        }

        switch (declared.GetString())
        {
            case "resume":
                form = SupervisionForm.Resume;

                return true;
            case "restart-stage":
                form = SupervisionForm.RestartStage;

                return true;
            case "retry":
                form = SupervisionForm.Retry;

                return true;
            case "recover":
                form = SupervisionForm.Recover;

                return true;
            default:
                violations.Add(
                    $"the member '{FormMember}' is '{declared.GetString()}', and a supervision form is one of 'resume', 'restart-stage', 'retry', and 'recover'");

                return false;
        }
    }

    /// <summary>Reads the member that has to name one of the three exhaustion answers.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="answer">
    /// When this method returns <see langword="true"/>, the answer; otherwise
    /// <see cref="RetryExhaustion.Fail"/>.
    /// </param>
    /// <returns><see langword="true"/> when the member is present and names a declared answer.</returns>
    private static bool TryReadExhaustion(
        JsonElement payload,
        List<string> violations,
        out RetryExhaustion answer)
    {
        answer = RetryExhaustion.Fail;

        if (!payload.TryGetProperty(ExhaustionMember, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(ExhaustionMember));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(
                LocalParameterPayload.DescribeWrongKind(ExhaustionMember, declared, "one of three answer names"));

            return false;
        }

        switch (declared.GetString())
        {
            case "fail":
                answer = RetryExhaustion.Fail;

                return true;
            case "resume":
                answer = RetryExhaustion.Resume;

                return true;
            case "restart-stage":
                answer = RetryExhaustion.RestartStage;

                return true;
            default:
                violations.Add(
                    $"the member '{ExhaustionMember}' is '{declared.GetString()}', and an exhaustion answer is one of 'fail', 'resume', and 'restart-stage'");

                return false;
        }
    }

    /// <summary>Reads the member that has to be the ladder of waits before the re-offers.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="backoff">
    /// When this method returns <see langword="true"/>, the ladder in attempt order; otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the member is an array of tick counts of zero or more.</returns>
    /// <remarks>
    /// An empty ladder is legal and means every re-offer happens at once, which is the honest encoding of a
    /// retry an author declared without a wait; the alternative would be a magic rung that meant "none".
    /// </remarks>
    private static bool TryReadBackoff(
        JsonElement payload,
        List<string> violations,
        out IReadOnlyList<TimeSpan> backoff)
    {
        backoff = [];

        if (!payload.TryGetProperty(BackoffMember, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(BackoffMember));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.Array)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(
                BackoffMember,
                declared,
                "an array of tick counts of zero or more"));

            return false;
        }

        List<TimeSpan> ladder = [];
        bool read = true;
        int rung = 0;

        foreach (JsonElement wait in declared.EnumerateArray())
        {
            rung++;

            if (wait.ValueKind is not JsonValueKind.Number || !wait.TryGetInt64(out long ticks) || ticks < 0)
            {
                violations.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"rung {rung} of the member '{BackoffMember}' is {wait.GetRawText()}, and it is a count of ticks of zero or more"));

                read = false;

                continue;
            }

            ladder.Add(TimeSpan.FromTicks(ticks));
        }

        backoff = read ? ladder : [];

        return read;
    }

    /// <summary>The parameter check of the <c>supervised</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(
                parameters,
                out SupervisionOptions? _,
                out IReadOnlyList<LocalInnerStage> _,
                out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
