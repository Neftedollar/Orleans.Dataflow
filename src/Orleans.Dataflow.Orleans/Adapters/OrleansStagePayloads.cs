using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// How an Orleans stream source states which stream it reads and how much of it the run will hold.
/// </summary>
/// <remarks>
/// <para>
/// Six members and every one of them is configuration a document can carry honestly: the element contract
/// the stream carries, the three parts of the stream's address, and the capacity and overflow policy of the
/// bounded ingress the deliveries land in. Nothing here is behavior, so nothing here is a delegate.
/// </para>
/// <para>
/// The buffer's two members are spelled exactly as <see cref="LocalBufferParameters"/> spells them and are
/// read through the very same parser, because a full ingress queue and a full buffer are the same situation
/// seen from the two sides of a graph and a second dialect of "drop the oldest" would be a second contract.
/// </para>
/// </remarks>
internal static class StreamSourcePayload
{
    /// <summary>The payload member holding the ingress capacity.</summary>
    internal const string CapacityMember = "capacity";

    /// <summary>The payload member holding the element contract the stream carries.</summary>
    internal const string ElementMember = "element";

    /// <summary>The payload member holding the stream key.</summary>
    internal const string KeyMember = "key";

    /// <summary>The payload member holding the stream namespace.</summary>
    internal const string NamespaceMember = "namespace";

    /// <summary>The payload member holding the ingress overflow policy.</summary>
    internal const string PolicyMember = "overflowPolicy";

    /// <summary>The payload member holding the stream provider's registration name.</summary>
    internal const string ProviderMember = "provider";

    /// <summary>Writes the payload of one stream source.</summary>
    /// <param name="element">The contract text of the elements the stream carries.</param>
    /// <param name="address">The stream's address.</param>
    /// <param name="ingress">The bounded ingress the deliveries land in.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(
        string element,
        OrleansStreamAddress address,
        BufferOptions ingress) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CapacityMember}\":{ingress.Capacity}," +
            $"\"{ElementMember}\":{JsonSerializer.Serialize(element)}," +
            $"\"{KeyMember}\":{JsonSerializer.Serialize(address.Key)}," +
            $"\"{NamespaceMember}\":{JsonSerializer.Serialize(address.Namespace)}," +
            $"\"{PolicyMember}\":\"{LocalBufferParameters.Spell(ingress.OverflowPolicy)}\"," +
            $"\"{ProviderMember}\":{JsonSerializer.Serialize(address.Provider)}}}"));

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid stream-source payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out StreamSourceDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!OrleansPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        int capacity = 0;
        OverflowPolicy policy = OverflowPolicy.Backpressure;

        if (LocalParameterPayload.TryReadPositiveInteger(payload, CapacityMember, found, out int declared))
        {
            capacity = declared;
        }

        string? element = OrleansPayload.ReadText(payload, ElementMember, found);
        string? key = OrleansPayload.ReadText(payload, KeyMember, found);
        string? streamNamespace = OrleansPayload.ReadText(payload, NamespaceMember, found);
        string? provider = OrleansPayload.ReadText(payload, ProviderMember, found);

        if (!payload.TryGetProperty(PolicyMember, out JsonElement policyMember))
        {
            found.Add(LocalParameterPayload.DescribeMissing(PolicyMember));
        }
        else if (policyMember.ValueKind is not JsonValueKind.String)
        {
            found.Add(LocalParameterPayload.DescribeWrongKind(
                PolicyMember,
                policyMember,
                "one of five policy names"));
        }
        else if (!LocalBufferParameters.TryParse(policyMember.GetString()!, out policy))
        {
            found.Add(
                $"the member '{PolicyMember}' is '{policyMember.GetString()}', and an overflow policy is one of 'backpressure', 'drop-oldest', 'drop-newest', 'drop-buffer', and 'fail'");
        }

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [CapacityMember, ElementMember, KeyMember, NamespaceMember, PolicyMember, ProviderMember],
            found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new StreamSourceDeclaration(
            element!,
            OrleansStreamAddress.Create(provider!, streamNamespace!, key!),
            new BufferOptions { Capacity = capacity, OverflowPolicy = policy });

        return true;
    }
}

/// <summary>What a stream source's payload declares.</summary>
/// <param name="Element">The contract text of the elements the stream carries.</param>
/// <param name="Address">The stream's address.</param>
/// <param name="Ingress">The bounded ingress the deliveries land in.</param>
internal sealed record StreamSourceDeclaration(
    string Element,
    OrleansStreamAddress Address,
    BufferOptions Ingress);

/// <summary>
/// How an Orleans stream sink states which stream it publishes to.
/// </summary>
/// <remarks>
/// Four members and no buffer: a sink publishes what the run hands it, one element at a time, and the
/// awaited publication is the whole of its backpressure. A capacity here would describe a queue nothing
/// keeps.
/// </remarks>
internal static class StreamSinkPayload
{
    /// <summary>Writes the payload of one stream sink.</summary>
    /// <param name="element">The contract text of the elements the stream carries.</param>
    /// <param name="address">The stream's address.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(string element, OrleansStreamAddress address) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{StreamSourcePayload.ElementMember}\":{JsonSerializer.Serialize(element)}," +
            $"\"{StreamSourcePayload.KeyMember}\":{JsonSerializer.Serialize(address.Key)}," +
            $"\"{StreamSourcePayload.NamespaceMember}\":{JsonSerializer.Serialize(address.Namespace)}," +
            $"\"{StreamSourcePayload.ProviderMember}\":{JsonSerializer.Serialize(address.Provider)}}}"));

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid stream-sink payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out StreamSinkDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!OrleansPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        string? element = OrleansPayload.ReadText(payload, StreamSourcePayload.ElementMember, found);
        string? key = OrleansPayload.ReadText(payload, StreamSourcePayload.KeyMember, found);
        string? streamNamespace = OrleansPayload.ReadText(payload, StreamSourcePayload.NamespaceMember, found);
        string? provider = OrleansPayload.ReadText(payload, StreamSourcePayload.ProviderMember, found);

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [
                StreamSourcePayload.ElementMember,
                StreamSourcePayload.KeyMember,
                StreamSourcePayload.NamespaceMember,
                StreamSourcePayload.ProviderMember,
            ],
            found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new StreamSinkDeclaration(
            element!,
            OrleansStreamAddress.Create(provider!, streamNamespace!, key!));

        return true;
    }
}

/// <summary>What a stream sink's payload declares.</summary>
/// <param name="Element">The contract text of the elements the stream carries.</param>
/// <param name="Address">The stream's address.</param>
internal sealed record StreamSinkDeclaration(string Element, OrleansStreamAddress Address);

/// <summary>
/// How an awaited grain call states which registration it addresses and how it is bounded.
/// </summary>
/// <remarks>
/// <para>
/// The name is what a document may carry in place of a CLR member, per ADR 0001. The two contract
/// references beside it are what makes the name checkable: a silo compares them against its own
/// registration under that name and refuses a document whose author compiled against a different signature,
/// which is the one check the CLR type system cannot make across a deployment boundary.
/// </para>
/// <para>
/// The sink form carries the same members without <c>output</c>, which is why one reader serves both: the
/// difference between them is a reply that is returned and a reply that is discarded, and a payload should
/// not have to restate that.
/// </para>
/// </remarks>
internal static class GrainCallPayload
{
    /// <summary>The payload member holding the registered call's name.</summary>
    internal const string CallMember = "call";

    /// <summary>The payload member holding the contract of the elements the call consumes.</summary>
    internal const string InputMember = "input";

    /// <summary>The payload member holding the greatest number of calls in flight at once.</summary>
    internal const string MaxInFlightMember = "maxInFlight";

    /// <summary>The payload member holding the contract of the elements the call produces.</summary>
    internal const string OutputMember = "output";

    /// <summary>The payload member holding the per-call timeout in milliseconds.</summary>
    internal const string TimeoutMember = "timeoutMilliseconds";

    /// <summary>Writes the payload of one grain call.</summary>
    /// <param name="call">The registered call's name.</param>
    /// <param name="input">The contract text of the elements the call consumes.</param>
    /// <param name="output">The contract text of the elements the call produces, or <see langword="null"/>.</param>
    /// <param name="maxInFlight">The greatest number of calls in flight at once.</param>
    /// <param name="timeout">The per-call timeout, or <see langword="null"/> for no timeout of our own.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(
        string call,
        string input,
        string? output,
        int maxInFlight,
        TimeSpan? timeout)
    {
        string outputMember = output is null
            ? string.Empty
            : $",\"{OutputMember}\":{JsonSerializer.Serialize(output)}";
        string timeoutMember = timeout is null
            ? string.Empty
            : string.Create(
                CultureInfo.InvariantCulture,
                $",\"{TimeoutMember}\":{(long)timeout.Value.TotalMilliseconds}");

        return CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CallMember}\":{JsonSerializer.Serialize(call)}," +
            $"\"{InputMember}\":{JsonSerializer.Serialize(input)}," +
            $"\"{MaxInFlightMember}\":{maxInFlight}{outputMember}{timeoutMember}}}"));
    }

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="expectsOutput">Whether this occurrence is the transforming form rather than the sink.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid grain-call payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        bool expectsOutput,
        out GrainCallDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!OrleansPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        string? call = OrleansPayload.ReadText(payload, CallMember, found);
        string? input = OrleansPayload.ReadText(payload, InputMember, found);
        string? output = expectsOutput ? OrleansPayload.ReadText(payload, OutputMember, found) : null;
        int maxInFlight = 0;

        if (LocalParameterPayload.TryReadPositiveInteger(payload, MaxInFlightMember, found, out int declared))
        {
            maxInFlight = declared;
        }

        TimeSpan? timeout = null;

        // Optional, and read only when it is there: a stage without a timeout of its own leaves the wait to
        // Orleans' own call timeout, which is a different contract rather than a missing member.
        if (payload.TryGetProperty(TimeoutMember, out JsonElement _) &&
            LocalParameterPayload.TryReadPositiveInteger(payload, TimeoutMember, found, out int milliseconds))
        {
            timeout = TimeSpan.FromMilliseconds(milliseconds);
        }

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            expectsOutput
                ? [CallMember, InputMember, MaxInFlightMember, OutputMember, TimeoutMember]
                : [CallMember, InputMember, MaxInFlightMember, TimeoutMember],
            found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new GrainCallDeclaration(call!, input!, output, maxInFlight, timeout);

        return true;
    }
}

/// <summary>What a grain call's payload declares.</summary>
/// <param name="Call">The registered call's name.</param>
/// <param name="Input">The contract text of the elements the call consumes.</param>
/// <param name="Output">The contract text of the replies, or <see langword="null"/> for the sink form.</param>
/// <param name="MaxInFlight">The greatest number of calls in flight at once.</param>
/// <param name="Timeout">The per-call timeout, or <see langword="null"/>.</param>
internal sealed record GrainCallDeclaration(
    string Call,
    string Input,
    string? Output,
    int MaxInFlight,
    TimeSpan? Timeout);

/// <summary>
/// How a keyed grain call states which registration it addresses, how it is bounded, and whether it runs
/// inside the run or on per-key executor grains.
/// </summary>
/// <remarks>
/// <para>
/// The four members a plain grain call has, plus one: <c>distributed</c>. That flag is the opt-in that keeps
/// M3's doctrine intact — runs distribute before stages do, so a stage that distributes below a run says so
/// in the document rather than doing it because it could. A document that does not ask for it gets the
/// run-local keyed path, which is the default and stays the default.
/// </para>
/// <para>
/// <b>There is no per-key bound member, and its absence is the contract.</b> A keyed stage holds exactly one
/// call in flight per key, always, because that is where its per-key ordering comes from: the next element
/// of a key is not sent until the previous one has replied, so nothing between the run and the grain has to
/// order anything. Orleans undertakes no pairwise ordering between activations, and this repository's own
/// probe watched it reorder pipelined calls inside a single silo, so a member that let a document ask for
/// two in flight per key would be a knob for silently losing the ordering the stage promises.
/// <c>maxInFlight</c> therefore bounds distinct keys and only distinct keys.
/// </para>
/// </remarks>
internal static class KeyedGrainCallPayload
{
    /// <summary>The payload member holding whether the stage runs on per-key executor grains.</summary>
    internal const string DistributedMember = "distributed";

    /// <summary>Writes the payload of one keyed grain call.</summary>
    /// <param name="call">The registered call's name.</param>
    /// <param name="input">The contract text of the elements the call consumes.</param>
    /// <param name="output">The contract text of the elements the call produces.</param>
    /// <param name="maxInFlight">The greatest number of calls in flight at once, across keys.</param>
    /// <param name="distributed">Whether the stage runs on per-key executor grains.</param>
    /// <param name="timeout">The per-call timeout, or <see langword="null"/> for no timeout of our own.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(
        string call,
        string input,
        string output,
        int maxInFlight,
        bool distributed,
        TimeSpan? timeout)
    {
        string timeoutMember = timeout is null
            ? string.Empty
            : string.Create(
                CultureInfo.InvariantCulture,
                $",\"{GrainCallPayload.TimeoutMember}\":{(long)timeout.Value.TotalMilliseconds}");

        return CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{GrainCallPayload.CallMember}\":{JsonSerializer.Serialize(call)}," +
            $"\"{DistributedMember}\":{(distributed ? "true" : "false")}," +
            $"\"{GrainCallPayload.InputMember}\":{JsonSerializer.Serialize(input)}," +
            $"\"{GrainCallPayload.MaxInFlightMember}\":{maxInFlight}," +
            $"\"{GrainCallPayload.OutputMember}\":{JsonSerializer.Serialize(output)}{timeoutMember}}}"));
    }

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid keyed-grain-call payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out KeyedGrainCallDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!OrleansPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        string? call = OrleansPayload.ReadText(payload, GrainCallPayload.CallMember, found);
        string? input = OrleansPayload.ReadText(payload, GrainCallPayload.InputMember, found);
        string? output = OrleansPayload.ReadText(payload, GrainCallPayload.OutputMember, found);
        int maxInFlight = 0;

        if (LocalParameterPayload.TryReadPositiveInteger(
            payload,
            GrainCallPayload.MaxInFlightMember,
            found,
            out int declared))
        {
            maxInFlight = declared;
        }

        bool distributed = false;

        if (!payload.TryGetProperty(DistributedMember, out JsonElement mode))
        {
            found.Add(LocalParameterPayload.DescribeMissing(DistributedMember));
        }
        else if (mode.ValueKind is not JsonValueKind.True and not JsonValueKind.False)
        {
            found.Add(LocalParameterPayload.DescribeWrongKind(DistributedMember, mode, "true or false"));
        }
        else
        {
            distributed = mode.ValueKind is JsonValueKind.True;
        }

        TimeSpan? timeout = null;

        // Optional, exactly as the plain grain call's is: a stage without a timeout of its own leaves the
        // wait to Orleans' own call timeout, which is a different contract rather than a missing member.
        if (payload.TryGetProperty(GrainCallPayload.TimeoutMember, out JsonElement _) &&
            LocalParameterPayload.TryReadPositiveInteger(
                payload,
                GrainCallPayload.TimeoutMember,
                found,
                out int milliseconds))
        {
            timeout = TimeSpan.FromMilliseconds(milliseconds);
        }

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [
                GrainCallPayload.CallMember,
                DistributedMember,
                GrainCallPayload.InputMember,
                GrainCallPayload.MaxInFlightMember,
                GrainCallPayload.OutputMember,
                GrainCallPayload.TimeoutMember,
            ],
            found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new KeyedGrainCallDeclaration(call!, input!, output!, maxInFlight, distributed, timeout);

        return true;
    }
}

/// <summary>What a keyed grain call's payload declares.</summary>
/// <param name="Call">The registered call's name.</param>
/// <param name="Input">The contract text of the elements the call consumes.</param>
/// <param name="Output">The contract text of the replies.</param>
/// <param name="MaxInFlight">The greatest number of calls in flight at once, across keys.</param>
/// <param name="Distributed">Whether the stage runs on per-key executor grains.</param>
/// <param name="Timeout">The per-call timeout, or <see langword="null"/>.</param>
internal sealed record KeyedGrainCallDeclaration(
    string Call,
    string Input,
    string Output,
    int MaxInFlight,
    bool Distributed,
    TimeSpan? Timeout);

/// <summary>
/// How a grain enumeration states which registration it opens.
/// </summary>
internal static class GrainEnumerablePayload
{
    /// <summary>The payload member holding the contract of the elements the enumeration produces.</summary>
    internal const string OutputMember = "output";

    /// <summary>The payload member holding the registered source's name.</summary>
    internal const string SourceMember = "source";

    /// <summary>Writes the payload of one grain enumeration.</summary>
    /// <param name="source">The registered source's name.</param>
    /// <param name="output">The contract text of the elements the enumeration produces.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(string source, string output) =>
        CanonicalJsonValue.Parse(
            $"{{\"{OutputMember}\":{JsonSerializer.Serialize(output)}," +
            $"\"{SourceMember}\":{JsonSerializer.Serialize(source)}}}");

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid grain-enumerable payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out GrainEnumerableDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!OrleansPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        string? output = OrleansPayload.ReadText(payload, OutputMember, found);
        string? source = OrleansPayload.ReadText(payload, SourceMember, found);

        LocalParameterPayload.ReportUnknownMembers(payload, [OutputMember, SourceMember], found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new GrainEnumerableDeclaration(source!, output!);

        return true;
    }
}

/// <summary>What a grain enumeration's payload declares.</summary>
/// <param name="Source">The registered source's name.</param>
/// <param name="Output">The contract text of the elements the enumeration produces.</param>
internal sealed record GrainEnumerableDeclaration(string Source, string Output);

/// <summary>
/// How a reminder trigger states how often it fires and how much of a backlog the run will hold.
/// </summary>
/// <remarks>
/// <para>
/// Three members: the period, and the capacity and overflow policy of the bounded ingress the ticks land
/// in. The policy is read through the very same parser a buffer's is, and refused for exactly one of the
/// five values — a clock cannot be slowed, so <c>backpressure</c> would mean parking the grain turn that
/// owns the cluster's reminder for this run, and a stage that did that would put every later tick behind
/// the run it was meant to wake.
/// </para>
/// <para>
/// The period is written in milliseconds and checked here only for being positive. Whether it clears the
/// cluster's configured <c>ReminderOptions.MinimumReminderPeriod</c> is a property of a silo rather than of
/// a document, and it is checked where that option can be read.
/// </para>
/// </remarks>
internal static class ReminderTriggerPayload
{
    /// <summary>The payload member holding the ingress capacity.</summary>
    internal const string CapacityMember = "capacity";

    /// <summary>The payload member holding the period between ticks in milliseconds.</summary>
    internal const string PeriodMember = "periodMilliseconds";

    /// <summary>The payload member holding the ingress overflow policy.</summary>
    internal const string PolicyMember = "overflowPolicy";

    /// <summary>Writes the payload of one reminder trigger.</summary>
    /// <param name="period">The period between ticks.</param>
    /// <param name="ingress">The bounded ingress the ticks land in.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(TimeSpan period, BufferOptions ingress) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CapacityMember}\":{ingress.Capacity}," +
            $"\"{PeriodMember}\":{(long)period.TotalMilliseconds}," +
            $"\"{PolicyMember}\":\"{LocalBufferParameters.Spell(ingress.OverflowPolicy)}\"}}"));

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid reminder-trigger payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out ReminderTriggerDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!OrleansPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        int capacity = 0;
        int period = 0;

        if (LocalParameterPayload.TryReadPositiveInteger(payload, CapacityMember, found, out int bound))
        {
            capacity = bound;
        }

        if (LocalParameterPayload.TryReadPositiveInteger(payload, PeriodMember, found, out int declared))
        {
            period = declared;
        }

        OverflowPolicy policy = OrleansPayload.ReadPolicy(payload, PolicyMember, found);

        if (policy is OverflowPolicy.Backpressure && found.Count == 0)
        {
            found.Add(
                $"the member '{PolicyMember}' is 'backpressure', and a reminder trigger cannot backpressure a cluster reminder: a tick that finds no room is dropped or fails by policy, so declare one of 'drop-oldest', 'drop-newest', 'drop-buffer', and 'fail'");
        }

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [CapacityMember, PeriodMember, PolicyMember],
            found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new ReminderTriggerDeclaration(
            TimeSpan.FromMilliseconds(period),
            new BufferOptions { Capacity = capacity, OverflowPolicy = policy });

        return true;
    }
}

/// <summary>What a reminder trigger's payload declares.</summary>
/// <param name="Period">The period between ticks.</param>
/// <param name="Ingress">The bounded ingress the ticks land in.</param>
internal sealed record ReminderTriggerDeclaration(TimeSpan Period, BufferOptions Ingress);

/// <summary>
/// How an observer bridge states which registration it exposes and how much of it the run will hold.
/// </summary>
internal static class ObserverBridgePayload
{
    /// <summary>The payload member holding the registered bridge's name.</summary>
    internal const string BridgeMember = "bridge";

    /// <summary>The payload member holding the ingress capacity.</summary>
    internal const string CapacityMember = "capacity";

    /// <summary>The payload member holding the contract of the elements pushed at the bridge.</summary>
    internal const string OutputMember = "output";

    /// <summary>The payload member holding the ingress overflow policy.</summary>
    internal const string PolicyMember = "overflowPolicy";

    /// <summary>Writes the payload of one observer bridge.</summary>
    /// <param name="bridge">The registered bridge's name.</param>
    /// <param name="output">The contract text of the elements pushed at it.</param>
    /// <param name="ingress">The bounded ingress the pushes land in.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(string bridge, string output, BufferOptions ingress) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{BridgeMember}\":{JsonSerializer.Serialize(bridge)}," +
            $"\"{CapacityMember}\":{ingress.Capacity}," +
            $"\"{OutputMember}\":{JsonSerializer.Serialize(output)}," +
            $"\"{PolicyMember}\":\"{LocalBufferParameters.Spell(ingress.OverflowPolicy)}\"}}"));

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid observer-bridge payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out ObserverBridgeDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!OrleansPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        int capacity = 0;

        if (LocalParameterPayload.TryReadPositiveInteger(payload, CapacityMember, found, out int declared))
        {
            capacity = declared;
        }

        string? bridge = OrleansPayload.ReadText(payload, BridgeMember, found);
        string? output = OrleansPayload.ReadText(payload, OutputMember, found);
        OverflowPolicy policy = OrleansPayload.ReadPolicy(payload, PolicyMember, found);

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [BridgeMember, CapacityMember, OutputMember, PolicyMember],
            found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new ObserverBridgeDeclaration(
            bridge!,
            output!,
            new BufferOptions { Capacity = capacity, OverflowPolicy = policy });

        return true;
    }
}

/// <summary>What an observer bridge's payload declares.</summary>
/// <param name="Bridge">The registered bridge's name.</param>
/// <param name="Output">The contract text of the elements pushed at it.</param>
/// <param name="Ingress">The bounded ingress the pushes land in.</param>
internal sealed record ObserverBridgeDeclaration(string Bridge, string Output, BufferOptions Ingress);

/// <summary>
/// How a Broadcast Channel sink states which channel it publishes to and what it expects of the provider.
/// </summary>
/// <remarks>
/// Five members: the element contract, the three parts of the channel's address, and the delivery mode the
/// author wrote the document against. The last one is a declaration rather than a setting — a channel's
/// <c>FireAndForgetDelivery</c> is configured on the provider and cannot be chosen per publication — so
/// what carrying it buys is a check: a silo whose provider is configured the other way refuses the document
/// instead of quietly giving the run different semantics from the ones it was written for.
/// </remarks>
internal static class BroadcastSinkPayload
{
    /// <summary>The payload member holding the element contract the channel carries.</summary>
    internal const string ElementMember = "element";

    /// <summary>The payload member holding the delivery mode the author declared.</summary>
    internal const string FireAndForgetMember = "fireAndForgetDelivery";

    /// <summary>The payload member holding the channel key.</summary>
    internal const string KeyMember = "key";

    /// <summary>The payload member holding the channel namespace.</summary>
    internal const string NamespaceMember = "namespace";

    /// <summary>The payload member holding the broadcast provider's registration name.</summary>
    internal const string ProviderMember = "provider";

    /// <summary>Writes the payload of one broadcast sink.</summary>
    /// <param name="element">The contract text of the elements the channel carries.</param>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="channelNamespace">The channel's namespace.</param>
    /// <param name="key">The channel's key.</param>
    /// <param name="fireAndForgetDelivery">The delivery mode the author declared.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(
        string element,
        string provider,
        string channelNamespace,
        string key,
        bool fireAndForgetDelivery) =>
        CanonicalJsonValue.Parse(
            $"{{\"{ElementMember}\":{JsonSerializer.Serialize(element)}," +
            $"\"{FireAndForgetMember}\":{(fireAndForgetDelivery ? "true" : "false")}," +
            $"\"{KeyMember}\":{JsonSerializer.Serialize(key)}," +
            $"\"{NamespaceMember}\":{JsonSerializer.Serialize(channelNamespace)}," +
            $"\"{ProviderMember}\":{JsonSerializer.Serialize(provider)}}}");

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid broadcast-sink payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out BroadcastSinkDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!OrleansPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        string? element = OrleansPayload.ReadText(payload, ElementMember, found);
        string? key = OrleansPayload.ReadText(payload, KeyMember, found);
        string? channelNamespace = OrleansPayload.ReadText(payload, NamespaceMember, found);
        string? provider = OrleansPayload.ReadText(payload, ProviderMember, found);
        bool fireAndForget = false;

        if (!payload.TryGetProperty(FireAndForgetMember, out JsonElement declared))
        {
            found.Add(LocalParameterPayload.DescribeMissing(FireAndForgetMember));
        }
        else if (declared.ValueKind is not JsonValueKind.True and not JsonValueKind.False)
        {
            found.Add(LocalParameterPayload.DescribeWrongKind(FireAndForgetMember, declared, "true or false"));
        }
        else
        {
            fireAndForget = declared.ValueKind is JsonValueKind.True;
        }

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [ElementMember, FireAndForgetMember, KeyMember, NamespaceMember, ProviderMember],
            found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new BroadcastSinkDeclaration(
            element!,
            provider!,
            channelNamespace!,
            key!,
            fireAndForget);

        return true;
    }
}

/// <summary>What a broadcast sink's payload declares.</summary>
/// <param name="Element">The contract text of the elements the channel carries.</param>
/// <param name="Provider">The broadcast provider's registration name.</param>
/// <param name="Namespace">The channel's namespace.</param>
/// <param name="Key">The channel's key.</param>
/// <param name="FireAndForgetDelivery">The delivery mode the author declared.</param>
internal sealed record BroadcastSinkDeclaration(
    string Element,
    string Provider,
    string Namespace,
    string Key,
    bool FireAndForgetDelivery);

/// <summary>
/// How a Broadcast Channel source states which channel it consumes and how much of it the run will hold.
/// </summary>
/// <remarks>
/// <para>
/// Five members, and the interesting thing about them is the one that is missing. There is no
/// <c>namespace</c>: a channel a run can consume is always in the namespace this package's relay grain
/// subscribes to, because Broadcast Channel subscription is implicit — a compile-time attribute on a grain
/// type — and no run can subscribe to a namespace chosen by a document. So the document names a channel
/// <em>key</em> and the namespace is the platform's answer rather than an author's choice. The sink's
/// payload keeps its namespace, because publishing needs no subscription.
/// </para>
/// <para>
/// There is no <c>fireAndForgetDelivery</c> either, and its absence is a measured result rather than an
/// omission. That mode decides whether a <em>publisher</em> waits for its subscribers and whether their
/// failures reach it; a subscriber's own contract is identical under both, and this relay never fails a
/// publication in either. A member here would therefore be a declaration with nothing to check it against.
/// </para>
/// <para>
/// The overflow policy is refused for exactly one of its five values, for the reason the reminder trigger
/// refuses the same one: the relay forwards on its own grain turn and serves every run listening to the
/// channel, so a run that waited for room would hold that turn and stop the channel for everybody — and
/// under a fire-and-forget provider it would hold it while no publisher was waiting at all.
/// </para>
/// </remarks>
internal static class BroadcastSourcePayload
{
    /// <summary>The payload member holding the ingress capacity.</summary>
    internal const string CapacityMember = "capacity";

    /// <summary>The payload member holding the element contract the channel carries.</summary>
    internal const string ElementMember = "element";

    /// <summary>The payload member holding the channel key within this package's own namespace.</summary>
    internal const string KeyMember = "key";

    /// <summary>The payload member holding the ingress overflow policy.</summary>
    internal const string PolicyMember = "overflowPolicy";

    /// <summary>The payload member holding the broadcast provider's registration name.</summary>
    internal const string ProviderMember = "provider";

    /// <summary>Writes the payload of one broadcast source.</summary>
    /// <param name="element">The contract text of the elements the channel carries.</param>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="key">The channel's key.</param>
    /// <param name="ingress">The bounded ingress the publications land in.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(
        string element,
        string provider,
        string key,
        BufferOptions ingress) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CapacityMember}\":{ingress.Capacity}," +
            $"\"{ElementMember}\":{JsonSerializer.Serialize(element)}," +
            $"\"{KeyMember}\":{JsonSerializer.Serialize(key)}," +
            $"\"{PolicyMember}\":\"{LocalBufferParameters.Spell(ingress.OverflowPolicy)}\"," +
            $"\"{ProviderMember}\":{JsonSerializer.Serialize(provider)}}}"));

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid broadcast-source payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out BroadcastSourceDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!OrleansPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        int capacity = 0;

        if (LocalParameterPayload.TryReadPositiveInteger(payload, CapacityMember, found, out int declared))
        {
            capacity = declared;
        }

        string? element = OrleansPayload.ReadText(payload, ElementMember, found);
        string? key = OrleansPayload.ReadText(payload, KeyMember, found);
        string? provider = OrleansPayload.ReadText(payload, ProviderMember, found);
        OverflowPolicy policy = OrleansPayload.ReadPolicy(payload, PolicyMember, found);

        if (policy is OverflowPolicy.Backpressure && found.Count == 0)
        {
            found.Add(
                $"the member '{PolicyMember}' is 'backpressure', and a broadcast source cannot backpressure a channel: the relay grain forwards to every run listening to that channel on one turn, so a run waiting for room would stop the channel for all of them and, under a fire-and-forget provider, for no publisher's benefit at all. Declare one of 'drop-oldest', 'drop-newest', 'drop-buffer', and 'fail'");
        }

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [CapacityMember, ElementMember, KeyMember, PolicyMember, ProviderMember],
            found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new BroadcastSourceDeclaration(
            element!,
            provider!,
            key!,
            new BufferOptions { Capacity = capacity, OverflowPolicy = policy });

        return true;
    }
}

/// <summary>What a broadcast source's payload declares.</summary>
/// <param name="Element">The contract text of the elements the channel carries.</param>
/// <param name="Provider">The broadcast provider's registration name.</param>
/// <param name="Key">The channel's key within this package's own channel namespace.</param>
/// <param name="Ingress">The bounded ingress the publications land in.</param>
internal sealed record BroadcastSourceDeclaration(
    string Element,
    string Provider,
    string Key,
    BufferOptions Ingress);

/// <summary>
/// The payload rules every Orleans adapter shares.
/// </summary>
/// <remarks>
/// Two of them, both about text: a payload is a JSON object, and a member that names something is a
/// non-empty string. The numeric rules are <see cref="LocalParameterPayload"/>'s and are reused rather than
/// restated, so a capacity is refused in the same words wherever it appears.
/// </remarks>
internal static class OrleansPayload
{
    /// <summary>Opens a payload that has to be a JSON object.</summary>
    /// <param name="parameters">The payload.</param>
    /// <param name="payload">When this method returns <see langword="true"/>, the object.</param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, the single violation saying what it was instead.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a JSON object.</returns>
    internal static bool TryOpen(
        CanonicalJsonValue parameters,
        out JsonElement payload,
        out IReadOnlyList<string> violations)
    {
        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            payload = default;
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        payload = parameters.ToElement();
        violations = [];

        return true;
    }

    /// <summary>Reads a member that has to be a non-empty string.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="member">The member name.</param>
    /// <param name="violations">The report under construction, appended to when the member is wrong.</param>
    /// <returns>The text, or <see langword="null"/> when the member is missing or wrong.</returns>
    internal static string? ReadText(JsonElement payload, string member, List<string> violations)
    {
        if (!payload.TryGetProperty(member, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(member));

            return null;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(member, declared, "a non-empty string"));

            return null;
        }

        string text = declared.GetString()!;

        if (string.IsNullOrWhiteSpace(text))
        {
            violations.Add($"the member '{member}' is empty, and it names something");

            return null;
        }

        return text;
    }

    /// <summary>Reads the overflow policy a bounded ingress declares.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="member">The member name.</param>
    /// <param name="violations">The report under construction, appended to when the member is wrong.</param>
    /// <returns>The policy, or <see cref="OverflowPolicy.Backpressure"/> when the member is wrong.</returns>
    /// <remarks>
    /// The default on a failed read is the backpressuring policy for the same reason a
    /// <see cref="BufferOptions"/> defaults to it: it is the policy that loses nothing, and a read that
    /// reported a violation is never used to build a declaration anyway.
    /// </remarks>
    internal static OverflowPolicy ReadPolicy(JsonElement payload, string member, List<string> violations)
    {
        if (!payload.TryGetProperty(member, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(member));

            return OverflowPolicy.Backpressure;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(member, declared, "one of five policy names"));

            return OverflowPolicy.Backpressure;
        }

        if (!LocalBufferParameters.TryParse(declared.GetString()!, out OverflowPolicy policy))
        {
            violations.Add(
                $"the member '{member}' is '{declared.GetString()}', and an overflow policy is one of 'backpressure', 'drop-oldest', 'drop-newest', 'drop-buffer', and 'fail'");

            return OverflowPolicy.Backpressure;
        }

        return policy;
    }
}
