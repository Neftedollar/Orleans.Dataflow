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
}
