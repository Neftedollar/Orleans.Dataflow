using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredJunctionFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// A provider for the fixture vocabulary, written the way a provider outside this repository would have to
/// write one.
/// </summary>
/// <remarks>
/// <para>
/// <b>Public surface only.</b> Everything this type touches — <see cref="IDataflowStageFactory"/>,
/// <see cref="DataflowStageRequest"/>, <see cref="DataflowStageRuntime"/>,
/// <see cref="DataflowRunTokens"/>, and <see cref="ILocalDataflowBuilder"/> on the host side — is public
/// API of the core package. The test assembly happens to be a friend of that package, which is why the
/// discipline is stated here and asserted separately: <c>RegisteredJunctionRunTests</c> checks that the
/// whole seam really is public, so "a provider could write this" is a fact about the package rather than
/// about this file's good behaviour.
/// </para>
/// <para>
/// One factory for the whole provider, dispatching on the stage reference, which is the registration shape
/// the seam asks for: a provider ships a vocabulary rather than a pile of entries, and a deployment that
/// registered half of it would discover the other half missing at the first element.
/// </para>
/// <para>
/// The junctions read their own payload. That is the point of a registered junction having one: what a
/// stage does is the provider's, what an occurrence configures is the document's, and two graphs differing
/// only in <c>mode</c> are two graphs with two fingerprints that really do behave differently.
/// </para>
/// </remarks>
internal sealed class RegisteredJunctionProvider : IDataflowStageFactory
{
    private readonly Dictionary<string, int> _built = [];
    private readonly Lock _gate = new();

    /// <summary>Gets the provider every fixture stage belongs to.</summary>
    internal static ProviderId Provider { get; } = ProviderId.Create(RegisteredFixtures.Provider);

    /// <summary>Gets how many times this factory was asked to build each node, keyed by node identifier.</summary>
    /// <value>
    /// One entry per node the seam resolved, counting the calls. The seam's contract is one call per node
    /// per materialization, and a junction is asked about twice while a plan is built — once to learn that
    /// it is a junction and once to build it — so this is what turns that contract into an assertion.
    /// </value>
    internal IReadOnlyDictionary<string, int> Built
    {
        get
        {
            lock (_gate)
            {
                return new Dictionary<string, int>(_built);
            }
        }
    }

    /// <inheritdoc/>
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        lock (_gate)
        {
            _built[request.Node.Id.Value] = _built.GetValueOrDefault(request.Node.Id.Value) + 1;
        }

        return request.Node.Stage.Stage.Value switch
        {
            "order-source" => DataflowStageRuntime.Source(_ => Orders()),
            "normalize" => DataflowStageRuntime.Element(
                static element => OrderDocument.FromEvent((OrderCreated)element!)),
            "enrich" => DataflowStageRuntime.Element(static element => element),
            "keys" => DataflowStageRuntime.Element(
                static element => new OrderKey(((OrderDocument)element!).OrderId)),
            "index-sink" or "durable-sink" => Counting(producesResult: false),
            "count-sink" or "key-count-sink" or "pair-count-sink" => Counting(producesResult: true),
            "split" or "spread" => Splitting(request) switch
            {
                SplitMode.Balance => DataflowStageRuntime.Balance(),

                // A deliberately wrong answer, reachable only from a payload that asks for it: two
                // projections for a junction whose document wires three legs. It exists so that the
                // planner's "a stage says one thing and the document says another" refusal can be
                // reached through the seam rather than only reasoned about.
                SplitMode.Halves =>
                    DataflowStageRuntime.Unzip([static element => element, static element => element]),
                _ => DataflowStageRuntime.Broadcast(),
            },
            "divide" => DataflowStageRuntime.Unzip(
            [
                static element => element,
                static element => new OrderKey(((OrderDocument)element!).OrderId),
            ]),
            "join" or "gather" => Joining(request) is JoinMode.Concat
                ? DataflowStageRuntime.Concat()
                : DataflowStageRuntime.Merge(),

            // A junction built for a stage the catalog declares as a flow. The catalog cannot catch this —
            // a specification describes ports and says nothing about what a factory will build — so the
            // planner is what has to, and this is the stage that proves it does.
            "enrich-miscast" => DataflowStageRuntime.Broadcast(),
            "pair" => DataflowStageRuntime.Zip(
                static columns => new OrderPair((OrderDocument)columns[0]!, (OrderKey)columns[1]!)),
            _ => throw new NotSupportedException(
                $"This build of the fixture provider does not implement the stage '{request.Node.Stage}'."),
        };
    }

    /// <summary>Builds the runtime of a terminal that counts what reaches it.</summary>
    /// <param name="producesResult">Whether a document may declare a result slot over it.</param>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// A seed factory rather than a seed, because the seam asks for one: a run's state has to be its own,
    /// and a boxed counter handed over as a value would be one object two runs both wrote into.
    /// </remarks>
    private static DataflowStageRuntime Counting(bool producesResult) =>
        DataflowStageRuntime.Terminal(
            static () => 0L,
            static (state, _) => (long)state! + 1L,
            finish: null,
            producesResult);

    /// <summary>Reads what a fan-out occurrence's payload declares.</summary>
    /// <param name="request">The node as the document declares it.</param>
    /// <returns>The declared mode.</returns>
    /// <exception cref="InvalidOperationException">The payload is not readable.</exception>
    /// <remarks>
    /// Through the vocabulary's own reader rather than through a parse of this factory's own, which is the
    /// half of the typed-parameter-builder pattern that lives on this side of the seam: the writer, the
    /// validator, and this all read one statement of what the payload is, so a member renamed in one place
    /// stops compiling in the other two. The refusal is unreachable by construction — the graph compiler ran
    /// the very same reader before a run was planned — and stated anyway, because a factory that defaulted
    /// instead would make an unreadable payload behave like a broadcast.
    /// </remarks>
    private static SplitMode Splitting(DataflowStageRequest request) =>
        JunctionModePayload.TryReadSplit(
            request.Node.Parameters,
            out SplitMode mode,
            out IReadOnlyList<string> violations)
            ? mode
            : throw Unreadable(request, violations);

    /// <summary>Reads what a fan-in occurrence's payload declares.</summary>
    /// <param name="request">The node as the document declares it.</param>
    /// <returns>The declared mode.</returns>
    /// <exception cref="InvalidOperationException">The payload is not readable.</exception>
    private static JoinMode Joining(DataflowStageRequest request) =>
        JunctionModePayload.TryReadJoin(
            request.Node.Parameters,
            out JoinMode mode,
            out IReadOnlyList<string> violations)
            ? mode
            : throw Unreadable(request, violations);

    /// <summary>Says that a node carries a payload this provider cannot read.</summary>
    /// <param name="request">The node.</param>
    /// <param name="violations">What the reader said was wrong with it.</param>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException Unreadable(
        DataflowStageRequest request,
        IReadOnlyList<string> violations) =>
        new($"The node '{request.Node.Id}', an occurrence of '{request.Node.Stage}', carries parameters this provider cannot read: {string.Join("; ", violations)}.");

    /// <summary>Opens one enumeration of the fixture's order events.</summary>
    /// <returns>The events, as the engine pulls them.</returns>
    /// <remarks>
    /// Asynchronous because every registered source is: the engine pulls it on the segment's own dedicated
    /// thread, which is what makes an awaited delivery ordinary rather than special.
    /// </remarks>
    private static async IAsyncEnumerable<object?> Orders()
    {
        foreach (OrderCreated order in OrderEvents)
        {
            yield return order;

            await Task.Yield();
        }
    }
}
