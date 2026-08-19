using System.Globalization;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// The probe that answers whether Orleans delivers pipelined calls between one caller and one callee in the
/// order they were sent.
/// </summary>
/// <remarks>
/// <para>
/// This was the open question the keyed adapter's credit protocol turned on, and it was not answerable from
/// the documentation: Orleans states no pairwise message-ordering guarantee between activations, and
/// <c>[Unordered]</c> has been a no-op since 7.x, so "ordered" is neither promised nor refusable. The
/// capability matrix nevertheless promises per-key ordered awaited replies, so the adapter had to be built
/// on something a cluster actually does rather than on something the docs left open.
/// </para>
/// <para>
/// <b>What the probe measures.</b> A caller pumps a run of sequenced calls at one callee without awaiting
/// between them, so several are outstanding at once, and the callee — non-reentrant, yielding once per turn
/// so its mailbox really fills — records the order they reached it. Three shapes are asked: a cold callee
/// that the first call has to activate, a warm one, and a caller that is not a grain at all, which is the
/// shape the adapter itself has because a run executes beside the grains of its silo rather than on one of
/// their turns.
/// </para>
/// <para>
/// <b>The answer, and what was built because of it.</b> Order does not hold. Pipelined calls arrive badly
/// out of order, in every round, from a grain caller and from a client caller alike, inside a single
/// in-process silo where every hop is local delivery — on the run that decided this, the first of two
/// hundred arrivals from a grain caller was the fourteenth call sent. So the keyed stage keeps exactly one
/// call in flight per key: the next element of a key is not sent until the previous one has replied, and
/// the ordering the capability matrix promises is a property of the adapter's own credit protocol rather
/// than of the transport. A per-key window greater than one was therefore never a legal option, and the
/// payload has no member for one.
/// </para>
/// <para>
/// <b>Why the reordering is measured and not asserted.</b> Every assertion here is about a permutation
/// rather than a sequence — each call arrives exactly once, whatever order it arrives in — because that is
/// the property the credit accounting rests on and the only one anything promises. The reordering itself is
/// counted and not asserted, and the reason is worth stating: <b>an absence of guarantee cannot be asserted
/// by observing it</b>. Orleans does not promise ordering, and it does not promise disorder either, so a
/// test demanding that some round arrive out of order is demanding a scheduling accident. It was asserted
/// once, with a margin computed from a measured per-round rate — and the margin was measured on one machine
/// and did not transfer: on other hardware every round arrived in order and a green build turned red for a
/// reason its own message described as "not a defect". A test whose failure is documented as harmless must
/// not be able to fail a build.
/// </para>
/// <para>
/// What was lost with the assertion is a canary for an Orleans version that begins to order pipelined
/// calls, and it is worth being precise about how little that was: the keyed stage keeps one call in flight
/// per key, so its ordering is a property of its own credit protocol and holds whichever way the transport
/// behaves. The canary guarded a note rather than a behaviour.
/// </para>
/// <para>
/// <b>What this does not prove.</b> One silo, so every hop here is local delivery and no connection is ever
/// re-established mid-run; a cluster whose caller and callee are on different silos can only be worse, which
/// is why the adapter's ordering does not depend on the answer either way. It also proves nothing about
/// ordering under a silo failure, which is failover's question and not this one's.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class KeyedOrderingProbeTests(DataflowCluster cluster)
{
    /// <summary>How many calls one probe pumps.</summary>
    /// <remarks>
    /// Enough that the callee's mailbox is deep for most of the run and few enough that the whole probe is
    /// a fraction of a second. A reordering that needed thousands of messages to appear would still be a
    /// reordering, which is the other reason the adapter does not rest on the answer.
    /// </remarks>
    private const int Calls = 200;

    /// <summary>How many rounds the measurement is taken over.</summary>
    /// <remarks>
    /// Twenty is enough for the count to say something about the machine it ran on, and small enough that
    /// the whole probe stays a fraction of a second. Nothing depends on the number any more: it sets the
    /// resolution of a measurement rather than the margin of an assertion, which is what it used to be and
    /// what made this file fail on hardware whose scheduler behaves differently from the one the margin was
    /// computed on.
    /// </remarks>
    private const int Rounds = 20;

    /// <summary>Gets the token that cancels a hung test rather than letting it block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task PipelinedCallsFromOneGrainToAColdCalleeAllArriveExactlyOnce()
    {
        // Cold on purpose: the first call is what activates the callee, and the rest arrive while the
        // activation is still being created. That is the moment in an activation's life with the most
        // machinery between a send and a turn, so it is the one worth asking about.
        (List<int> replies, List<int> arrivals) = await PumpFromGrainAsync("cold");

        AssertEveryCallArrivedExactlyOnce(replies, arrivals);
    }

    [Fact]
    public async Task PipelinedCallsFromOneGrainToAWarmCalleeAllArriveExactlyOnce()
    {
        string callee = string.Create(CultureInfo.InvariantCulture, $"warm-{Guid.NewGuid():N}");

        // One call first, awaited, so the callee is activated and its directory entry is cached before the
        // run that is being measured starts.
        _ = await cluster.Cluster.Client.GetGrain<IOrderingProbeCalleeGrain>(callee).ReceiveAsync(-1);

        List<int> replies = await cluster.Cluster.Client
            .GetGrain<IOrderingProbeCallerGrain>("probe-caller")
            .PumpAsync(callee, Calls);
        List<int> arrivals = await cluster.Cluster.Client
            .GetGrain<IOrderingProbeCalleeGrain>(callee)
            .ArrivalsAsync();

        // The warm-up call is part of the record and is not part of the run being measured.
        Assert.Equal(-1, arrivals[0]);

        AssertEveryCallArrivedExactlyOnce(replies, [.. arrivals[1..]]);
    }

    [Fact]
    public async Task PipelinedCallsFromOutsideAnyGrainAllArriveExactlyOnce()
    {
        // The shape the keyed adapter actually has. A run's engine executes on dedicated threads beside the
        // grains of its silo, so the caller is not an activation and its sends are not ordered by a turn.
        (List<int> replies, List<int> arrivals) = await PumpFromClientAsync("client");

        AssertEveryCallArrivedExactlyOnce(replies, arrivals);
    }

    [Fact]
    public async Task PipelinedCallsArriveExactlyOnceInEveryRoundHoweverTheyAreOrdered()
    {
        // Both shapes are asked because the adapter's caller is a client rather than a grain, and a runtime
        // could plausibly treat the two differently.
        // What is asserted is the permutation, in every round of both shapes: whatever order the calls
        // arrive in, each one arrives once. That is the property the credit accounting rests on, and unlike
        // the ordering it is a promise rather than an observation. Each round checks it and the count comes
        // back, so a round lost to an early return or a swallowed failure fails here rather than passing
        // quietly.
        Assert.Equal(Rounds, await RoundsCheckedAsync(async label => (await PumpFromGrainAsync(label)).Arrivals));
        Assert.Equal(Rounds, await RoundsCheckedAsync(async label => (await PumpFromClientAsync(label)).Arrivals));
    }

    [Fact]
    public async Task HoldingOneCallInFlightIsWhatMakesArrivalsFollowSendOrder()
    {
        // The positive control, and the mechanism the keyed adapter is built on. The same caller, the same
        // callee, the same number of calls — and each one awaited before the next is sent, so there is never
        // a second message whose order anything has to keep. This is what the stage's per-key credit does,
        // written out in a test so the claim is demonstrated rather than argued.
        string callee = string.Create(CultureInfo.InvariantCulture, $"serial-{Guid.NewGuid():N}");
        IOrderingProbeCalleeGrain target = cluster.Cluster.Client.GetGrain<IOrderingProbeCalleeGrain>(callee);

        for (int sequence = 0; sequence < Calls; sequence++)
        {
            _ = await target.ReceiveAsync(sequence).WaitAsync(Token);
        }

        Assert.Equal(Enumerable.Range(0, Calls), await target.ArrivalsAsync());
    }

    /// <summary>Runs several rounds and says whether any of them arrived out of send order.</summary>
    /// <param name="round">Pumps one round and returns the arrivals.</param>
    /// <returns><see langword="true"/> when at least one round was reordered.</returns>
    private static async Task<int> RoundsCheckedAsync(Func<string, Task<List<int>>> round)
    {
        int checkedRounds = 0;

        for (int attempt = 0; attempt < Rounds; attempt++)
        {
            List<int> arrivals = await round(string.Create(CultureInfo.InvariantCulture, $"verdict-{attempt}"));

            // The permutation, which is the part that is a promise: every call arrived, and each of them
            // once, whatever order they came in.
            Assert.Equal(Enumerable.Range(0, Calls), [.. arrivals.Order()]);

            checkedRounds++;
        }

        return checkedRounds;
    }

    /// <summary>Pumps one run of calls from a caller grain at a callee nothing else addresses.</summary>
    /// <param name="label">What this round is called, for a callee key nothing else uses.</param>
    /// <returns>The sequence numbers the replies carried and the order the calls arrived in.</returns>
    private async Task<(List<int> Replies, List<int> Arrivals)> PumpFromGrainAsync(string label)
    {
        string callee = string.Create(CultureInfo.InvariantCulture, $"{label}-{Guid.NewGuid():N}");
        List<int> replies = await cluster.Cluster.Client
            .GetGrain<IOrderingProbeCallerGrain>("probe-caller")
            .PumpAsync(callee, Calls);
        List<int> arrivals = await cluster.Cluster.Client
            .GetGrain<IOrderingProbeCalleeGrain>(callee)
            .ArrivalsAsync();

        return (replies, arrivals);
    }

    /// <summary>Pumps one run of calls from the client at a callee nothing else addresses.</summary>
    /// <param name="label">What this round is called, for a callee key nothing else uses.</param>
    /// <returns>The sequence numbers the replies carried and the order the calls arrived in.</returns>
    private async Task<(List<int> Replies, List<int> Arrivals)> PumpFromClientAsync(string label)
    {
        string callee = string.Create(CultureInfo.InvariantCulture, $"{label}-{Guid.NewGuid():N}");
        IOrderingProbeCalleeGrain target = cluster.Cluster.Client.GetGrain<IOrderingProbeCalleeGrain>(callee);
        List<Task<int>> pending = [];

        for (int sequence = 0; sequence < Calls; sequence++)
        {
            pending.Add(target.ReceiveAsync(sequence));
        }

        List<int> replies = [.. await Task.WhenAll(pending).WaitAsync(Token)];

        return (replies, await target.ArrivalsAsync());
    }

    /// <summary>Asserts that every call reached the callee once and that every reply came back.</summary>
    /// <param name="replies">The sequence numbers the replies carried.</param>
    /// <param name="arrivals">The sequence numbers in arrival order.</param>
    /// <remarks>
    /// A permutation and not a sequence, deliberately. What the credit accounting needs is that a call in
    /// flight is eventually one reply and never zero or two; what it does not need, and therefore does not
    /// assert here, is the order — the adapter earns that itself by keeping one call in flight per key.
    /// </remarks>
    private static void AssertEveryCallArrivedExactlyOnce(List<int> replies, List<int> arrivals)
    {
        Assert.Equal(Calls, replies.Count);
        Assert.Equal(Calls, arrivals.Count);
        Assert.Equal(Enumerable.Range(0, Calls), [.. replies.Order()]);
        Assert.Equal(Enumerable.Range(0, Calls), [.. arrivals.Order()]);
    }
}
