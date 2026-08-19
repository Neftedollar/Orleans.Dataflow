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
/// <b>Why these assertions and not others.</b> The exactly-once tests assert a permutation rather than a
/// sequence — that is the property the credit accounting rests on, and it is true whatever the ordering
/// turns out to be. The verdict test asserts the reordering itself, so that an Orleans version which starts
/// ordering pipelined calls fails a test and sends somebody back to these notes, rather than silently making
/// a design decision look arbitrary. Neither failure would be a defect in this repository.
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

    /// <summary>How many rounds the verdict is taken over.</summary>
    /// <remarks>
    /// One reordered round is enough to pass, because the claim being recorded is "this can happen" and a
    /// probe that demanded it happen every time would be asserting a stronger fact than was observed. The
    /// count is set by the failure side rather than the success side: a single round arrives in send order
    /// roughly two times in five — measured across suite runs, not assumed from the first session, where
    /// every round happened to reorder — so five rounds failed spuriously about once in seventy suite runs.
    /// Twenty puts the odds of all rounds landing in order below one in ten million, which is the margin a
    /// probe needs to mean "Orleans changed" rather than "the scheduler had a calm morning".
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
    public async Task PipelinedCallsDoNotArriveInTheOrderTheyWereSent()
    {
        // The verdict, asserted rather than merely recorded, so that a version of Orleans which begins to
        // order pipelined calls fails here and sends somebody to the notes above. Both shapes are asked
        // because the adapter's caller is a client rather than a grain, and a runtime could plausibly treat
        // the two differently.
        Assert.True(
            await AnyRoundReorderedAsync(async label => (await PumpFromGrainAsync(label)).Arrivals),
            $"A grain caller's {Calls} pipelined calls arrived in send order in all {Rounds} rounds. Orleans promises no such thing, so this is a change in observed behavior rather than a defect: re-read the keyed adapter's as-implemented notes before relying on it.");

        Assert.True(
            await AnyRoundReorderedAsync(async label => (await PumpFromClientAsync(label)).Arrivals),
            $"A client caller's {Calls} pipelined calls arrived in send order in all {Rounds} rounds. The same note applies: the keyed stage does not depend on it either way.");
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
    private static async Task<bool> AnyRoundReorderedAsync(Func<string, Task<List<int>>> round)
    {
        for (int attempt = 0; attempt < Rounds; attempt++)
        {
            List<int> arrivals = await round(string.Create(CultureInfo.InvariantCulture, $"verdict-{attempt}"));

            if (!arrivals.SequenceEqual(Enumerable.Range(0, Calls)))
            {
                return true;
            }
        }

        return false;
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
