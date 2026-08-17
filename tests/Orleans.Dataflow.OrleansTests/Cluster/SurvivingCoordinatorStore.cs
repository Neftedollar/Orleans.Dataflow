using System.Collections.Concurrent;
using System.Globalization;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// The store the multi-silo tests keep the coordinator's register in: one that outlives any silo, and that
/// implements optimistic concurrency the way <see cref="Grains.OrleansDataflowStorage"/> says a coordinator
/// store must.
/// </summary>
/// <remarks>
/// <para>
/// It exists because Orleans' own memory grain storage cannot answer the question these tests ask. That
/// provider keeps its data in <c>MemoryStorageGrain</c> activations — ordinary grains, ten of them by
/// default, placed across the cluster like any others — so killing a silo takes part of the store with it.
/// Measured rather than assumed: in a three-silo cluster with thirty pipelines, killing the silo that held
/// four of the ten storage grains reset six of the thirty coordinators' registers to empty, and their next
/// run was issued epoch one all over again. A test that asserted "the epoch keeps rising across a kill"
/// against that store would be asserting a coin flip. A production deployment puts a real store behind the
/// provider name precisely so that a silo dying is not a store dying, and this class is that store,
/// in-process.
/// </para>
/// <para>
/// It is a store and not a mock. State is round-tripped through the silo's own <see cref="Serializer"/>,
/// so a grain that mutates the object it wrote does not thereby change what was persisted, and an ETag is
/// compared on every write and refused with <see cref="InconsistentStateException"/> when it does not
/// match — which is the primitive the coordinator's fencing is built on, not a courtesy.
/// </para>
/// <para>
/// One instance is shared by every silo of one cluster. That is exactly what "external store" means here:
/// the silos are in one process, so an object they all hold a reference to is as external to any one of
/// them as a database would be, and it survives all three of them dying.
/// </para>
/// </remarks>
internal sealed class SurvivingCoordinatorStore
{
    private readonly ConcurrentDictionary<StateKey, StoredState> _states = new();

    /// <summary>Reports the ETag one grain's state currently carries in the store.</summary>
    /// <param name="grain">The grain whose state to look up.</param>
    /// <param name="stateName">The name of the state, as its <c>PersistentState</c> attribute declares it.</param>
    /// <returns>The ETag as a number, or zero when the store holds nothing for that grain.</returns>
    /// <remarks>
    /// A number rather than the opaque text an ETag usually is, because these tests assert a lineage: an
    /// ETag that rose from four to five across a silo dying says the fresh activation continued the
    /// sequence, and one that fell back to one says it started over. Only a store can answer that, which
    /// is why the store is the thing asked.
    /// </remarks>
    internal long Version(GrainId grain, string stateName) =>
        _states.TryGetValue(new StateKey(grain, stateName), out StoredState? stored)
            ? stored.Version
            : 0L;

    /// <summary>Writes one grain's state behind its back, as a competing activation's write would.</summary>
    /// <param name="grain">The grain whose state to supersede.</param>
    /// <param name="stateName">The name of the state.</param>
    /// <exception cref="InvalidOperationException">The store holds no state for that grain yet.</exception>
    /// <remarks>
    /// <para>
    /// The only honest way to produce a real ETag conflict against a live coordinator. Orleans will not let
    /// two activations of one grain exist, so a test cannot stage the split brain the fencing defends
    /// against; what it can do is put the store into exactly the state that split brain would leave it in —
    /// the same bytes under a newer ETag, written by somebody else — and then watch the live activation
    /// discover that at its next write.
    /// </para>
    /// <para>
    /// The payload is deliberately unchanged. What is being tested is the ETag comparison and the runtime's
    /// documented reaction to losing it, not whether a different value survives the round trip.
    /// </para>
    /// </remarks>
    internal void Supersede(GrainId grain, string stateName)
    {
        StateKey key = new(grain, stateName);

        if (!_states.TryGetValue(key, out StoredState? stored))
        {
            throw new InvalidOperationException(
                $"The store holds no '{stateName}' state for the grain '{grain}', so there is nothing for a competing writer to supersede. A coordinator writes its register on its first accepted start; supersede it after that and not before.");
        }

        _states[key] = new StoredState(stored.Payload, stored.Version + 1);
    }

    /// <summary>Builds the storage provider one silo registers over this store.</summary>
    /// <param name="serializer">The silo's serializer, which is what makes a write a copy.</param>
    /// <returns>The provider.</returns>
    internal IGrainStorage Provider(Serializer serializer) => new Storage(this, serializer);

    /// <summary>What the store holds for one grain's state.</summary>
    /// <param name="Payload">The serialized state.</param>
    /// <param name="Version">The version the ETag is the text of.</param>
    private sealed record StoredState(byte[] Payload, long Version)
    {
        /// <summary>Gets the ETag a reader is handed and a writer must present.</summary>
        internal string ETag { get; } = Version.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>What one state is addressed by.</summary>
    /// <param name="Grain">The grain owning the state.</param>
    /// <param name="StateName">The state's declared name, since one grain may hold several.</param>
    private readonly record struct StateKey(GrainId Grain, string StateName);

    /// <summary>The Orleans-facing face of the store.</summary>
    /// <param name="store">The store the silos share.</param>
    /// <param name="serializer">The silo's serializer.</param>
    /// <remarks>
    /// One of these per silo, all of them over one store. Nothing here is asynchronous in truth — the
    /// store is a dictionary in this process — and none of it pretends to be: every method completes
    /// synchronously, which keeps a storage call from being a scheduling point that a failover test would
    /// then have to reason about.
    /// </remarks>
    private sealed class Storage(SurvivingCoordinatorStore store, Serializer serializer) : IGrainStorage
    {
        /// <inheritdoc/>
        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            ArgumentNullException.ThrowIfNull(grainState);

            if (store._states.TryGetValue(new StateKey(grainId, stateName), out StoredState? stored))
            {
                grainState.State = serializer.Deserialize<T>(stored.Payload);
                grainState.ETag = stored.ETag;
                grainState.RecordExists = true;
            }
            else
            {
                // The state the bridge created is left exactly as it is, which is what a store that holds
                // nothing has to do: replacing it would invent a value the grain never wrote.
                grainState.ETag = null;
                grainState.RecordExists = false;
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            ArgumentNullException.ThrowIfNull(grainState);

            StateKey key = new(grainId, stateName);
            StoredState? stored = store._states.TryGetValue(key, out StoredState? found) ? found : null;

            Check(stateName, grainId, grainState.ETag, stored);

            StoredState next = new(serializer.SerializeToArray(grainState.State), (stored?.Version ?? 0L) + 1L);

            store._states[key] = next;
            grainState.ETag = next.ETag;
            grainState.RecordExists = true;

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            ArgumentNullException.ThrowIfNull(grainState);

            StateKey key = new(grainId, stateName);
            StoredState? stored = store._states.TryGetValue(key, out StoredState? found) ? found : null;

            Check(stateName, grainId, grainState.ETag, stored);

            _ = store._states.TryRemove(key, out _);
            grainState.ETag = null;
            grainState.RecordExists = false;

            return Task.CompletedTask;
        }

        /// <summary>Refuses a write whose ETag is not the one the store holds.</summary>
        /// <param name="stateName">The state being written.</param>
        /// <param name="grainId">The grain writing it.</param>
        /// <param name="presented">The ETag the writer presented, which is null before its first write.</param>
        /// <param name="stored">What the store holds, or null when it holds nothing.</param>
        /// <exception cref="InconsistentStateException">The presented ETag is not the stored one.</exception>
        private static void Check(string stateName, GrainId grainId, string? presented, StoredState? stored)
        {
            string? current = stored?.ETag;

            if (string.Equals(presented, current, StringComparison.Ordinal))
            {
                return;
            }

            throw new InconsistentStateException(
                $"The write of '{stateName}' for the grain '{grainId}' presents the ETag '{presented ?? "<none>"}' and the store holds '{current ?? "<none>"}'. Somebody else wrote this state after this writer read it.",
                current ?? string.Empty,
                presented ?? string.Empty);
        }
    }
}
