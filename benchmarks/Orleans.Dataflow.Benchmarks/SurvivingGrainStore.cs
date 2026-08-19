using System.Collections.Concurrent;
using System.Globalization;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// The store the recovery scenario keeps the coordinator's register in: one that outlives any silo.
/// </summary>
/// <remarks>
/// <para>
/// Orleans' own memory grain storage cannot serve here, and the reason is structural rather than a matter
/// of degree: that provider keeps its data in <c>MemoryStorageGrain</c> activations — ordinary grains,
/// placed across the cluster like any others — so killing a silo takes part of the store with it, and a
/// coordinator whose register vanished issues epoch one all over again. What a deployment does instead is
/// put a real store behind the provider name, and this is that store, in-process.
/// </para>
/// <para>
/// A store and not a stand-in: state is round-tripped through the silo's own <see cref="Serializer"/>, so
/// writing does not alias what the grain still holds, and an ETag is compared on every write and refused
/// with <see cref="InconsistentStateException"/> when it does not match, which is the primitive the
/// coordinator's fencing is built on.
/// </para>
/// <para>
/// One instance is shared by every silo of the cluster. The silos are in one process, so an object they all
/// reference is as external to any one of them as a database would be, and it survives all of them.
/// </para>
/// </remarks>
internal sealed class SurvivingGrainStore
{
    private readonly ConcurrentDictionary<StateKey, StoredState> _states = new();

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
    private sealed class Storage(SurvivingGrainStore store, Serializer serializer) : IGrainStorage
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
