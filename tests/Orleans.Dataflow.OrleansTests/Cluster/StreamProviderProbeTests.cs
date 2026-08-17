using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Runtime;
using Orleans.Streams;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What the memory stream provider actually reports about itself.
/// </summary>
/// <remarks>
/// The research notes named one unknown for this phase and told it to be probed rather than guessed: the
/// memory provider's <c>IsRewindable</c> is undocumented. This is that probe, kept as a test so that the
/// answer stays true rather than becoming a sentence in a document that nobody re-runs. The assertion
/// message carries the value, so a run of the suite records it whether or not the assertion is the one that
/// fails.
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class StreamProviderProbeTests(DataflowCluster cluster)
{
    [Fact]
    public void TheMemoryStreamProviderReportsThatItIsRewindable()
    {
        IStreamProvider provider = cluster.Cluster.Silos[0].ServiceProvider
            .GetRequiredKeyedService<IStreamProvider>(AdapterVocabulary.StreamProvider);

        bool rewindable = provider.IsRewindable;

        Assert.True(
            rewindable,
            string.Create(
                CultureInfo.InvariantCulture,
                $"The memory stream provider registered by AddMemoryStreams reports IsRewindable = {rewindable}. Orleans 10.2.2, provider implementation {provider.GetType().FullName}. Phase 2 exposes no rewind API regardless: a cursor without a checkpoint owner is a foot-gun, and the source subscribes without a sequence token, so a run reads what arrives after it subscribed and never history."));
    }

    [Fact]
    public void AGuidKeyedAddressAddressesTheSameStreamAsItsTextKeyedForm()
    {
        Guid key = Guid.NewGuid();

        OrleansStreamAddress fromGuid = OrleansStreamAddress.Create("p", "ns", key);
        OrleansStreamAddress fromText = OrleansStreamAddress.Create(
            "p",
            "ns",
            key.ToString("N", CultureInfo.InvariantCulture));

        // The address is only half the claim; the other half is that Orleans itself agrees, which is why
        // the two stream identities are compared rather than only the two addresses.
        Assert.Equal(fromText, fromGuid);
        Assert.Equal(
            StreamId.Create(fromText.Namespace, fromText.Key),
            StreamId.Create("ns", key));
    }
}
