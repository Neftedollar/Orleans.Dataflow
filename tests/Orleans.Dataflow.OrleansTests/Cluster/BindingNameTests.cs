using Orleans.Dataflow.Adapters;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a binding name and a broadcast channel part have to be before a document can carry them.
/// </summary>
/// <remarks>
/// <para>
/// A registration name is what a document stores in place of a CLR member, and it reaches that document as
/// a JSON string. A JSON string writer has no exact form for an unpaired surrogate — it substitutes the
/// replacement character — so a name carrying one used to be stored as a different name than the one
/// registered, and two distinct such names collapsed into one. Refusing at the factory is this library's
/// standing answer: before the run exists, naming the argument that was wrong.
/// </para>
/// <para>
/// <b>The consistency is the API property, which is why every entry point is listed.</b> A surface where a
/// stream address refuses ill-formed text and a bridge name silently mangles it is worse than either
/// consistent extreme, because a caller cannot learn one rule from one call. These tests exist to make the
/// list complete rather than to test one validator twice — the exhaustive exotic-text cases live in
/// <c>StreamAddressTests</c> and <c>JsonTextTests</c>, and one acceptance case per validator is enough here.
/// </para>
/// <para>
/// No cluster and therefore no collection: nothing here deploys a silo.
/// </para>
/// </remarks>
public sealed class BindingNameTests
{
    /// <summary>A high surrogate with nothing following it, which is not text.</summary>
    private const string NotText = "orders-\ud83d";

    /// <summary>Exotic text that is well formed, and therefore a name a deployment may keep using.</summary>
    private const string ExoticText = "orders-\U0001F600-你好";

    /// <summary>The contract every binding below declares, which is never the thing under test.</summary>
    private static ElementContract<string> Contract => ElementContract.For<string>("binding-name-probe", 1);

    [Fact]
    public void EveryNamedOrleansBindingFactoryRefusesANameThatIsNotWellFormedText()
    {
        (string Factory, Func<string, object> Create)[] factories =
        [
            ("GrainCallBinding", name => GrainCallBinding.Create<string, string>(
                name,
                Contract,
                Contract,
                static (_, element, _) => Task.FromResult(element))),
            ("KeyedGrainCallBinding", name => KeyedGrainCallBinding.Create<string, string>(
                name,
                Contract,
                Contract,
                static element => element,
                static (_, element, _) => Task.FromResult(element))),
            ("GrainCallSinkBinding", name => GrainCallSinkBinding.Create<string>(
                name,
                Contract,
                static (_, _, _) => Task.CompletedTask)),
            ("GrainEnumerableBinding", name => GrainEnumerableBinding.Create<string>(
                name,
                Contract,
                static (_, _) => Empty())),
            ("ObserverBridgeBinding", name => ObserverBridgeBinding.Create(name, Contract)),
        ];

        foreach ((string factory, Func<string, object> create) in factories)
        {
            ArgumentException refused = Assert.Throws<ArgumentException>("name", () => create(NotText));

            // The parameter name is what an exception carries; the message says it too, because a caller
            // reading a log line has the message and not the parameter.
            Assert.Contains("name", refused.Message, StringComparison.Ordinal);
            Assert.NotNull(create(ExoticText));

            // Named in the assertion so a failure says which of the five moved.
            Assert.False(string.IsNullOrEmpty(factory));
        }
    }

    [Fact]
    public void ABroadcastChannelRefusesAProviderOrAChannelThatIsNotWellFormedText()
    {
        // The one pair that reaches a payload through neither a named binding nor a stream address, which
        // is exactly why it was the last one still substituting.
        BroadcastElementBinding<string> element = BroadcastElementBinding.Create(Contract);
        BufferOptions ingress = new() { Capacity = 8, OverflowPolicy = OverflowPolicy.DropOldest };

        ArgumentException provider = Assert.Throws<ArgumentException>(
            "provider",
            () => OrleansStages.BroadcastSourceParameters(element, NotText, "orders", ingress));
        ArgumentException channel = Assert.Throws<ArgumentException>(
            "channel",
            () => OrleansStages.BroadcastSourceParameters(element, "memory", NotText, ingress));

        Assert.Contains("provider", provider.Message, StringComparison.Ordinal);
        Assert.Contains("channel", channel.Message, StringComparison.Ordinal);

        Assert.False(
            OrleansStages.BroadcastSourceParameters(element, ExoticText, ExoticText, ingress).IsDefault);
    }

    [Fact]
    public void TheBroadcastChannelAddressHelperRefusesTheSameStringsAsTheAddressItBuilds()
    {
        // It delegates rather than repeating the check, so this asserts the delegation actually happens
        // and reports under the helper's own parameter names.
        _ = Assert.Throws<ArgumentException>(
            "provider",
            () => OrleansStages.BroadcastSourceChannel(NotText, "orders"));
        _ = Assert.Throws<ArgumentException>(
            "key",
            () => OrleansStages.BroadcastSourceChannel("memory", NotText));

        Assert.False(OrleansStages.BroadcastSourceChannel(ExoticText, ExoticText).IsDefault);
    }

    /// <summary>An enumeration that yields nothing, for a binding whose opener is never the thing tested.</summary>
    /// <returns>The empty sequence.</returns>
    private static async IAsyncEnumerable<string> Empty()
    {
        await Task.CompletedTask;

        yield break;
    }
}
