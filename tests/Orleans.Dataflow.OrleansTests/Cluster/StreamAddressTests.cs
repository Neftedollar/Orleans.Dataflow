using Orleans.Dataflow.Adapters;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What <see cref="OrleansStreamAddress.Create(string, string, string)"/> accepts and what it refuses.
/// </summary>
/// <remarks>
/// <para>
/// The address imposes no grammar on purpose — Orleans does not, and a namespace or key that a working
/// deployment already uses must not stop working because this library invented a rule. What it does check
/// is that each part is text at all, and that check was added because the alternative was measured: a part
/// carrying an unpaired surrogate has no exact UTF-8 form, so the JSON writer that puts the address into a
/// graph document substituted <c>U+FFFD</c> for it. Two distinct ill-formed keys collapsed to the same
/// payload bytes and addressed one stream, and the document named a key that was not the caller's.
/// </para>
/// <para>
/// So the pair of claims below is the contract: exotic text is text and passes untouched, and ill-formed
/// text is refused before anything is built from it, with the part named so the caller knows which of the
/// three arguments to look at. No cluster and therefore no collection — nothing here deploys a silo.
/// </para>
/// </remarks>
public sealed class StreamAddressTests
{
    /// <summary>A high surrogate with nothing following it, which is not text.</summary>
    private const string UnpairedHighSurrogate = "orders-\ud83d";

    /// <summary>A low surrogate with nothing before it, which is not text either.</summary>
    private const string UnpairedLowSurrogate = "\udc00-orders";

    [Fact]
    public void EachPartRefusesAnUnpairedSurrogateAndNamesItself()
    {
        (string Parameter, string Provider, string Namespace, string Key)[] cases =
        [
            ("provider", UnpairedHighSurrogate, "orders", "17"),
            ("streamNamespace", "memory", UnpairedHighSurrogate, "17"),
            ("key", "memory", "orders", UnpairedHighSurrogate),
            ("provider", UnpairedLowSurrogate, "orders", "17"),
            ("streamNamespace", "memory", UnpairedLowSurrogate, "17"),
            ("key", "memory", "orders", UnpairedLowSurrogate),
        ];

        foreach ((string parameter, string provider, string streamNamespace, string key) in cases)
        {
            ArgumentException refused = Assert.Throws<ArgumentException>(
                parameter,
                () => OrleansStreamAddress.Create(provider, streamNamespace, key));

            // The parameter name is what an exception carries; the message says it too, because a caller
            // reading a log line has the message and not the parameter.
            Assert.Contains(parameter, refused.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void WellFormedExoticTextIsAcceptedUnchanged()
    {
        (string Provider, string Namespace, string Key)[] cases =
        [
            ("memory", "orders", "17"),
            ("你好", "你好-ns", "你好-key"),
            ("\U0001F600", "orders-\U0001F600", "\U0001F600\U0001F600"),

            // The replacement character as a literal is ordinary text and must stay ordinary text: it is
            // what the old substitution produced, and refusing it would refuse a key somebody may hold.
            ("memory", "orders-\ufffd", "\ufffd"),
            ("mémoire", "commandes", "clé-17"),
        ];

        foreach ((string provider, string streamNamespace, string key) in cases)
        {
            OrleansStreamAddress address = OrleansStreamAddress.Create(provider, streamNamespace, key);

            // Unchanged means unchanged: the same code units come back out, not a normalized or repaired
            // form of them.
            Assert.Equal(provider, address.Provider, StringComparer.Ordinal);
            Assert.Equal(streamNamespace, address.Namespace, StringComparer.Ordinal);
            Assert.Equal(key, address.Key, StringComparer.Ordinal);
        }
    }

    [Fact]
    public void TwoDistinctIllFormedKeysNoLongerCollapseIntoOneAddress()
    {
        // The defect stated as a test rather than as a sentence: these two keys differ, and both used to be
        // written into a document as the same single replacement character. Neither address exists now, so
        // there is nothing left to alias.
        Assert.NotEqual(UnpairedHighSurrogate, UnpairedLowSurrogate, StringComparer.Ordinal);

        _ = Assert.Throws<ArgumentException>(
            "key",
            () => OrleansStreamAddress.Create("memory", "orders", "\ud83d"));
        _ = Assert.Throws<ArgumentException>(
            "key",
            () => OrleansStreamAddress.Create("memory", "orders", "\udc00"));
    }

    [Fact]
    public void TheGuidOverloadStillRefusesAnIllFormedProviderOrNamespace()
    {
        // The guid overload delegates to the string one, so the key it builds is always well formed and the
        // other two parts are checked exactly as they are above. Asserted rather than assumed, because a
        // future edit could give the overload its own path.
        _ = Assert.Throws<ArgumentException>(
            "provider",
            () => OrleansStreamAddress.Create(UnpairedHighSurrogate, "orders", Guid.NewGuid()));
        _ = Assert.Throws<ArgumentException>(
            "streamNamespace",
            () => OrleansStreamAddress.Create("memory", UnpairedLowSurrogate, Guid.NewGuid()));
    }
}
