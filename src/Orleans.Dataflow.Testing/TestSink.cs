using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// The sinks a test closes a graph with.
/// </summary>
/// <remarks>
/// <para>
/// A test sink is an ordinary sink of the local vocabulary: it closes any chain of the element type it
/// consumes, and the document it produces validates against the stage catalog like every other. What is
/// not ordinary is what it does with an element, and that is the whole point — every other sink consumes
/// as fast as it is fed, so no other sink can be made to stand still while a test measures what the graph
/// in front of it managed to produce.
/// </para>
/// <para>
/// The factories live on a non-generic companion class beside <see cref="Sink"/>, so that the type
/// argument is written only where it cannot be inferred.
/// </para>
/// </remarks>
public static class TestSink
{
    /// <summary>Creates a sink that delivers an element only when a test asks for one.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <param name="controlName">The author-stable name to expose the probe under.</param>
    /// <returns>The sink.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="controlName"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="controlName"/> is not a valid result slot identifier.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The probe is a per-run control reached by name, exactly as an ingress queue is, and it resolves at
    /// the start of a run for the same reason: a test receives from a run that is already running. It is
    /// declared here rather than handed back by <c>To</c> because the sink is composed into the graph
    /// before the graph is closed, and because a control is not a result — the graph this closes declares
    /// no result at all.
    /// </para>
    /// <para>
    /// The sink declares no capacity, because it holds nothing: an element that reaches it waits on the
    /// run's own thread until a receiver takes it. That is what makes a run in front of it advance exactly
    /// as far as its declared bounds allow and no further, which is the measurement the probe exists for.
    /// </para>
    /// </remarks>
    public static Sink<T> Probe<T>(string controlName)
    {
        ResultSlotId control = LocalOptionGuard.SlotName(controlName, nameof(controlName));
        Func<LocalSinkProbe, object> facade = static probe => new SinkProbe<T>(probe);

        return new Sink<T>(LocalStageChain.Of(
            LocalStageDescriptor.SinkProbe(control, typeof(ISinkProbe<T>), facade)));
    }
}
