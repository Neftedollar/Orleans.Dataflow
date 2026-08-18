using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// A scope whose stages can hand their state over and take it back.
/// </summary>
/// <remarks>
/// <para>
/// The durable half of a checkpoint, and the one seam of the three that is an interface rather than a class:
/// the only thing that implements it is a stage, and what a checkpoint needs from that stage is these two
/// methods and nothing else. Naming the narrowing is what keeps the capture loop from having a
/// <see cref="LocalElementStage"/> in its hand and being tempted to do something with it.
/// </para>
/// <para>
/// Everything outside one of these <b>resets on resume</b>, and the reset is the documented contract rather
/// than a caveat: a scan outside a durable scope starts from its seed in the resumed run, and a test asserts
/// that by value.
/// </para>
/// <para>
/// <b>The state is a canonical value and that is the seam's requirement.</b> A checkpoint is bytes another
/// process reads, so no CLR type name and no serializer's opinion may enter here; a stage whose state cannot
/// be written down in that plane is refused inside a durable scope by name, rather than resuming into a
/// state that was quietly reset.
/// </para>
/// </remarks>
internal interface ILocalDurableState
{
    /// <summary>Hands over everything this scope's stages are holding.</summary>
    /// <returns>The state, as a canonical value this scope's own reader understands.</returns>
    /// <remarks>
    /// Asked on the capture loop's thread while the run is quiescent, which is what makes reading a stage's
    /// state safe without a lock: no segment is executing, so nothing is being folded into it.
    /// </remarks>
    CanonicalJsonValue Export();

    /// <summary>Takes back a state this scope exported earlier.</summary>
    /// <param name="state">The exported value, as a checkpoint carried it.</param>
    /// <exception cref="InvalidOperationException">
    /// The value does not describe this scope's chain, which is a checkpoint of a different graph or a
    /// hand-written one.
    /// </exception>
    /// <remarks>
    /// Called once, before the resumed run's first element, on the thread that materializes it. A scope
    /// restored mid-run would be a scope whose state changed under an element that was already inside it.
    /// </remarks>
    void Restore(CanonicalJsonValue state);
}
