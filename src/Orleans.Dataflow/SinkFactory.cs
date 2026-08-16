namespace Orleans.Dataflow;

/// <summary>
/// The sink vocabulary of one element type, handed to the sink-factory lambda overloads of <c>To</c>.
/// </summary>
/// <typeparam name="T">The element type, fixed by the source the sink is attached to.</typeparam>
/// <remarks>
/// <para>
/// This type exists to close one inference hole. <c>Sink.Aggregate(0L, (count, _) =&gt; count + 1)</c> does not
/// compile, because the element type appears only as an implicit lambda parameter and C# does not flow the
/// outer call's element type inward (ADR 0004 section 3; <c>CS0411</c>, and a partial type-argument list is
/// not a legal spelling either). Passing a lambda that receives this factory instead pins <typeparamref
/// name="T"/> from the source, so <c>To(s =&gt; s.Aggregate(0L, (count, _) =&gt; count + 1), "processed", out
/// var processed)</c> needs no type argument and no lambda annotation anywhere.
/// </para>
/// <para>
/// The factory is stateless and is never constructed by an author: it arrives as the argument of the
/// lambda, or by name from <see cref="Sink.For{T}"/>, and its methods are the same ones on
/// <see cref="Sink"/> with <typeparamref name="T"/> already supplied. Both ways reach one instance per
/// closed generic type, so a sink built either way is the same sink.
/// </para>
/// </remarks>
public sealed class SinkFactory<T>
{
    /// <summary>Initializes a new instance of the <see cref="SinkFactory{T}"/> class.</summary>
    private SinkFactory()
    {
    }

    /// <summary>Gets the one instance handed to every sink-factory lambda of this element type.</summary>
    /// <remarks>
    /// The type is stateless, so one instance per closed generic type is all there is to have. This is
    /// also what <see cref="Sink.For{T}"/> returns, which is why the named spelling and the lambda's
    /// argument are the same object rather than two objects that behave alike.
    /// </remarks>
    internal static SinkFactory<T> Instance { get; } = new();

    /// <summary>Creates a sink that consumes every element and produces nothing.</summary>
    /// <returns>The sink.</returns>
    public Sink<T> Ignore() => Sink.Ignore<T>();

    /// <summary>Creates a sink that folds every element into a state value and exposes the final state.</summary>
    /// <typeparam name="TState">The type of the state, which is also the type of the result.</typeparam>
    /// <param name="seed">The initial state.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <returns>The result-bearing sink.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="folder"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <typeparamref name="TState"/> is inferred from <paramref name="seed"/> and the element type is
    /// already known, so both lambda parameters of <paramref name="folder"/> are typed without an
    /// annotation.
    /// </remarks>
    public SinkWithResult<T, TState> Aggregate<TState>(TState seed, Func<TState, T, TState> folder) =>
        Sink.Aggregate(seed, folder);

    /// <summary>Returns a one-line diagnostic summary of this factory.</summary>
    /// <returns>The literal <c>sink factory</c>.</returns>
    /// <remarks>The factory is stateless, so there is nothing else to say about an instance.</remarks>
    public override string ToString() => "sink factory";
}
