namespace Orleans.Dataflow;

/// <summary>
/// Produces the next element of an unfolded source, or reports that there is none.
/// </summary>
/// <typeparam name="TState">The type of the state the source carries between elements.</typeparam>
/// <typeparam name="T">The element type the source produces.</typeparam>
/// <param name="state">The state the source carries; the seed for the first call.</param>
/// <param name="value">The element to emit, when this generator returns <see langword="true"/>.</param>
/// <param name="next">The state to hand the next call, when this generator returns <see langword="true"/>.</param>
/// <returns>
/// <see langword="true"/> to emit <paramref name="value"/> and continue from <paramref name="next"/>;
/// <see langword="false"/> to end the source, in which case both outputs are ignored.
/// </returns>
/// <remarks>
/// <para>
/// The try-shape rather than an option-returning function, because C# has no option type and every
/// substitute for one costs more than it saves here. A nullable step record cannot be inferred through a
/// conditional expression, so the author would have to write both type arguments at the call site; a tuple
/// with a flag names its members positionally, so a reader has to remember which of two same-typed values
/// is the element and which is the next state. Two named <see langword="out"/> parameters say which is
/// which at every call site, infer both type arguments from the lambda, and cannot confuse "no more
/// elements" with an element that happens to equal the default value.
/// </para>
/// <para>
/// A generator that assigns both outputs unconditionally and returns whether it had anything to say is the
/// ordinary spelling, and it costs no more lines than the option-returning form would:
/// </para>
/// <code>
/// Source.Unfold(1, (int state, out int value, out int next) =>
/// {
///     value = state;
///     next = state * 2;
///
///     return state &lt;= 1024;
/// });
/// </code>
/// <para>
/// The generator is the author's own code and may do anything, including never returning
/// <see langword="false"/>. An unfold is bounded by its own logic and by nothing else, so an endless one is
/// bounded downstream by <c>Take</c>, which completes the run when it has what it asked for.
/// </para>
/// </remarks>
public delegate bool UnfoldGenerator<TState, T>(TState state, out T value, out TState next);
