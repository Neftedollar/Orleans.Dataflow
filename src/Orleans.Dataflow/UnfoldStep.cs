namespace Orleans.Dataflow;

/// <summary>
/// One step of an asynchronously unfolded source: the element to emit and the state to carry on from.
/// </summary>
/// <typeparam name="TState">The type of the state the source carries between elements.</typeparam>
/// <typeparam name="T">The element type the source produces.</typeparam>
/// <param name="Value">The element to emit.</param>
/// <param name="Next">The state the next call receives.</param>
/// <remarks>
/// <para>
/// The asynchronous sibling of <see cref="UnfoldGenerator{TState, T}"/> cannot be the try-shape that
/// delegate is: an <see langword="async"/> method has no <see langword="out"/> parameters, so the two named
/// outputs have to become one returned value. A step is that value, and
/// <c>UnfoldStep&lt;TState, T&gt;?</c> is how a generator says there are no more elements — a
/// <see langword="null"/> step is the end of the source, and it cannot be confused with an element that
/// happens to equal <c>default(T)</c>.
/// </para>
/// <para>
/// The members are named rather than positional at the use site, so a generator whose element and state
/// have the same type cannot swap them unnoticed. That is the property the try-shape bought with its
/// <see langword="out"/> parameter names, kept here at the cost recorded on
/// <see cref="Source.UnfoldAsync{TState, T}"/>: the type arguments have to be written at the call site,
/// because a conditional expression over a step and <see langword="null"/> has no natural type to infer
/// from.
/// </para>
/// </remarks>
public readonly record struct UnfoldStep<TState, T>(T Value, TState Next);
