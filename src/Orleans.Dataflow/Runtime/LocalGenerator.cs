namespace Orleans.Dataflow.Runtime;

/// <summary>
/// An author's unfold generator, wrapped to work over boxed state and boxed elements.
/// </summary>
/// <param name="state">The state the source carries; the seed for the first call.</param>
/// <param name="value">The element to emit, when this generator returns <see langword="true"/>.</param>
/// <param name="next">The state to hand the next call, when this generator returns <see langword="true"/>.</param>
/// <returns><see langword="true"/> to emit an element and continue; <see langword="false"/> to end.</returns>
/// <remarks>
/// The runtime counterpart of <see cref="Orleans.Dataflow.UnfoldGenerator{TState, T}"/>, in the boxed
/// vocabulary the plan speaks. It is a delegate of its own rather than the public one closed over
/// <see cref="object"/>, because the public one is what an author writes against and this one is what
/// <see cref="LocalDelegateAdapter"/> produces from it: the wrapping happens once per materialization, and
/// every element after that costs one delegate call.
/// </remarks>
internal delegate bool LocalGenerator(object? state, out object? value, out object? next);

/// <summary>
/// An author's asynchronous unfold generator, wrapped to work over boxed state and boxed elements.
/// </summary>
/// <param name="state">The state the source carries; the seed for the first call.</param>
/// <param name="cancellationToken">The run's own token, handed to the author's generator.</param>
/// <param name="value">The element to emit, when this generator returns <see langword="true"/>.</param>
/// <param name="next">The state to hand the next call, when this generator returns <see langword="true"/>.</param>
/// <returns><see langword="true"/> to emit an element and continue; <see langword="false"/> to end.</returns>
/// <remarks>
/// The runtime counterpart of <see cref="Orleans.Dataflow.AsyncUnfoldGenerator{TState, T}"/>, and
/// deliberately the same try-shape as <see cref="LocalGenerator"/> rather than a second vocabulary: the
/// public surface differs because an <see langword="async"/> method has no <see langword="out"/>
/// parameters, and inside the runtime that difference has already been absorbed. The waiting happens in the
/// wrapper, on the segment's own dedicated thread, so a segment's pull loop is one shape for every source
/// it has.
/// </remarks>
internal delegate bool LocalAsyncGenerator(
    object? state,
    CancellationToken cancellationToken,
    out object? value,
    out object? next);
