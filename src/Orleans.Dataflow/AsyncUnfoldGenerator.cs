namespace Orleans.Dataflow;

/// <summary>
/// Produces the next element of an asynchronously unfolded source, or reports that there is none.
/// </summary>
/// <typeparam name="TState">The type of the state the source carries between elements.</typeparam>
/// <typeparam name="T">The element type the source produces.</typeparam>
/// <param name="state">The state the source carries; the seed for the first call.</param>
/// <param name="cancellationToken">
/// The run's own token, cancelled when the run is cancelled and when anything in the run fails.
/// </param>
/// <returns>
/// The step to emit and continue from, or <see langword="null"/> to end the source.
/// </returns>
/// <remarks>
/// <para>
/// A named delegate rather than a <see cref="Func{T1, T2, TResult}"/> for two reasons that both outlive the
/// spelling: it is the shape the F# frontend binds to, and the local runtime recognizes a binding by its
/// delegate type, so a named type makes "this stage is bound to the wrong thing" a sentence instead of a
/// cast failure.
/// </para>
/// <para>
/// The token is part of the shape rather than an optional convenience, exactly as it is for the
/// asynchronous mapping stages: a generator with nowhere to receive a token could not be stopped at all,
/// and a source that ignores the token it was given delays the run's stop until it next yields.
/// </para>
/// <code>
/// Source.UnfoldAsync&lt;int, string&gt;(1, async (state, token) =&gt;
///     state &lt;= 1024 ? new(await RenderAsync(state, token), state * 2) : null);
/// </code>
/// </remarks>
public delegate Task<UnfoldStep<TState, T>?> AsyncUnfoldGenerator<TState, T>(
    TState state,
    CancellationToken cancellationToken);
