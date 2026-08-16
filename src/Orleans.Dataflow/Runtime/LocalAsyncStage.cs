namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The head of an asynchronous segment: the callback it runs, how many of them it runs at once, and
/// whether it emits results in input order or in completion order.
/// </summary>
/// <remarks>
/// <para>
/// An asynchronous stage is the only stage a run does not apply on the thread that pulled the element.
/// It is therefore the head of its own segment rather than one more entry in a fused chain: the elements
/// reach it through a bounded channel, it admits up to <see cref="MaxConcurrency"/> of them at once, and
/// the fused synchronous stages that follow it run on its own loop as it emits.
/// </para>
/// <para>
/// <see cref="Ordered"/> is the whole difference between the two spellings the authoring surface offers,
/// and it is a property of the stage rather than of its options, because it is chosen by which operator
/// was written and not by a number that could be changed independently.
/// </para>
/// </remarks>
internal sealed class LocalAsyncStage
{
    /// <summary>Initializes a new instance of the <see cref="LocalAsyncStage"/> class.</summary>
    /// <param name="callback">The author's callback over boxed elements.</param>
    /// <param name="maxConcurrency">The greatest number of callbacks in flight at one time; at least one.</param>
    /// <param name="ordered">Whether results are emitted in input order.</param>
    internal LocalAsyncStage(
        Func<object?, CancellationToken, Task<object?>> callback,
        int maxConcurrency,
        bool ordered)
    {
        Callback = callback;
        MaxConcurrency = maxConcurrency;
        Ordered = ordered;
    }

    /// <summary>Gets the author's callback, wrapped to take and produce boxed elements.</summary>
    /// <value>
    /// A delegate that never throws synchronously: a callback that throws before returning a task returns
    /// a faulted one instead, so the run has one way to observe a failure rather than two.
    /// </value>
    internal Func<object?, CancellationToken, Task<object?>> Callback { get; }

    /// <summary>Gets the greatest number of callbacks that may be in flight at one time.</summary>
    /// <remarks>
    /// For an ordered stage a slot is freed by emission and not by completion, because a result that
    /// finished early has to be held until its turn comes. That is what makes head-of-line blocking block
    /// admission eventually while never blocking it early.
    /// </remarks>
    internal int MaxConcurrency { get; }

    /// <summary>Gets a value indicating whether results are emitted in the order their elements arrived.</summary>
    /// <value>
    /// <see langword="true"/> for <c>select-async</c>; <see langword="false"/> for
    /// <c>select-async-unordered</c>, which emits each result as its callback completes.
    /// </value>
    internal bool Ordered { get; }
}
