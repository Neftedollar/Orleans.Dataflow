using System.Collections;
using System.Reflection;
using System.Threading.Channels;
using Orleans.Dataflow.Authoring;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The one place where an authoring-side delegate, held as <see cref="object"/> in its original
/// constructed type, becomes a delegate the run loop can call over boxed elements.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="LocalStageDescriptor"/> keeps <c>Func&lt;TIn, TOut&gt;</c>, <c>Func&lt;T, bool&gt;</c>, and
/// <c>Func&lt;TState, T, TState&gt;</c> boxed as <see cref="object"/>, because one chain spans element
/// types that change at every mapping stage. Recovering those types is what this type does, once per
/// stage per materialization: the delegate's own constructed type names them, a private generic template
/// is closed over them, and the closure it returns is an ordinary delegate call for every element after
/// that.
/// </para>
/// <para>
/// The alternative, <see cref="Delegate.DynamicInvoke"/> on every element, was rejected on two counts: it
/// costs reflection per element rather than per stage, and it wraps whatever the author's delegate throws
/// in a <see cref="TargetInvocationException"/>, which would put a runtime type between an author's
/// exception and the run that reports it. The exception a stage throws here is the exception the run
/// faults with, unwrapped and instance-identical.
/// </para>
/// <para>
/// Every failure this type raises is an <see cref="InvalidOperationException"/> describing a binding whose
/// shape does not match the stage it is bound to. None is reachable through the authoring API, whose
/// generic signatures make the shapes agree by construction; they exist so that a hand-built or foreign
/// binding table fails where the mismatch is, rather than as an <see cref="InvalidCastException"/> from
/// inside a run.
/// </para>
/// </remarks>
internal static class LocalDelegateAdapter
{
    /// <summary>The template closed to wrap a mapping delegate.</summary>
    private static readonly MethodInfo SelectorTemplate = Template(nameof(BoxSelector));

    /// <summary>The template closed to wrap a predicate delegate.</summary>
    private static readonly MethodInfo PredicateTemplate = Template(nameof(BoxPredicate));

    /// <summary>The template closed to wrap a folding delegate.</summary>
    private static readonly MethodInfo FolderTemplate = Template(nameof(BoxFolder));

    /// <summary>The template closed to wrap an asynchronous mapping delegate.</summary>
    private static readonly MethodInfo AsyncSelectorTemplate = Template(nameof(BoxAsyncSelector));

    /// <summary>The template closed to wrap an asynchronous mapping over value tasks.</summary>
    private static readonly MethodInfo ValueTaskSelectorTemplate = Template(nameof(BoxValueTaskSelector));

    /// <summary>The template closed to wrap an asynchronous callback with no result.</summary>
    private static readonly MethodInfo AsyncCallbackTemplate = Template(nameof(BoxAsyncCallback));

    /// <summary>The template closed to wrap a per-element action.</summary>
    private static readonly MethodInfo ActionTemplate = Template(nameof(BoxAction));

    /// <summary>The template closed to wrap an unfold generator.</summary>
    private static readonly MethodInfo GeneratorTemplate = Template(nameof(BoxGenerator));

    /// <summary>The template closed to read the value of a task.</summary>
    private static readonly MethodInfo TaskValueTemplate = Template(nameof(BoxTaskValue));

    /// <summary>The template closed to wrap a deferred element factory.</summary>
    private static readonly MethodInfo FactoryTemplate = Template(nameof(BoxFactory));

    /// <summary>The template closed to wrap an asynchronous deferred element factory.</summary>
    private static readonly MethodInfo AsyncFactoryTemplate = Template(nameof(BoxAsyncFactory));

    /// <summary>The template closed to wrap an asynchronous unfold generator.</summary>
    private static readonly MethodInfo AsyncGeneratorTemplate = Template(nameof(BoxAsyncGenerator));

    /// <summary>The template closed to bridge a channel reader.</summary>
    private static readonly MethodInfo ChannelSourceTemplate = Template(nameof(BoxChannelSource));

    /// <summary>The template closed to bridge a channel writer.</summary>
    private static readonly MethodInfo ChannelSinkTemplate = Template(nameof(BoxChannelSink));

    /// <summary>Reads a source binding as a sequence the run loop can enumerate.</summary>
    /// <param name="behavior">The bound sequence, as the authoring value received it.</param>
    /// <param name="kind">The stage shape, for the diagnostic.</param>
    /// <returns>The sequence, viewed through the non-generic interface every sequence implements.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a sequence.</exception>
    /// <remarks>
    /// The non-generic view is what makes the source reflection-free: every <c>IEnumerable&lt;T&gt;</c> is
    /// an <see cref="IEnumerable"/>, and the run loop only ever needs elements as <see cref="object"/>.
    /// </remarks>
    internal static IEnumerable Elements(object? behavior, LocalStageKind kind) =>
        behavior as IEnumerable ??
        throw new InvalidOperationException(
            $"A '{kind}' stage must be bound to a sequence, and this one is bound to {Describe(behavior)}.");

    /// <summary>Wraps a mapping delegate into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;TIn, TOut&gt;</c>.</param>
    /// <returns>The wrapped mapping.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a one-argument function.</exception>
    internal static Func<object?, object?> Selector(object? behavior)
    {
        Type[] arguments = Arguments(behavior, typeof(Func<,>), LocalStageKind.Select, "Func<TIn, TOut>");

        return (Func<object?, object?>)Close(SelectorTemplate, [arguments[0], arguments[1]], behavior);
    }

    /// <summary>Reads a source binding as the exception the run is to fail with.</summary>
    /// <param name="behavior">The bound exception, as the authoring value received it.</param>
    /// <returns>The exception.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not an exception.</exception>
    internal static Exception Failure(object? behavior) =>
        behavior as Exception ??
        throw new InvalidOperationException(
            $"A '{LocalStageKind.Failed}' stage must be bound to an exception to fail with, and this one is bound to {Describe(behavior)}.");

    /// <summary>Reads a distinct binding as the equality it deduplicates by.</summary>
    /// <param name="behavior">The bound comparer, as the authoring value received it.</param>
    /// <returns>The comparer, viewed through the non-generic interface every comparer implements.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not an equality comparer.</exception>
    /// <remarks>
    /// The non-generic view is what makes deduplication reflection-free: every
    /// <see cref="EqualityComparer{T}.Default"/> implements it, its members already answer for
    /// <see langword="null"/> and for a value of the wrong type, and the run loop only ever has elements as
    /// <see cref="object"/>.
    /// </remarks>
    internal static IEqualityComparer Comparer(object? behavior) =>
        behavior as IEqualityComparer ??
        throw new InvalidOperationException(
            $"A '{LocalStageKind.Distinct}' stage must be bound to an equality comparer, and this one is bound to {Describe(behavior)}.");

    /// <summary>Wraps a predicate delegate into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;T, bool&gt;</c>.</param>
    /// <param name="kind">The stage shape, for the diagnostic.</param>
    /// <returns>The wrapped predicate.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="behavior"/> is not a one-argument function returning <see cref="bool"/>.
    /// </exception>
    internal static Func<object?, bool> Predicate(object? behavior, LocalStageKind kind)
    {
        const string Expected = "Func<T, bool>";

        Type[] arguments = Arguments(behavior, typeof(Func<,>), kind, Expected);

        if (arguments[1] != typeof(bool))
        {
            throw Mismatch(behavior, kind, Expected);
        }

        return (Func<object?, bool>)Close(PredicateTemplate, [arguments[0]], behavior);
    }

    /// <summary>Wraps a folding delegate into one over boxed state and boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;TState, T, TState&gt;</c>.</param>
    /// <param name="kind">The stage shape, for the diagnostic.</param>
    /// <returns>The wrapped folder.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a two-argument function.</exception>
    /// <remarks>
    /// One wrapper for the two shapes that fold, because they fold identically and differ only in what the
    /// run does with the state afterwards: a sink keeps it and a scan emits it.
    /// </remarks>
    internal static Func<object?, object?, object?> Folder(object? behavior, LocalStageKind kind)
    {
        const string Expected = "Func<TState, T, TState>";

        Type[] arguments = Arguments(behavior, typeof(Func<,,>), kind, Expected);

        if (arguments[2] != arguments[0])
        {
            throw Mismatch(behavior, kind, Expected);
        }

        return (Func<object?, object?, object?>)Close(FolderTemplate, [arguments[0], arguments[1]], behavior);
    }

    /// <summary>Wraps a per-element action into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Action&lt;T&gt;</c>.</param>
    /// <returns>The wrapped action.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a one-argument action.</exception>
    internal static Action<object?> Action(object? behavior)
    {
        Type[] arguments = Arguments(behavior, typeof(Action<>), LocalStageKind.ForEach, "Action<T>");

        return (Action<object?>)Close(ActionTemplate, [arguments[0]], behavior);
    }

    /// <summary>Wraps an unfold generator into one over boxed state and boxed elements.</summary>
    /// <param name="behavior">The bound <c>UnfoldGenerator&lt;TState, T&gt;</c>.</param>
    /// <returns>The wrapped generator.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="behavior"/> is not an <see cref="UnfoldGenerator{TState, T}"/>.
    /// </exception>
    internal static LocalGenerator Generator(object? behavior)
    {
        Type[] arguments = Arguments(
            behavior,
            typeof(UnfoldGenerator<,>),
            LocalStageKind.Unfold,
            "UnfoldGenerator<TState, T>");

        return (LocalGenerator)Close(GeneratorTemplate, [arguments[0], arguments[1]], behavior);
    }

    /// <summary>Wraps a task into a function that reads its value, blocking until it has one.</summary>
    /// <param name="behavior">The bound <c>Task&lt;T&gt;</c>.</param>
    /// <returns>The reader.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a task with a value.</exception>
    /// <remarks>
    /// The result type is found by walking the base types rather than by reading the bound object's own
    /// type, because a task an <see langword="async"/> method returns is an instance of a private class
    /// deriving from <see cref="Task{TResult}"/> rather than of <see cref="Task{TResult}"/> itself. A check
    /// that compared the constructed type would accept <c>Task.FromResult</c> and reject every task an
    /// author actually awaits.
    /// </remarks>
    internal static Func<object?> TaskValue(object? behavior) =>
        (Func<object?>)Close(
            TaskValueTemplate,
            [Derived(behavior, typeof(Task<>), LocalStageKind.FromTask, "Task<T>")],
            behavior);

    /// <summary>Wraps an asynchronous mapping delegate into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;TIn, CancellationToken, Task&lt;TOut&gt;&gt;</c>.</param>
    /// <param name="kind">The stage shape, for the diagnostic.</param>
    /// <returns>The wrapped mapping.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="behavior"/> is not a two-argument function taking a
    /// <see cref="CancellationToken"/> and returning a <see cref="Task{TResult}"/>.
    /// </exception>
    /// <remarks>
    /// The token is part of the required shape rather than an optional convenience: an asynchronous stage
    /// cancels its in-flight callbacks when the run is cancelled or when anything in the run fails, and a
    /// callback with nowhere to receive a token could not be cancelled at all.
    /// </remarks>
    internal static Func<object?, CancellationToken, Task<object?>> AsyncSelector(
        object? behavior,
        LocalStageKind kind)
    {
        const string Expected = "Func<TIn, CancellationToken, Task<TOut>>";

        Type[] arguments = Arguments(behavior, typeof(Func<,,>), kind, Expected);

        if (arguments[1] != typeof(CancellationToken) ||
            !arguments[2].IsGenericType ||
            arguments[2].GetGenericTypeDefinition() != typeof(Task<>))
        {
            throw Mismatch(behavior, kind, Expected);
        }

        return (Func<object?, CancellationToken, Task<object?>>)Close(
            AsyncSelectorTemplate,
            [arguments[0], arguments[2].GetGenericArguments()[0]],
            behavior);
    }

    /// <summary>Wraps an asynchronous mapping over value tasks into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;TIn, CancellationToken, ValueTask&lt;TOut&gt;&gt;</c>.</param>
    /// <param name="kind">The stage shape, for the diagnostic.</param>
    /// <returns>The wrapped mapping, in the one callback shape the asynchronous segment driver knows.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="behavior"/> is not a two-argument function taking a
    /// <see cref="CancellationToken"/> and returning a <see cref="ValueTask{TResult}"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The conversion is the whole of the value-task family's implementation, and it happens here rather
    /// than in the run loop on purpose: what an asynchronous stage does — how many callbacks it admits, in
    /// what order it emits, what a failure does to the ones beside it, what a drain awaits — is one
    /// implementation, and a second driver differing only in the shape of the thing it awaits would be two
    /// statements of one contract that could disagree. The price is one task per element, on a path that
    /// already allocates a continuation per element.
    /// </para>
    /// <para>
    /// The wrapper awaits the value task exactly once, which is the whole of the single-consumption rule a
    /// <see cref="ValueTask{TResult}"/> imposes: an implementation backed by a pooled source may be
    /// consumed once, and a runtime that awaited one twice, or that awaited it after reading its result,
    /// would corrupt whatever else that source is now serving.
    /// </para>
    /// </remarks>
    internal static Func<object?, CancellationToken, Task<object?>> ValueTaskSelector(
        object? behavior,
        LocalStageKind kind)
    {
        const string Expected = "Func<TIn, CancellationToken, ValueTask<TOut>>";

        Type[] arguments = Arguments(behavior, typeof(Func<,,>), kind, Expected);

        if (arguments[1] != typeof(CancellationToken) ||
            !arguments[2].IsGenericType ||
            arguments[2].GetGenericTypeDefinition() != typeof(ValueTask<>))
        {
            throw Mismatch(behavior, kind, Expected);
        }

        return (Func<object?, CancellationToken, Task<object?>>)Close(
            ValueTaskSelectorTemplate,
            [arguments[0], arguments[2].GetGenericArguments()[0]],
            behavior);
    }

    /// <summary>Wraps an asynchronous callback with no result into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;T, CancellationToken, Task&gt;</c>.</param>
    /// <returns>The wrapped callback, whose task always produces <see langword="null"/>.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="behavior"/> is not a two-argument function taking a
    /// <see cref="CancellationToken"/> and returning a <see cref="Task"/>.
    /// </exception>
    /// <remarks>
    /// A callback sink is an asynchronous stage that emits nothing, so it is executed as one that emits
    /// <see langword="null"/> and whose segment has nothing to hand it to. That is what gives it the
    /// concurrency bound, the token, the failure semantics, and the "completion awaits every callback"
    /// promise of the mapping stages without a second implementation of any of them.
    /// </remarks>
    internal static Func<object?, CancellationToken, Task<object?>> AsyncCallback(object? behavior)
    {
        const string Expected = "Func<T, CancellationToken, Task>";

        Type[] arguments = Arguments(behavior, typeof(Func<,,>), LocalStageKind.ForEachAsync, Expected);

        if (arguments[1] != typeof(CancellationToken) || arguments[2] != typeof(Task))
        {
            throw Mismatch(behavior, LocalStageKind.ForEachAsync, Expected);
        }

        return (Func<object?, CancellationToken, Task<object?>>)Close(
            AsyncCallbackTemplate,
            [arguments[0]],
            behavior);
    }

    /// <summary>Wraps a deferred element factory into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;T&gt;</c>.</param>
    /// <returns>The wrapped factory.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a no-argument function.</exception>
    internal static Func<object?> Factory(object? behavior)
    {
        Type[] arguments = Arguments(behavior, typeof(Func<>), LocalStageKind.FromFactory, "Func<T>");

        return (Func<object?>)Close(FactoryTemplate, [arguments[0]], behavior);
    }

    /// <summary>Wraps an asynchronous deferred element factory into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;CancellationToken, Task&lt;T&gt;&gt;</c>.</param>
    /// <returns>The wrapped factory, which blocks the calling thread until the task settles.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="behavior"/> is not a one-argument function taking a
    /// <see cref="CancellationToken"/> and returning a <see cref="Task{TResult}"/>.
    /// </exception>
    internal static Func<CancellationToken, object?> AsyncFactory(object? behavior)
    {
        const string Expected = "Func<CancellationToken, Task<T>>";

        Type[] arguments = Arguments(behavior, typeof(Func<,>), LocalStageKind.FromAsyncFactory, Expected);

        if (arguments[0] != typeof(CancellationToken) ||
            !arguments[1].IsGenericType ||
            arguments[1].GetGenericTypeDefinition() != typeof(Task<>))
        {
            throw Mismatch(behavior, LocalStageKind.FromAsyncFactory, Expected);
        }

        return (Func<CancellationToken, object?>)Close(
            AsyncFactoryTemplate,
            [arguments[1].GetGenericArguments()[0]],
            behavior);
    }

    /// <summary>Wraps an asynchronous unfold generator into one over boxed state and boxed elements.</summary>
    /// <param name="behavior">The bound <c>AsyncUnfoldGenerator&lt;TState, T&gt;</c>.</param>
    /// <returns>The wrapped generator.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="behavior"/> is not an <see cref="AsyncUnfoldGenerator{TState, T}"/>.
    /// </exception>
    internal static LocalAsyncGenerator AsyncGenerator(object? behavior)
    {
        Type[] arguments = Arguments(
            behavior,
            typeof(AsyncUnfoldGenerator<,>),
            LocalStageKind.UnfoldAsync,
            "AsyncUnfoldGenerator<TState, T>");

        return (LocalAsyncGenerator)Close(AsyncGeneratorTemplate, [arguments[0], arguments[1]], behavior);
    }

    /// <summary>Reads a source binding as the opener of an asynchronous sequence.</summary>
    /// <param name="behavior">The bound opener, which the authoring surface closed over the element type.</param>
    /// <returns>The opener.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not an opener.</exception>
    /// <remarks>
    /// The one source binding that is not the author's own value and is not recovered by reflection either.
    /// <see cref="IAsyncEnumerable{T}"/> is an interface, and one class may implement it for two element
    /// types, so the bound object's own type is not a statement of which of them the graph means; the type
    /// argument the author wrote is, and closing the opener over it at authoring time is how that statement
    /// is kept.
    /// </remarks>
    internal static LocalAsyncCursorFactory AsyncCursors(object? behavior) =>
        behavior as LocalAsyncCursorFactory ??
        throw new InvalidOperationException(
            $"A '{LocalStageKind.FromAsyncEnumerable}' stage must be bound to an opener of an asynchronous sequence, and this one is bound to {Describe(behavior)}.");

    /// <summary>Reads a source binding as the facade factory of an ingress queue.</summary>
    /// <param name="behavior">The bound factory, which the authoring surface closed over the element type.</param>
    /// <returns>The factory.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not such a factory.</exception>
    /// <remarks>
    /// Pinned at authoring for the same reason an asynchronous sequence's opener is: the queue the runtime
    /// builds works in boxed elements, and the typed handle an author receives can only be built by code
    /// that has the type argument.
    /// </remarks>
    internal static Func<LocalIngressQueue, object> QueueFacade(object? behavior) =>
        behavior as Func<LocalIngressQueue, object> ??
        throw new InvalidOperationException(
            $"A '{LocalStageKind.Queue}' stage must be bound to a factory of its typed control, and this one is bound to {Describe(behavior)}.");

    /// <summary>Reads a sink binding as the facade factory of a probe sink.</summary>
    /// <param name="behavior">The bound factory, which the authoring surface closed over the element type.</param>
    /// <returns>The factory.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not such a factory.</exception>
    /// <remarks>
    /// The mirror image of <see cref="QueueFacade"/> at the other end of a chain, and pinned at authoring
    /// for the same reason: the rendezvous the runtime builds works in boxed elements, and only code
    /// holding the type argument can hand an author a typed receiver over it.
    /// </remarks>
    internal static Func<LocalSinkProbe, object> ProbeFacade(object? behavior) =>
        behavior as Func<LocalSinkProbe, object> ??
        throw new InvalidOperationException(
            $"A '{LocalStageKind.SinkProbe}' stage must be bound to a factory of its typed control, and this one is bound to {Describe(behavior)}.");

    /// <summary>Reads a sink binding as the projection of a collected list into its result type.</summary>
    /// <param name="behavior">The bound projection, which the authoring surface closed over the element type.</param>
    /// <returns>The projection.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not such a projection.</exception>
    internal static Func<object?, object?> Freeze(object? behavior) =>
        behavior as Func<object?, object?> ??
        throw new InvalidOperationException(
            $"A '{LocalStageKind.Collect}' stage must be bound to a projection of its collected elements, and this one is bound to {Describe(behavior)}.");

    /// <summary>Bridges a channel reader into the boxed vocabulary a pull loop speaks.</summary>
    /// <param name="behavior">The bound <c>ChannelReader&lt;T&gt;</c>.</param>
    /// <returns>The bridge.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a channel reader.</exception>
    internal static LocalChannelSource ChannelSource(object? behavior) =>
        (LocalChannelSource)Close(
            ChannelSourceTemplate,
            [Derived(behavior, typeof(ChannelReader<>), LocalStageKind.FromChannel, "ChannelReader<T>")],
            behavior);

    /// <summary>Bridges a channel writer into the boxed vocabulary a terminal speaks.</summary>
    /// <param name="behavior">The bound <c>ChannelWriter&lt;T&gt;</c>.</param>
    /// <returns>The bridge.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a channel writer.</exception>
    internal static LocalChannelSink ChannelSink(object? behavior) =>
        (LocalChannelSink)Close(
            ChannelSinkTemplate,
            [Derived(behavior, typeof(ChannelWriter<>), LocalStageKind.ToChannel, "ChannelWriter<T>")],
            behavior);

    /// <summary>Reads one of this type's private generic templates.</summary>
    /// <param name="name">The template method's name.</param>
    /// <returns>The generic method definition.</returns>
    private static MethodInfo Template(string name) =>
        typeof(LocalDelegateAdapter).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <summary>Reads the type arguments of a bound delegate after checking its shape.</summary>
    /// <param name="behavior">The bound delegate.</param>
    /// <param name="definition">The delegate type definition the stage requires.</param>
    /// <param name="kind">The stage shape, for the diagnostic.</param>
    /// <param name="expected">The required delegate shape as text, for the diagnostic.</param>
    /// <returns>The delegate type's type arguments.</returns>
    /// <exception cref="InvalidOperationException">The binding does not have the required shape.</exception>
    private static Type[] Arguments(object? behavior, Type definition, LocalStageKind kind, string expected)
    {
        if (behavior is null)
        {
            throw Mismatch(behavior, kind, expected);
        }

        Type type = behavior.GetType();

        return type.IsGenericType && type.GetGenericTypeDefinition() == definition
            ? type.GetGenericArguments()
            : throw Mismatch(behavior, kind, expected);
    }

    /// <summary>Reads the one type argument of a generic base class a bound value derives from.</summary>
    /// <param name="behavior">The bound value.</param>
    /// <param name="definition">The generic base class definition the stage requires.</param>
    /// <param name="kind">The stage shape, for the diagnostic.</param>
    /// <param name="expected">The required type as text, for the diagnostic.</param>
    /// <returns>The base class's type argument.</returns>
    /// <exception cref="InvalidOperationException">The binding does not derive from that base class.</exception>
    /// <remarks>
    /// The base types are walked rather than the bound object's own type compared, because the useful
    /// instances of these abstractions are all private classes deriving from them: a channel's reader and
    /// writer, and the task an <see langword="async"/> method returns. Walking a base chain is also exact
    /// where searching interfaces would not be — a class has one base at each level, so there is never a
    /// second candidate to choose between.
    /// </remarks>
    private static Type Derived(object? behavior, Type definition, LocalStageKind kind, string expected)
    {
        if (behavior is null)
        {
            throw Mismatch(behavior, kind, expected);
        }

        for (Type? type = behavior.GetType(); type is not null; type = type.BaseType)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == definition)
            {
                return type.GetGenericArguments()[0];
            }
        }

        throw Mismatch(behavior, kind, expected);
    }

    /// <summary>Closes a template over the recovered type arguments and invokes it.</summary>
    /// <param name="template">The generic method definition.</param>
    /// <param name="arguments">The type arguments to close it over.</param>
    /// <param name="behavior">The bound delegate to wrap.</param>
    /// <returns>The wrapper the template built.</returns>
    private static object Close(MethodInfo template, Type[] arguments, object? behavior) =>
        template.MakeGenericMethod(arguments).Invoke(null, [behavior])!;

    /// <summary>Builds the exception for a binding whose shape does not match its stage.</summary>
    /// <param name="behavior">The bound value.</param>
    /// <param name="kind">The stage shape.</param>
    /// <param name="expected">The required delegate shape as text.</param>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException Mismatch(object? behavior, LocalStageKind kind, string expected) =>
        new($"A '{kind}' stage must be bound to a {expected}, and this one is bound to {Describe(behavior)}.");

    /// <summary>Renders a bound value's type for a diagnostic.</summary>
    /// <param name="behavior">The bound value.</param>
    /// <returns>The type name, or a literal for <see langword="null"/>.</returns>
    private static string Describe(object? behavior) => behavior is null ? "nothing" : behavior.GetType().ToString();

    /// <summary>Wraps a typed mapping into one over boxed elements.</summary>
    /// <typeparam name="TIn">The element type the mapping consumes.</typeparam>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="selector">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>Invoked only by reflection, over the type arguments recovered from the delegate itself.</remarks>
    private static Func<object?, object?> BoxSelector<TIn, TOut>(Func<TIn, TOut> selector) =>
        element => selector((TIn)element!);

    /// <summary>Wraps a typed predicate into one over boxed elements.</summary>
    /// <typeparam name="TIn">The element type the predicate tests.</typeparam>
    /// <param name="predicate">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>Invoked only by reflection, over the type arguments recovered from the delegate itself.</remarks>
    private static Func<object?, bool> BoxPredicate<TIn>(Func<TIn, bool> predicate) =>
        element => predicate((TIn)element!);

    /// <summary>Wraps a typed asynchronous mapping into one over boxed elements.</summary>
    /// <typeparam name="TIn">The element type the mapping consumes.</typeparam>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="selector">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>
    /// <para>
    /// Invoked only by reflection, over the type arguments recovered from the delegate itself. The wrapper
    /// is an asynchronous method rather than a continuation, so a callback that throws before it returns a
    /// task produces a faulted task exactly as one that throws afterwards does. The run therefore has one
    /// way to observe a callback failure instead of two, and the exception it faults with is the author's
    /// own instance either way.
    /// </para>
    /// <para>
    /// A callback that returns no task at all is reported as a sentence rather than dereferenced, for the
    /// same reason a sequence that produces no enumerator is.
    /// </para>
    /// </remarks>
    private static Func<object?, CancellationToken, Task<object?>> BoxAsyncSelector<TIn, TOut>(
        Func<TIn, CancellationToken, Task<TOut>> selector) =>
        async (element, token) =>
        {
            Task<TOut> pending = selector((TIn)element!, token) ??
                throw new InvalidOperationException(
                    "The callback of an asynchronous stage returned no task. A callback a graph is bound to has to produce something to await.");

            return await pending.ConfigureAwait(false);
        };

    /// <summary>Wraps a typed asynchronous mapping over value tasks into one over boxed elements.</summary>
    /// <typeparam name="TIn">The element type the mapping consumes.</typeparam>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="selector">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>
    /// Invoked only by reflection, over the type arguments recovered from the delegate itself. An
    /// <see langword="await"/> and nothing else: a value task is a struct and can never be the null a
    /// task-returning callback can return, and awaiting it once is what turns it into the task the
    /// asynchronous segment driver holds. A callback that throws before returning its value task produces a
    /// faulted task here exactly as one that throws afterwards does.
    /// </remarks>
    private static Func<object?, CancellationToken, Task<object?>> BoxValueTaskSelector<TIn, TOut>(
        Func<TIn, CancellationToken, ValueTask<TOut>> selector) =>
        async (element, token) => await selector((TIn)element!, token).ConfigureAwait(false);

    /// <summary>Wraps a typed folder into one over boxed state and boxed elements.</summary>
    /// <typeparam name="TState">The state type, which is also the result type.</typeparam>
    /// <typeparam name="TIn">The element type the fold consumes.</typeparam>
    /// <param name="folder">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>Invoked only by reflection, over the type arguments recovered from the delegate itself.</remarks>
    private static Func<object?, object?, object?> BoxFolder<TState, TIn>(Func<TState, TIn, TState> folder) =>
        (state, element) => folder((TState)state!, (TIn)element!);

    /// <summary>Wraps a typed action into one over boxed elements.</summary>
    /// <typeparam name="TIn">The element type the action consumes.</typeparam>
    /// <param name="callback">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>Invoked only by reflection, over the type argument recovered from the delegate itself.</remarks>
    private static Action<object?> BoxAction<TIn>(Action<TIn> callback) =>
        element => callback((TIn)element!);

    /// <summary>Wraps a typed asynchronous callback into one over boxed elements.</summary>
    /// <typeparam name="TIn">The element type the callback consumes.</typeparam>
    /// <param name="callback">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>
    /// Invoked only by reflection, over the type argument recovered from the delegate itself. An
    /// asynchronous method rather than a continuation, for the reason
    /// <see cref="BoxAsyncSelector{TIn, TOut}"/> is one: a callback that throws before returning a task
    /// produces a faulted task exactly as one that throws afterwards does.
    /// </remarks>
    private static Func<object?, CancellationToken, Task<object?>> BoxAsyncCallback<TIn>(
        Func<TIn, CancellationToken, Task> callback) =>
        async (element, token) =>
        {
            Task pending = callback((TIn)element!, token) ??
                throw new InvalidOperationException(
                    "The callback of an asynchronous sink returned no task. A callback a graph is bound to has to produce something to await.");

            await pending.ConfigureAwait(false);

            return null;
        };

    /// <summary>Wraps a typed unfold generator into one over boxed state and boxed elements.</summary>
    /// <typeparam name="TState">The state type the generator carries.</typeparam>
    /// <typeparam name="T">The element type the generator produces.</typeparam>
    /// <param name="generator">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>
    /// Invoked only by reflection, over the type arguments recovered from the delegate itself. The two
    /// outputs are copied out whatever the generator answered, because a generator that stopped has
    /// assigned them both and the caller ignores them; reading them only on the true path would make the
    /// wrapper's rules differ from the delegate's.
    /// </remarks>
    private static LocalGenerator BoxGenerator<TState, T>(UnfoldGenerator<TState, T> generator) =>
        (object? state, out object? value, out object? next) =>
        {
            bool produced = generator((TState)state!, out T element, out TState following);

            value = element;
            next = following;

            return produced;
        };

    /// <summary>Wraps a typed task into a function that reads its value.</summary>
    /// <typeparam name="T">The task's result type, which is the element type.</typeparam>
    /// <param name="task">The author's task.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>
    /// Invoked only by reflection, over the result type recovered from the task's own type hierarchy. The
    /// value is read through the awaiter rather than through <see cref="Task{TResult}.Result"/>, which is
    /// what makes a failing task fault the run with the author's own exception instead of with the
    /// <see cref="AggregateException"/> a task wraps it in.
    /// </remarks>
    private static Func<object?> BoxTaskValue<T>(Task<T> task) =>
        () => task.GetAwaiter().GetResult();

    /// <summary>Wraps a typed deferred factory into one over boxed elements.</summary>
    /// <typeparam name="T">The element type the factory produces.</typeparam>
    /// <param name="factory">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>Invoked only by reflection, over the type argument recovered from the delegate itself.</remarks>
    private static Func<object?> BoxFactory<T>(Func<T> factory) => () => factory();

    /// <summary>Wraps a typed asynchronous deferred factory into one over boxed elements.</summary>
    /// <typeparam name="T">The element type the factory produces.</typeparam>
    /// <param name="factory">The author's delegate.</param>
    /// <returns>The wrapper, which blocks the calling thread until the task settles.</returns>
    /// <remarks>
    /// Invoked only by reflection, over the type argument recovered from the delegate itself. The value is
    /// read through the awaiter rather than through <see cref="Task{TResult}.Result"/>, which is what makes
    /// a failing factory fault the run with the author's own exception instead of with the
    /// <see cref="AggregateException"/> a task wraps it in. A factory that returns no task at all is
    /// reported as a sentence rather than dereferenced.
    /// </remarks>
    private static Func<CancellationToken, object?> BoxAsyncFactory<T>(Func<CancellationToken, Task<T>> factory) =>
        token =>
        {
            Task<T> pending = factory(token) ??
                throw new InvalidOperationException(
                    "The factory of a deferred source returned no task. A factory a graph is bound to has to produce something to await.");

            return pending.GetAwaiter().GetResult();
        };

    /// <summary>Wraps a typed asynchronous unfold generator into one over boxed state and boxed elements.</summary>
    /// <typeparam name="TState">The state type the generator carries.</typeparam>
    /// <typeparam name="T">The element type the generator produces.</typeparam>
    /// <param name="generator">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>
    /// Invoked only by reflection, over the type arguments recovered from the delegate itself. A step that
    /// is absent ends the source and leaves both outputs at their default, which the caller ignores; the
    /// value is read through the awaiter so that a failing generator faults the run with the author's own
    /// exception, unwrapped.
    /// </remarks>
    private static LocalAsyncGenerator BoxAsyncGenerator<TState, T>(AsyncUnfoldGenerator<TState, T> generator) =>
        (object? state, CancellationToken token, out object? value, out object? next) =>
        {
            Task<UnfoldStep<TState, T>?> pending = generator((TState)state!, token) ??
                throw new InvalidOperationException(
                    "The generator of an asynchronous unfold returned no task. A generator a graph is bound to has to produce something to await.");

            if (pending.GetAwaiter().GetResult() is not { } step)
            {
                value = null;
                next = null;

                return false;
            }

            value = step.Value;
            next = step.Next;

            return true;
        };

    /// <summary>Bridges a typed channel reader.</summary>
    /// <typeparam name="T">The element type the channel carries.</typeparam>
    /// <param name="reader">The author's reader.</param>
    /// <returns>The bridge.</returns>
    /// <remarks>Invoked only by reflection, over the type argument recovered from the reader's base type.</remarks>
    private static LocalChannelSource<T> BoxChannelSource<T>(ChannelReader<T> reader) => new(reader);

    /// <summary>Bridges a typed channel writer.</summary>
    /// <typeparam name="T">The element type the channel carries.</typeparam>
    /// <param name="writer">The author's writer.</param>
    /// <returns>The bridge.</returns>
    /// <remarks>Invoked only by reflection, over the type argument recovered from the writer's base type.</remarks>
    private static LocalChannelSink<T> BoxChannelSink<T>(ChannelWriter<T> writer) => new(writer);
}
