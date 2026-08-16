using System.Collections;
using System.Reflection;
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

    /// <summary>Reads a source binding as a sequence the run loop can enumerate.</summary>
    /// <param name="behavior">The bound sequence, as the authoring value received it.</param>
    /// <returns>The sequence, viewed through the non-generic interface every sequence implements.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a sequence.</exception>
    /// <remarks>
    /// The non-generic view is what makes the source reflection-free: every <c>IEnumerable&lt;T&gt;</c> is
    /// an <see cref="IEnumerable"/>, and the run loop only ever needs elements as <see cref="object"/>.
    /// </remarks>
    internal static IEnumerable Elements(object? behavior) =>
        behavior as IEnumerable ??
        throw new InvalidOperationException(
            $"A '{LocalStageKind.FromEnumerable}' stage must be bound to a sequence, and this one is bound to {Describe(behavior)}.");

    /// <summary>Wraps a mapping delegate into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;TIn, TOut&gt;</c>.</param>
    /// <returns>The wrapped mapping.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a one-argument function.</exception>
    internal static Func<object?, object?> Selector(object? behavior)
    {
        Type[] arguments = Arguments(behavior, typeof(Func<,>), LocalStageKind.Select, "Func<TIn, TOut>");

        return (Func<object?, object?>)Close(SelectorTemplate, [arguments[0], arguments[1]], behavior);
    }

    /// <summary>Wraps a predicate delegate into one over boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;T, bool&gt;</c>.</param>
    /// <returns>The wrapped predicate.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="behavior"/> is not a one-argument function returning <see cref="bool"/>.
    /// </exception>
    internal static Func<object?, bool> Predicate(object? behavior)
    {
        Type[] arguments = Arguments(behavior, typeof(Func<,>), LocalStageKind.Where, "Func<T, bool>");

        if (arguments[1] != typeof(bool))
        {
            throw Mismatch(behavior, LocalStageKind.Where, "Func<T, bool>");
        }

        return (Func<object?, bool>)Close(PredicateTemplate, [arguments[0]], behavior);
    }

    /// <summary>Wraps a folding delegate into one over boxed state and boxed elements.</summary>
    /// <param name="behavior">The bound <c>Func&lt;TState, T, TState&gt;</c>.</param>
    /// <returns>The wrapped folder.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="behavior"/> is not a two-argument function.</exception>
    internal static Func<object?, object?, object?> Folder(object? behavior)
    {
        Type[] arguments = Arguments(behavior, typeof(Func<,,>), LocalStageKind.Fold, "Func<TState, T, TState>");

        if (arguments[2] != arguments[0])
        {
            throw Mismatch(behavior, LocalStageKind.Fold, "Func<TState, T, TState>");
        }

        return (Func<object?, object?, object?>)Close(FolderTemplate, [arguments[0], arguments[1]], behavior);
    }

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

    /// <summary>Wraps a typed folder into one over boxed state and boxed elements.</summary>
    /// <typeparam name="TState">The state type, which is also the result type.</typeparam>
    /// <typeparam name="TIn">The element type the fold consumes.</typeparam>
    /// <param name="folder">The author's delegate.</param>
    /// <returns>The wrapper.</returns>
    /// <remarks>Invoked only by reflection, over the type arguments recovered from the delegate itself.</remarks>
    private static Func<object?, object?, object?> BoxFolder<TState, TIn>(Func<TState, TIn, TState> folder) =>
        (state, element) => folder((TState)state!, (TIn)element!);
}
