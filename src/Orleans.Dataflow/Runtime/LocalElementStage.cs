namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One element-to-element stage of a compiled run: a mapping or a filter, already reduced to delegates
/// over boxed elements.
/// </summary>
/// <remarks>
/// <para>
/// A local graph's element types live in the C# type system and never in the document, so a runtime that
/// walks a chain whose element type changes at every mapping stage has no single type to be generic over.
/// The plan therefore speaks in <see cref="object"/>, and
/// <see cref="LocalDelegateAdapter"/> is the one place where the author's typed delegate is wrapped into
/// that shape. The wrapping happens once per stage per materialization; every element afterwards costs one
/// delegate call and, for a value element type, one box.
/// </para>
/// <para>
/// The two shapes are one type with one nullable field each, the way
/// <see cref="Authoring.LocalStageDescriptor"/> holds its behavior, rather than a small class hierarchy: a
/// stage is a closed set of two cases, and one type keeps the whole per-element decision on one screen.
/// </para>
/// </remarks>
internal sealed class LocalElementStage
{
    private readonly Func<object?, object?>? _selector;
    private readonly Func<object?, bool>? _predicate;

    /// <summary>Initializes a new instance of the <see cref="LocalElementStage"/> class.</summary>
    /// <param name="selector">The mapping, or <see langword="null"/> when this stage filters.</param>
    /// <param name="predicate">The test, or <see langword="null"/> when this stage maps.</param>
    private LocalElementStage(Func<object?, object?>? selector, Func<object?, bool>? predicate)
    {
        _selector = selector;
        _predicate = predicate;
    }

    /// <summary>Creates a mapping stage.</summary>
    /// <param name="selector">The mapping over boxed elements.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Select(Func<object?, object?> selector) => new(selector, predicate: null);

    /// <summary>Creates a filtering stage.</summary>
    /// <param name="predicate">The test over boxed elements.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Where(Func<object?, bool> predicate) => new(selector: null, predicate);

    /// <summary>Pushes one element through this stage.</summary>
    /// <param name="element">The element arriving from upstream.</param>
    /// <param name="result">
    /// When this method returns <see langword="true"/>, the element to hand downstream; otherwise an
    /// unspecified value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the element continues downstream; <see langword="false"/> when this
    /// stage dropped it.
    /// </returns>
    /// <remarks>
    /// An exception the author's delegate throws is not caught here. It travels up to the run loop, which
    /// is the single place that decides what a stage failure does to a run, so that failure semantics are
    /// stated once rather than per stage.
    /// </remarks>
    internal bool TryApply(object? element, out object? result)
    {
        if (_selector is not null)
        {
            result = _selector(element);

            return true;
        }

        result = element;

        return _predicate!(element);
    }
}
