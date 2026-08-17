using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// What one host has registered for the .NET push adapters: the named observables its documents may
/// address.
/// </summary>
/// <remarks>
/// <para>
/// ADR 0001's rule made operational for the push bridges. A document names an observable; a deployment
/// names the code. Nothing a document says can reach code this registry does not already hold, and a
/// document naming something it does not hold is refused before a run exists.
/// </para>
/// <para>
/// Immutable once built, and built while the host is being built, so every run in the process answers the
/// same question the same way.
/// </para>
/// </remarks>
internal sealed class DotnetAdapterRegistry
{
    private readonly Dictionary<string, IObservableEntry> _observables;

    /// <summary>Initializes a new instance of the <see cref="DotnetAdapterRegistry"/> class.</summary>
    /// <param name="observables">The observable bindings, keyed by name.</param>
    private DotnetAdapterRegistry(Dictionary<string, IObservableEntry> observables) =>
        _observables = observables;

    /// <summary>Gets the registry that holds nothing.</summary>
    /// <value>
    /// The registry an authoring process validates against: it can check the shape of a payload and
    /// nothing about which names a host publishes, which is exactly the split between a catalog and a host.
    /// </value>
    internal static DotnetAdapterRegistry Empty { get; } = new([]);

    /// <summary>Gets a value indicating whether this registry checks names at all.</summary>
    /// <value><see langword="true"/> for <see cref="Empty"/> and for a host that registered nothing.</value>
    internal bool IsEmpty => _observables.Count == 0;

    /// <summary>Lists the observable names this host publishes, in ordinal order.</summary>
    /// <value>The names, sorted so that a diagnostic reads the same on every run.</value>
    internal IReadOnlyList<string> Observables
    {
        get
        {
            string[] sorted = [.. _observables.Keys];

            Array.Sort(sorted, StringComparer.Ordinal);

            return sorted;
        }
    }

    /// <summary>Resolves an observable by name.</summary>
    /// <param name="name">The name a payload carries.</param>
    /// <param name="source">When this method returns <see langword="true"/>, the binding.</param>
    /// <returns><see langword="true"/> when this host registers that observable.</returns>
    internal bool TryGetObservable(string name, out IObservableEntry? source) =>
        _observables.TryGetValue(name, out source);

    /// <summary>The accumulating builder a host's registration fills.</summary>
    /// <remarks>
    /// Accumulates and then refuses everything at once, exactly as the silo's own registration surface
    /// does: a deployment fixing one collision per restart learns the shape of the contract one restart at
    /// a time.
    /// </remarks>
    internal sealed class Builder
    {
        private readonly Dictionary<string, IObservableEntry> _observables = new(StringComparer.Ordinal);
        private readonly List<string> _violations = [];

        /// <summary>Gets a value indicating whether the .NET vocabulary was asked for.</summary>
        /// <value><see langword="true"/> once one binding or one explicit request has been added.</value>
        /// <remarks>
        /// This is what publishes the vocabulary. A host that asks for none of it keeps exactly the catalog
        /// it wrote — and therefore exactly the catalog fingerprint it had.
        /// </remarks>
        internal bool Any { get; private set; }

        /// <summary>Publishes the vocabulary without registering any binding.</summary>
        /// <remarks>
        /// The timer needs no registration, because it addresses nothing a deployment could have written:
        /// its whole configuration is a period and a bound. So a host that wants only the timer says so
        /// once, and a host that registers an observable gets the timer as part of the same vocabulary,
        /// because they ship as one and a half-published vocabulary would fail at the first element rather
        /// than at the start.
        /// </remarks>
        internal void Request() => Any = true;

        /// <summary>Registers one observable binding.</summary>
        /// <param name="source">The binding.</param>
        internal void Add(IObservableEntry source)
        {
            Any = true;

            if (!_observables.TryAdd(source.Name, source))
            {
                _violations.Add(
                    $"the observable '{source.Name}' is registered more than once, and a document addressing that name would have two answers");
            }
        }

        /// <summary>Resolves everything registered into the value a host reads.</summary>
        /// <returns>The registry.</returns>
        /// <exception cref="ArgumentException">One name was registered twice.</exception>
        internal DotnetAdapterRegistry Build() =>
            _violations.Count > 0
                ? throw new ArgumentException(
                    $"The .NET adapter registration breaks {_violations.Count} invariant{(_violations.Count == 1 ? string.Empty : "s")}: {string.Join("; ", _violations)}.")
                : new DotnetAdapterRegistry(_observables);
    }
}

/// <summary>
/// The check every .NET push adapter applies to a node's payload: the shape first, and then the names,
/// against whichever registry the process holds.
/// </summary>
/// <param name="registry">
/// The host's registry, or <see cref="DotnetAdapterRegistry.Empty"/> in a process that only validates
/// shapes.
/// </param>
/// <param name="kind">Which adapter this validator belongs to.</param>
/// <remarks>
/// The name check is deployment-scoped, exactly as the Orleans adapters' is and for the same reason: what
/// a validator must not depend on is the document it is validating, and what it may depend on is the
/// deployment that registered it. The shape half of every check is answerable anywhere, which is what an
/// authoring process gets.
/// </remarks>
internal sealed class DotnetStageValidator(DotnetAdapterRegistry registry, DotnetStageKind kind)
    : IStageParameterValidator
{
    /// <inheritdoc/>
    public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) => kind switch
    {
        DotnetStageKind.Timer => Timer(parameters),
        _ => Observable(parameters),
    };

    /// <summary>Checks a timer's payload.</summary>
    /// <param name="parameters">The payload.</param>
    /// <returns>The violations.</returns>
    private static IReadOnlyList<string> Timer(CanonicalJsonValue parameters) =>
        TimerPayload.TryRead(parameters, out TimerDeclaration? _, out IReadOnlyList<string> violations)
            ? []
            : violations;

    /// <summary>Checks an observable source's payload.</summary>
    /// <param name="parameters">The payload.</param>
    /// <returns>The violations.</returns>
    private IReadOnlyList<string> Observable(CanonicalJsonValue parameters)
    {
        if (!ObservablePayload.TryRead(
            parameters,
            out ObservableDeclaration? declaration,
            out IReadOnlyList<string> violations))
        {
            return violations;
        }

        if (registry.IsEmpty)
        {
            return [];
        }

        if (!registry.TryGetObservable(declaration!.Source, out IObservableEntry? source))
        {
            IReadOnlyList<string> known = registry.Observables;

            return
            [
                known.Count == 0
                    ? $"the observable '{declaration.Source}' is not registered in this host, which registers no observable at all"
                    : $"the observable '{declaration.Source}' is not registered in this host, which registers {string.Join(", ", known.Select(static one => $"'{one}'"))}",
            ];
        }

        string output = source!.Output.ToString();

        return string.Equals(output, declaration.Output, StringComparison.Ordinal)
            ? []
            : [
                $"the member '{ObservablePayload.OutputMember}' declares '{declaration.Output}' and this host registers '{declaration.Source}' over '{output}', so the document was written against a different signature",
            ];
    }
}

/// <summary>Which of the two .NET push adapters a validator or a factory is dealing with.</summary>
internal enum DotnetStageKind
{
    /// <summary>A run-scoped periodic tick source.</summary>
    Timer,

    /// <summary>A subscription to a named <see cref="IObservable{T}"/> that feeds a run's bounded ingress.</summary>
    Observable,
}
