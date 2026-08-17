using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// What one silo has registered for the Orleans adapters: the element contracts its streams may carry and
/// the named calls and enumerations its documents may address.
/// </summary>
/// <remarks>
/// <para>
/// This is ADR 0001's rule made operational for the adapters. A document names a call, a source, or an
/// element contract; a deployment names the code. Nothing a document says can reach code this registry does
/// not already hold, and a document naming something it does not hold is refused before a run exists.
/// </para>
/// <para>
/// Immutable once built, and built while the silo is being built, so every activation in the silo answers
/// the same question the same way — the property the run grain's materialization depends on.
/// </para>
/// </remarks>
internal sealed class OrleansAdapterRegistry
{
    private readonly Dictionary<string, IStreamElementEntry> _elements;
    private readonly Dictionary<string, IGrainCallEntry> _calls;
    private readonly Dictionary<string, IGrainCallSinkEntry> _callSinks;
    private readonly Dictionary<string, IGrainEnumerableEntry> _enumerables;

    /// <summary>Initializes a new instance of the <see cref="OrleansAdapterRegistry"/> class.</summary>
    /// <param name="elements">The stream element bindings, keyed by contract text.</param>
    /// <param name="calls">The transforming call bindings, keyed by name.</param>
    /// <param name="callSinks">The terminating call bindings, keyed by name.</param>
    /// <param name="enumerables">The enumeration bindings, keyed by name.</param>
    private OrleansAdapterRegistry(
        Dictionary<string, IStreamElementEntry> elements,
        Dictionary<string, IGrainCallEntry> calls,
        Dictionary<string, IGrainCallSinkEntry> callSinks,
        Dictionary<string, IGrainEnumerableEntry> enumerables)
    {
        _elements = elements;
        _calls = calls;
        _callSinks = callSinks;
        _enumerables = enumerables;
    }

    /// <summary>Gets the registry that holds nothing.</summary>
    /// <value>
    /// The registry an authoring process validates against: it can check the shape of a payload and
    /// nothing about which names a silo publishes, which is exactly the split between a catalog and a host.
    /// </value>
    internal static OrleansAdapterRegistry Empty { get; } = new([], [], [], []);

    /// <summary>Gets a value indicating whether this registry checks names at all.</summary>
    /// <value><see langword="true"/> for <see cref="Empty"/> and for a silo that registered nothing.</value>
    internal bool IsEmpty =>
        _elements.Count == 0 && _calls.Count == 0 && _callSinks.Count == 0 && _enumerables.Count == 0;

    /// <summary>Resolves a stream element contract by its canonical text.</summary>
    /// <param name="contract">The contract text a payload carries.</param>
    /// <param name="element">When this method returns <see langword="true"/>, the binding.</param>
    /// <returns><see langword="true"/> when this silo binds a CLR type to that contract for streams.</returns>
    internal bool TryGetElement(string contract, out IStreamElementEntry? element) =>
        _elements.TryGetValue(contract, out element);

    /// <summary>Resolves a transforming call by name.</summary>
    /// <param name="name">The name a payload carries.</param>
    /// <param name="call">When this method returns <see langword="true"/>, the binding.</param>
    /// <returns><see langword="true"/> when this silo registers that call.</returns>
    internal bool TryGetCall(string name, out IGrainCallEntry? call) => _calls.TryGetValue(name, out call);

    /// <summary>Resolves a terminating call by name.</summary>
    /// <param name="name">The name a payload carries.</param>
    /// <param name="call">When this method returns <see langword="true"/>, the binding.</param>
    /// <returns><see langword="true"/> when this silo registers that call.</returns>
    internal bool TryGetCallSink(string name, out IGrainCallSinkEntry? call) =>
        _callSinks.TryGetValue(name, out call);

    /// <summary>Resolves an enumeration by name.</summary>
    /// <param name="name">The name a payload carries.</param>
    /// <param name="source">When this method returns <see langword="true"/>, the binding.</param>
    /// <returns><see langword="true"/> when this silo registers that source.</returns>
    internal bool TryGetEnumerable(string name, out IGrainEnumerableEntry? source) =>
        _enumerables.TryGetValue(name, out source);

    /// <summary>Lists the stream element contracts this silo publishes, in ordinal order.</summary>
    /// <returns>The contract texts.</returns>
    internal IReadOnlyList<string> Elements => Sorted(_elements.Keys);

    /// <summary>Lists the transforming call names this silo publishes, in ordinal order.</summary>
    /// <returns>The names.</returns>
    internal IReadOnlyList<string> Calls => Sorted(_calls.Keys);

    /// <summary>Lists the terminating call names this silo publishes, in ordinal order.</summary>
    /// <returns>The names.</returns>
    internal IReadOnlyList<string> CallSinks => Sorted(_callSinks.Keys);

    /// <summary>Lists the enumeration names this silo publishes, in ordinal order.</summary>
    /// <returns>The names.</returns>
    internal IReadOnlyList<string> Enumerables => Sorted(_enumerables.Keys);

    /// <summary>Sorts a key set so a diagnostic reads the same on every run.</summary>
    /// <param name="keys">The keys.</param>
    /// <returns>The sorted keys.</returns>
    private static string[] Sorted(IEnumerable<string> keys)
    {
        string[] sorted = [.. keys];

        Array.Sort(sorted, StringComparer.Ordinal);

        return sorted;
    }

    /// <summary>The accumulating builder a silo registration fills.</summary>
    /// <remarks>
    /// Accumulates and then refuses everything at once, exactly as the silo's own registration surface
    /// does: a deployment fixing one collision per restart learns the shape of the contract one restart at
    /// a time.
    /// </remarks>
    internal sealed class Builder
    {
        private readonly Dictionary<string, IStreamElementEntry> _elements = new(StringComparer.Ordinal);
        private readonly Dictionary<string, IGrainCallEntry> _calls = new(StringComparer.Ordinal);
        private readonly Dictionary<string, IGrainCallSinkEntry> _callSinks = new(StringComparer.Ordinal);
        private readonly Dictionary<string, IGrainEnumerableEntry> _enumerables = new(StringComparer.Ordinal);
        private readonly List<string> _violations = [];

        /// <summary>Gets a value indicating whether anything at all has been registered.</summary>
        /// <value><see langword="true"/> once one binding of any kind has been added.</value>
        /// <remarks>
        /// This is what publishes the adapter vocabulary. A silo that registers no Orleans binding
        /// registers no Orleans stage either, so a deployment that does not use the adapters keeps exactly
        /// the catalog it wrote — and therefore exactly the catalog fingerprint it had.
        /// </remarks>
        internal bool Any { get; private set; }

        /// <summary>Registers one stream element binding.</summary>
        /// <param name="element">The binding.</param>
        internal void Add(IStreamElementEntry element)
        {
            Any = true;

            string contract = element.Contract.ToString();

            if (!_elements.TryAdd(contract, element))
            {
                _violations.Add(
                    $"the stream element contract '{contract}' is registered more than once, and one contract is carried by one CLR type in one silo");
            }
        }

        /// <summary>Registers one transforming call binding.</summary>
        /// <param name="call">The binding.</param>
        internal void Add(IGrainCallEntry call)
        {
            Any = true;

            if (!_calls.TryAdd(call.Name, call))
            {
                _violations.Add(
                    $"the grain call '{call.Name}' is registered more than once, and a document addressing that name would have two answers");
            }
        }

        /// <summary>Registers one terminating call binding.</summary>
        /// <param name="call">The binding.</param>
        internal void Add(IGrainCallSinkEntry call)
        {
            Any = true;

            if (!_callSinks.TryAdd(call.Name, call))
            {
                _violations.Add(
                    $"the grain call sink '{call.Name}' is registered more than once, and a document addressing that name would have two answers");
            }
        }

        /// <summary>Registers one enumeration binding.</summary>
        /// <param name="source">The binding.</param>
        internal void Add(IGrainEnumerableEntry source)
        {
            Any = true;

            if (!_enumerables.TryAdd(source.Name, source))
            {
                _violations.Add(
                    $"the grain enumerable '{source.Name}' is registered more than once, and a document addressing that name would have two answers");
            }
        }

        /// <summary>Resolves everything registered into the value the silo reads.</summary>
        /// <returns>The registry.</returns>
        /// <exception cref="ArgumentException">One name or contract was registered twice.</exception>
        internal OrleansAdapterRegistry Build() =>
            _violations.Count > 0
                ? throw new ArgumentException(
                    $"The Orleans adapter registration breaks {_violations.Count} invariant{(_violations.Count == 1 ? string.Empty : "s")}: {string.Join("; ", _violations)}.")
                : new OrleansAdapterRegistry(_elements, _calls, _callSinks, _enumerables);
    }
}

/// <summary>
/// The check every Orleans adapter applies to a node's payload: the shape first, and then the names,
/// against whichever registry the process holds.
/// </summary>
/// <param name="registry">
/// The silo's registry, or <see cref="OrleansAdapterRegistry.Empty"/> in a process that only validates
/// shapes.
/// </param>
/// <param name="kind">Which adapter this validator belongs to.</param>
/// <remarks>
/// <para>
/// The name check is deployment-scoped, and that is deliberate rather than an oversight of
/// <see cref="IStageParameterValidator"/>'s purity rule. What a validator must not depend on is the
/// document it is validating; what it may depend on is the deployment that registered it — the same thing
/// a catalog itself depends on, and the same reason a stage missing from a silo's catalog is a refusal
/// there and not here. The shape half of every check is answerable anywhere, which is why an authoring
/// process gets exactly that half.
/// </para>
/// <para>
/// The consequence is stated rather than hidden: two silos whose registries differ accept different
/// documents while publishing one catalog fingerprint, because a validator is behavior and never reaches a
/// fingerprint. That is the limit <see cref="StageSpecification.ParameterValidator"/> already documents,
/// and the refusal a document meets on the wrong silo names the missing binding and lists the ones that
/// silo does publish.
/// </para>
/// </remarks>
internal sealed class OrleansStageValidator(OrleansAdapterRegistry registry, OrleansStageKind kind)
    : IStageParameterValidator
{
    /// <inheritdoc/>
    public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) => kind switch
    {
        OrleansStageKind.StreamSource => StreamSource(parameters),
        OrleansStageKind.StreamSink => StreamSink(parameters),
        OrleansStageKind.GrainCall => GrainCall(parameters),
        OrleansStageKind.GrainCallSink => GrainCallSink(parameters),
        _ => GrainEnumerable(parameters),
    };

    /// <summary>Reports every way an unregistered name is wrong.</summary>
    /// <param name="what">What kind of thing the name addresses, in prose.</param>
    /// <param name="name">The name the payload carried.</param>
    /// <param name="known">The names this silo does publish.</param>
    /// <returns>The violation fragment.</returns>
    private static string Unregistered(string what, string name, IReadOnlyList<string> known) =>
        known.Count == 0
            ? $"the {what} '{name}' is not registered in this silo, which registers no {what} at all"
            : $"the {what} '{name}' is not registered in this silo, which registers {string.Join(", ", known.Select(static one => $"'{one}'"))}";

    /// <summary>Reports a registration whose contract is not the one the document was written against.</summary>
    /// <param name="member">The payload member that named the contract.</param>
    /// <param name="name">The name the payload carried.</param>
    /// <param name="declared">The contract the document declares.</param>
    /// <param name="registered">The contract this silo registered.</param>
    /// <returns>The violation fragment.</returns>
    private static string Mismatch(string member, string name, string declared, string registered) =>
        $"the member '{member}' declares '{declared}' and this silo registers '{name}' over '{registered}', so the document was written against a different signature";

    /// <summary>Checks a stream source's payload.</summary>
    /// <param name="parameters">The payload.</param>
    /// <returns>The violations.</returns>
    private IReadOnlyList<string> StreamSource(CanonicalJsonValue parameters)
    {
        if (!StreamSourcePayload.TryRead(
            parameters,
            out StreamSourceDeclaration? declaration,
            out IReadOnlyList<string> violations))
        {
            return violations;
        }

        return Element(declaration!.Element);
    }

    /// <summary>Checks a stream sink's payload.</summary>
    /// <param name="parameters">The payload.</param>
    /// <returns>The violations.</returns>
    private IReadOnlyList<string> StreamSink(CanonicalJsonValue parameters)
    {
        if (!StreamSinkPayload.TryRead(
            parameters,
            out StreamSinkDeclaration? declaration,
            out IReadOnlyList<string> violations))
        {
            return violations;
        }

        return Element(declaration!.Element);
    }

    /// <summary>Checks that a stream element contract is one this silo carries.</summary>
    /// <param name="element">The contract text.</param>
    /// <returns>The violations.</returns>
    private IReadOnlyList<string> Element(string element)
    {
        if (registry.IsEmpty)
        {
            return [];
        }

        return registry.TryGetElement(element, out IStreamElementEntry? _)
            ? []
            : [Unregistered("stream element contract", element, registry.Elements)];
    }

    /// <summary>Checks a transforming grain call's payload.</summary>
    /// <param name="parameters">The payload.</param>
    /// <returns>The violations.</returns>
    private IReadOnlyList<string> GrainCall(CanonicalJsonValue parameters)
    {
        if (!GrainCallPayload.TryRead(
            parameters,
            expectsOutput: true,
            out GrainCallDeclaration? declaration,
            out IReadOnlyList<string> violations))
        {
            return violations;
        }

        if (registry.IsEmpty)
        {
            return [];
        }

        if (!registry.TryGetCall(declaration!.Call, out IGrainCallEntry? call))
        {
            return [Unregistered("grain call", declaration.Call, registry.Calls)];
        }

        List<string> found = [];
        string input = call!.Input.ToString();
        string output = call.Output.ToString();

        if (!string.Equals(input, declaration.Input, StringComparison.Ordinal))
        {
            found.Add(Mismatch(GrainCallPayload.InputMember, declaration.Call, declaration.Input, input));
        }

        if (!string.Equals(output, declaration.Output, StringComparison.Ordinal))
        {
            found.Add(Mismatch(GrainCallPayload.OutputMember, declaration.Call, declaration.Output!, output));
        }

        return found;
    }

    /// <summary>Checks a terminating grain call's payload.</summary>
    /// <param name="parameters">The payload.</param>
    /// <returns>The violations.</returns>
    private IReadOnlyList<string> GrainCallSink(CanonicalJsonValue parameters)
    {
        if (!GrainCallPayload.TryRead(
            parameters,
            expectsOutput: false,
            out GrainCallDeclaration? declaration,
            out IReadOnlyList<string> violations))
        {
            return violations;
        }

        if (registry.IsEmpty)
        {
            return [];
        }

        if (!registry.TryGetCallSink(declaration!.Call, out IGrainCallSinkEntry? call))
        {
            return [Unregistered("grain call sink", declaration.Call, registry.CallSinks)];
        }

        string input = call!.Input.ToString();

        return string.Equals(input, declaration.Input, StringComparison.Ordinal)
            ? []
            : [Mismatch(GrainCallPayload.InputMember, declaration.Call, declaration.Input, input)];
    }

    /// <summary>Checks a grain enumeration's payload.</summary>
    /// <param name="parameters">The payload.</param>
    /// <returns>The violations.</returns>
    private IReadOnlyList<string> GrainEnumerable(CanonicalJsonValue parameters)
    {
        if (!GrainEnumerablePayload.TryRead(
            parameters,
            out GrainEnumerableDeclaration? declaration,
            out IReadOnlyList<string> violations))
        {
            return violations;
        }

        if (registry.IsEmpty)
        {
            return [];
        }

        if (!registry.TryGetEnumerable(declaration!.Source, out IGrainEnumerableEntry? source))
        {
            return [Unregistered("grain enumerable", declaration.Source, registry.Enumerables)];
        }

        string output = source!.Output.ToString();

        return string.Equals(output, declaration.Output, StringComparison.Ordinal)
            ? []
            : [Mismatch(GrainEnumerablePayload.OutputMember, declaration.Source, declaration.Output, output)];
    }
}

/// <summary>Which of the five Orleans adapters a validator or a factory is dealing with.</summary>
internal enum OrleansStageKind
{
    /// <summary>A subscription that feeds a run's bounded ingress.</summary>
    StreamSource,

    /// <summary>A publication awaited per element.</summary>
    StreamSink,

    /// <summary>An awaited grain call that transforms elements.</summary>
    GrainCall,

    /// <summary>An awaited grain call that terminates a graph.</summary>
    GrainCallSink,

    /// <summary>A grain enumeration that heads a run.</summary>
    GrainEnumerable,
}
