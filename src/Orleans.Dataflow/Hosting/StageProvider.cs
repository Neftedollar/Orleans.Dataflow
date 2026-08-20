using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// One provider's vocabulary written once: each stage's declaration and the code behind it, side by side,
/// producing both halves of the registration seam.
/// </summary>
/// <remarks>
/// <para>
/// A stage published by hand takes three artifacts in three places — a <see cref="StageSpecification"/> in a
/// catalog, a branch in an <see cref="IDataflowStageFactory"/>, and the line of registration that ties them
/// together — and nothing but the author keeps them in step. Adding a stage to the catalog and forgetting
/// the branch is a silo that accepts a document and refuses it at materialization; adding the branch and
/// forgetting the catalog entry is a stage nothing can name. Both are the same mistake, and both come from
/// stating one fact in two places.
/// </para>
/// <para>
/// This states it once. Each call declares a stage and the code that builds it in one expression;
/// <see cref="Catalog"/> is the definition half and the provider itself is the runtime half, so
/// <c>AddProvider</c> registers a vocabulary that cannot have drifted.
/// </para>
/// <para>
/// <b>The split underneath is untouched and still the point.</b> A process that only validates documents
/// wants a catalog and must not need a factory, so <see cref="Catalog"/> is a
/// <see cref="StageCatalog"/> like any other, publishable on its own, serializable, and fingerprinted the
/// same way; and <see cref="ILocalDataflowBuilder.AddCatalog"/> and
/// <see cref="ILocalDataflowBuilder.AddFactory"/> keep working exactly as they did for a provider whose two
/// halves genuinely ship apart — a catalog in a contracts package and a factory in the deployment that
/// implements it. What this type covers is the common case where one deployment does both, and it covers it
/// without taking the general case away.
/// </para>
/// <para>
/// Every stage is checked against this provider when it is declared: a reference belonging to somebody else
/// is refused here rather than becoming a catalog entry no factory of this provider will ever be asked for.
/// </para>
/// <para>
/// A provider is built and then used, and the first use closes it: reading <see cref="Catalog"/> or being
/// asked by a host to build a node. Declaring another stage afterwards is refused rather than silently
/// producing a second, different catalog while the first is already registered — and rather than adding to a
/// lookup table a running host is reading, which is how this type meets the thread-safety
/// <see cref="IDataflowStageFactory"/> requires of every implementation: what a host reads concurrently is
/// a table that can no longer change.
/// </para>
/// </remarks>
public sealed class StageProvider : IDataflowStageFactory
{
    /// <summary>The declared stages in declaration order, so the catalog reads as the source does.</summary>
    private readonly List<StageSpecification> _specifications = [];

    /// <summary>What builds each declared stage, by the reference that names it.</summary>
    private readonly Dictionary<StageRef, Func<DataflowStageRequest, DataflowStageRuntime>> _builders = [];

    /// <summary>The catalog once it has been read.</summary>
    private StageCatalog? _catalog;

    /// <summary>Whether this vocabulary has been used, and is therefore closed to further declarations.</summary>
    /// <remarks>
    /// Set by both halves of the seam: reading <see cref="Catalog"/> and being asked to build a node. Either
    /// one means a host is now relying on this vocabulary, and a stage declared afterwards would leave the
    /// registration describing something that no longer exists. It also makes the thread-safety
    /// <see cref="IDataflowStageFactory"/> requires of every implementation true rather than assumed: the
    /// dictionary a host reads concurrently is one nothing can be added to any more.
    /// </remarks>
    private bool _closed;

    /// <summary>Initializes a new instance of the <see cref="StageProvider"/> class.</summary>
    /// <param name="provider">The validated provider.</param>
    private StageProvider(ProviderId provider) => Provider = provider;

    /// <summary>Gets the provider whose stages this vocabulary declares.</summary>
    /// <value>A created <see cref="ProviderId"/>.</value>
    public ProviderId Provider { get; }

    /// <summary>Gets the definition half: the catalog of everything declared here.</summary>
    /// <value>A catalog whose specifications are the declared stages, in canonical order.</value>
    /// <exception cref="ArgumentException">
    /// The declared stages do not form a valid catalog, which for a provider built through these methods
    /// means one stage reference was declared twice.
    /// </exception>
    /// <remarks>
    /// Built once and remembered, and reading it closes the provider. A deployment that registered a catalog
    /// and then declared another stage would be running against a catalog that no longer describes it, and
    /// the refusal at the later declaration is what turns that into a line number rather than a document
    /// refused at run time.
    /// </remarks>
    public StageCatalog Catalog
    {
        get
        {
            _closed = true;

            return _catalog ??= StageCatalog.Create(_specifications);
        }
    }

    /// <summary>Starts a vocabulary for the provider named by this text.</summary>
    /// <param name="provider">The provider identifier segment, such as <c>weather</c>.</param>
    /// <returns>The empty vocabulary.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="provider"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="provider"/> is not a valid identifier segment.
    /// </exception>
    /// <remarks>
    /// <see cref="ProviderId"/> owns the segment grammar and the diagnostic for breaking it, so the message
    /// is reused verbatim and only the parameter name is corrected.
    /// </remarks>
    public static StageProvider Create(string provider)
    {
        ArgumentNullException.ThrowIfNull(provider);

        try
        {
            return new StageProvider(ProviderId.Create(provider));
        }
        catch (ArgumentException failure)
        {
            throw new ArgumentException(failure.Message, nameof(provider), failure);
        }
    }

    /// <summary>Starts a vocabulary for a provider the caller already holds.</summary>
    /// <param name="provider">The provider; must not be the default value.</param>
    /// <returns>The empty vocabulary.</returns>
    /// <exception cref="ArgumentException"><paramref name="provider"/> is the default value.</exception>
    public static StageProvider Create(ProviderId provider) =>
        provider.IsDefault
            ? throw new ArgumentException(
                $"A {nameof(StageProvider)} requires a created {nameof(ProviderId)}; the default {nameof(ProviderId)} names no provider.",
                nameof(provider))
            : new StageProvider(provider);

    /// <summary>Declares a stage that produces on one port and consumes nothing, and what builds it.</summary>
    /// <param name="stage">The stage reference; must belong to this provider.</param>
    /// <param name="parameterContract">The contract of this stage's payload.</param>
    /// <param name="outputPort">The one port the stage produces on.</param>
    /// <param name="build">What turns a node of this stage into the thing that runs.</param>
    /// <returns>This vocabulary, so stages chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="build"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> belongs to another provider or was already declared, or the declaration
    /// breaks a specification invariant.
    /// </exception>
    /// <exception cref="InvalidOperationException">This provider is closed.</exception>
    public StageProvider Source(
        StageRef stage,
        ContractReference parameterContract,
        OutputPortSpecification outputPort,
        Func<DataflowStageRequest, DataflowStageRuntime> build) =>
        Declare(stage, build, () => StageSpecification.Source(stage, parameterContract, outputPort));

    /// <summary>Declares a stage that produces on one port, checks its payloads, and what builds it.</summary>
    /// <param name="stage">The stage reference; must belong to this provider.</param>
    /// <param name="parameterContract">The contract of this stage's payload.</param>
    /// <param name="outputPort">The one port the stage produces on.</param>
    /// <param name="parameterValidator">The check to apply to parameter payloads.</param>
    /// <param name="build">What turns a node of this stage into the thing that runs.</param>
    /// <returns>This vocabulary, so stages chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="parameterValidator"/> or <paramref name="build"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> belongs to another provider or was already declared, or the declaration
    /// breaks a specification invariant.
    /// </exception>
    /// <exception cref="InvalidOperationException">This provider is closed.</exception>
    public StageProvider Source(
        StageRef stage,
        ContractReference parameterContract,
        OutputPortSpecification outputPort,
        IStageParameterValidator parameterValidator,
        Func<DataflowStageRequest, DataflowStageRuntime> build) =>
        Declare(
            stage,
            build,
            () => StageSpecification.Source(stage, parameterContract, outputPort, parameterValidator));

    /// <summary>Declares a stage that consumes on one port and produces on one, and what builds it.</summary>
    /// <param name="stage">The stage reference; must belong to this provider.</param>
    /// <param name="parameterContract">The contract of this stage's payload.</param>
    /// <param name="inputPort">The one port the stage consumes on.</param>
    /// <param name="outputPort">The one port the stage produces on.</param>
    /// <param name="build">What turns a node of this stage into the thing that runs.</param>
    /// <returns>This vocabulary, so stages chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="build"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> belongs to another provider or was already declared, or the declaration
    /// breaks a specification invariant.
    /// </exception>
    /// <exception cref="InvalidOperationException">This provider is closed.</exception>
    public StageProvider Flow(
        StageRef stage,
        ContractReference parameterContract,
        InputPortSpecification inputPort,
        OutputPortSpecification outputPort,
        Func<DataflowStageRequest, DataflowStageRuntime> build) =>
        Declare(stage, build, () => StageSpecification.Flow(stage, parameterContract, inputPort, outputPort));

    /// <summary>Declares a transforming stage that checks its payloads, and what builds it.</summary>
    /// <param name="stage">The stage reference; must belong to this provider.</param>
    /// <param name="parameterContract">The contract of this stage's payload.</param>
    /// <param name="inputPort">The one port the stage consumes on.</param>
    /// <param name="outputPort">The one port the stage produces on.</param>
    /// <param name="parameterValidator">The check to apply to parameter payloads.</param>
    /// <param name="build">What turns a node of this stage into the thing that runs.</param>
    /// <returns>This vocabulary, so stages chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="parameterValidator"/> or <paramref name="build"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> belongs to another provider or was already declared, or the declaration
    /// breaks a specification invariant.
    /// </exception>
    /// <exception cref="InvalidOperationException">This provider is closed.</exception>
    public StageProvider Flow(
        StageRef stage,
        ContractReference parameterContract,
        InputPortSpecification inputPort,
        OutputPortSpecification outputPort,
        IStageParameterValidator parameterValidator,
        Func<DataflowStageRequest, DataflowStageRuntime> build) =>
        Declare(
            stage,
            build,
            () => StageSpecification.Flow(
                stage,
                parameterContract,
                inputPort,
                outputPort,
                parameterValidator));

    /// <summary>Declares a stage that consumes on one port and yields no result, and what builds it.</summary>
    /// <param name="stage">The stage reference; must belong to this provider.</param>
    /// <param name="parameterContract">The contract of this stage's payload.</param>
    /// <param name="inputPort">The one port the stage consumes on.</param>
    /// <param name="build">What turns a node of this stage into the thing that runs.</param>
    /// <returns>This vocabulary, so stages chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="build"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> belongs to another provider or was already declared, or the declaration
    /// breaks a specification invariant.
    /// </exception>
    /// <exception cref="InvalidOperationException">This provider is closed.</exception>
    public StageProvider Sink(
        StageRef stage,
        ContractReference parameterContract,
        InputPortSpecification inputPort,
        Func<DataflowStageRequest, DataflowStageRuntime> build) =>
        Declare(stage, build, () => StageSpecification.Sink(stage, parameterContract, inputPort));

    /// <summary>Declares a terminal that yields no result, checks its payloads, and what builds it.</summary>
    /// <param name="stage">The stage reference; must belong to this provider.</param>
    /// <param name="parameterContract">The contract of this stage's payload.</param>
    /// <param name="inputPort">The one port the stage consumes on.</param>
    /// <param name="parameterValidator">The check to apply to parameter payloads.</param>
    /// <param name="build">What turns a node of this stage into the thing that runs.</param>
    /// <returns>This vocabulary, so stages chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="parameterValidator"/> or <paramref name="build"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> belongs to another provider or was already declared, or the declaration
    /// breaks a specification invariant.
    /// </exception>
    /// <exception cref="InvalidOperationException">This provider is closed.</exception>
    public StageProvider Sink(
        StageRef stage,
        ContractReference parameterContract,
        InputPortSpecification inputPort,
        IStageParameterValidator parameterValidator,
        Func<DataflowStageRequest, DataflowStageRuntime> build) =>
        Declare(
            stage,
            build,
            () => StageSpecification.Sink(stage, parameterContract, inputPort, parameterValidator));

    /// <summary>Declares a stage that consumes on one port and yields one result, and what builds it.</summary>
    /// <param name="stage">The stage reference; must belong to this provider.</param>
    /// <param name="parameterContract">The contract of this stage's payload.</param>
    /// <param name="inputPort">The one port the stage consumes on.</param>
    /// <param name="resultPort">The one port the run's value is read from.</param>
    /// <param name="build">What turns a node of this stage into the thing that runs.</param>
    /// <returns>This vocabulary, so stages chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="build"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> belongs to another provider or was already declared, or the declaration
    /// breaks a specification invariant.
    /// </exception>
    /// <exception cref="InvalidOperationException">This provider is closed.</exception>
    public StageProvider Sink(
        StageRef stage,
        ContractReference parameterContract,
        InputPortSpecification inputPort,
        ResultPortSpecification resultPort,
        Func<DataflowStageRequest, DataflowStageRuntime> build) =>
        Declare(stage, build, () => StageSpecification.Sink(stage, parameterContract, inputPort, resultPort));

    /// <summary>Declares a terminal that yields one result, checks its payloads, and what builds it.</summary>
    /// <param name="stage">The stage reference; must belong to this provider.</param>
    /// <param name="parameterContract">The contract of this stage's payload.</param>
    /// <param name="inputPort">The one port the stage consumes on.</param>
    /// <param name="resultPort">The one port the run's value is read from.</param>
    /// <param name="parameterValidator">The check to apply to parameter payloads.</param>
    /// <param name="build">What turns a node of this stage into the thing that runs.</param>
    /// <returns>This vocabulary, so stages chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="parameterValidator"/> or <paramref name="build"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> belongs to another provider or was already declared, or the declaration
    /// breaks a specification invariant.
    /// </exception>
    /// <exception cref="InvalidOperationException">This provider is closed.</exception>
    public StageProvider Sink(
        StageRef stage,
        ContractReference parameterContract,
        InputPortSpecification inputPort,
        ResultPortSpecification resultPort,
        IStageParameterValidator parameterValidator,
        Func<DataflowStageRequest, DataflowStageRuntime> build) =>
        Declare(
            stage,
            build,
            () => StageSpecification.Sink(
                stage,
                parameterContract,
                inputPort,
                resultPort,
                parameterValidator));

    /// <summary>Declares any stage at all from its specification, and what builds it.</summary>
    /// <param name="specification">The stage's whole declaration; its stage must belong to this provider.</param>
    /// <param name="build">What turns a node of that stage into the thing that runs.</param>
    /// <returns>This vocabulary, so stages chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="specification"/> or <paramref name="build"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The specification's stage belongs to another provider or was already declared.
    /// </exception>
    /// <exception cref="InvalidOperationException">This provider is closed.</exception>
    /// <remarks>
    /// The general form, and the one a junction or a stage that requires a capability of its host uses:
    /// <see cref="StageSpecification.Create"/> and the junction shape factories say things the named methods
    /// here deliberately do not, and this is where such a specification is paired with its code.
    /// </remarks>
    public StageProvider Add(
        StageSpecification specification,
        Func<DataflowStageRequest, DataflowStageRuntime> build)
    {
        ArgumentNullException.ThrowIfNull(specification);

        return Declare(specification.Stage, build, () => specification);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Explicit, so that <see cref="Create(string)"/> is the only <c>Create</c> an author sees on this type.
    /// A host reaches this through the interface, which is exactly how it reaches a hand-written factory.
    /// </remarks>
    DataflowStageRuntime IDataflowStageFactory.Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;

        _closed = true;

        if (!_builders.TryGetValue(node.Stage, out Func<DataflowStageRequest, DataflowStageRuntime>? build))
        {
            throw new InvalidOperationException(
                $"The node '{node.Id}' is an occurrence of '{node.Stage}', which the provider '{Provider}' does not implement. This vocabulary declares {DescribeDeclared()}.");
        }

        return build(request);
    }

    /// <summary>Records one stage's declaration and its code, after every check they have to pass.</summary>
    /// <param name="stage">The stage being declared.</param>
    /// <param name="build">What builds it.</param>
    /// <param name="declare">
    /// The specification, deferred so that a stage refused for belonging elsewhere is refused before its
    /// ports are validated: the first complaint an author should read is the one that explains the rest.
    /// </param>
    /// <returns>This vocabulary.</returns>
    private StageProvider Declare(
        StageRef stage,
        Func<DataflowStageRequest, DataflowStageRuntime> build,
        Func<StageSpecification> declare)
    {
        ArgumentNullException.ThrowIfNull(build);

        if (_closed)
        {
            throw new InvalidOperationException(
                $"The provider '{Provider}' was closed when it was first used — its catalog was read, or a host asked it to build a node — and '{stage}' is being declared after that. A deployment registers the vocabulary it declared; declaring a stage afterwards would leave the registration describing something that no longer exists, and would add to a table a running host is reading. Declare every stage before the vocabulary is registered.");
        }

        if (stage.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(StageProvider)} declaration requires a created {nameof(StageRef)}; the default {nameof(StageRef)} names no stage.",
                nameof(stage));
        }

        if (stage.Provider != Provider)
        {
            throw new ArgumentException(
                $"The stage '{stage}' belongs to the provider '{stage.Provider}', and this vocabulary declares the provider '{Provider}'. One factory serves one provider, so a stage of another provider declared here would be a catalog entry this factory is never asked to build.",
                nameof(stage));
        }

        if (_builders.ContainsKey(stage))
        {
            throw new ArgumentException(
                $"The stage '{stage}' is declared twice in the provider '{Provider}'. Two declarations of one reference are two answers to one question rather than a merge.",
                nameof(stage));
        }

        StageSpecification specification = declare();

        _specifications.Add(specification);
        _builders.Add(stage, build);

        return this;
    }

    /// <summary>Gets the types that hold this vocabulary's code.</summary>
    /// <value>The declaring types of the build delegates, without duplicates.</value>
    /// <remarks>
    /// A vocabulary declared here has no CLR type of its own: the factory a host registers is this library's
    /// <see cref="StageProvider"/>, not the author's. So the question "where does this provider's code live"
    /// has to be answered by the delegates it was given, whose declaring types — a class, or the closure type
    /// the compiler synthesized for a lambda — are in the author's own assembly and namespace.
    /// <para>
    /// Internal because it exists for one reader: the conformance kit's check that no core option type names
    /// anything of the provider under test, which keys off the factory's assembly and namespace and would
    /// otherwise measure this library against itself and report every core option that mentions a
    /// <c>Orleans.Dataflow.Hosting</c> type as this provider's.
    /// </para>
    /// </remarks>
    internal IReadOnlyList<Type> ImplementationTypes =>
        [.. _builders.Values
            .Select(static build => build.Method.DeclaringType)
            .OfType<Type>()
            .Distinct()];

    /// <summary>Renders what this vocabulary declares, for the diagnostic of a stage it does not.</summary>
    /// <returns>The declared stage references in canonical order, or a phrase saying there are none.</returns>
    private string DescribeDeclared() =>
        _builders.Count == 0
            ? "no stages at all"
            : string.Join(", ", _builders.Keys.Order().Select(stage => $"'{stage}'"));
}
