using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// A registered junction that splits one stream into legs that all carry one contract, paired with the CLR
/// types its ports carry here.
/// </summary>
/// <typeparam name="TIn">The element type the junction consumes in this process.</typeparam>
/// <typeparam name="TOut">The element type every leg of the junction produces in this process.</typeparam>
/// <remarks>
/// <para>
/// The <see cref="Specification"/> was resolved and checked when the handle was created: the stage is
/// registered, it declares exactly one input port, exactly <see cref="Legs"/> output ports, and no result
/// port, and every one of those ports carries the element contract this handle declares for it. Every port
/// therefore carries a real contract, which is the whole reason a fan-out pipeline built from registered
/// stages closes with no seam a compiler has to forgive.
/// </para>
/// <para>
/// Leg order is the specification's own canonical port order, ordinal by port name. That is fixed by the
/// catalog rather than by the author, so a branch attached at position <c>n</c> is wired to the same port
/// in every process that resolves the stage, and a router the provider registered answers positions in that
/// same order.
/// </para>
/// <para>
/// What the junction <em>does</em> with the element — deliver it to every leg, to one leg with room, or to
/// the leg a function names — is the provider's, stated by the runtime its factory builds and never by this
/// handle. A document says which stage stands here and what its payload is; behavior is resolved by
/// identity, exactly as it is for every other registered stage.
/// </para>
/// </remarks>
public sealed class RegisteredFanOut<TIn, TOut>
{
    /// <summary>Initializes a new instance of the <see cref="RegisteredFanOut{TIn, TOut}"/> class.</summary>
    /// <param name="specification">The resolved, checked specification.</param>
    /// <param name="input">The element contract the junction's input port accepts.</param>
    /// <param name="output">The element contract every leg carries.</param>
    internal RegisteredFanOut(
        StageSpecification specification,
        ElementContract<TIn> input,
        ElementContract<TOut> output)
    {
        Specification = specification;
        Input = input;
        Output = output;
    }

    /// <summary>Gets the specification this handle resolved to.</summary>
    /// <value>The catalog entry, whose ports and parameter contract every occurrence declares.</value>
    public StageSpecification Specification { get; }

    /// <summary>Gets the reference every occurrence of this handle names in a document.</summary>
    public StageRef Stage => Specification.Stage;

    /// <summary>Gets the contract of the elements this junction consumes.</summary>
    public ElementContract<TIn> Input { get; }

    /// <summary>Gets the contract of the elements every leg of this junction carries.</summary>
    public ElementContract<TOut> Output { get; }

    /// <summary>Gets how many legs this junction has.</summary>
    /// <value>The number of output ports the specification declares, which is what a call must supply.</value>
    public int Legs => Specification.OutputPorts.Count;

    /// <summary>Returns a one-line diagnostic summary of this handle.</summary>
    /// <returns>
    /// Text of the form <c>registered fan-out orleans-test/split@v1: order-document@v1 as OrderDocument -&gt;
    /// 2 legs of order-document@v1 as OrderDocument</c>.
    /// </returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"registered fan-out {Stage}: {Input} -> {Legs} legs of {Output}");
}

/// <summary>
/// A registered junction that splits one stream into two legs carrying different contracts, paired with the
/// CLR types its ports carry here.
/// </summary>
/// <typeparam name="TIn">The element type the junction consumes in this process.</typeparam>
/// <typeparam name="TLeft">The element type the first leg produces in this process.</typeparam>
/// <typeparam name="TRight">The element type the second leg produces in this process.</typeparam>
/// <remarks>
/// <para>
/// The unzip-shaped fan-out: one row in, two different things out. It is a separate handle rather than an
/// option on <see cref="RegisteredFanOut{TIn, TOut}"/> because the two legs have two element types, and a
/// type is not something a parameter can carry.
/// </para>
/// <para>
/// It is also the sharpest statement the registered surface makes about port contracts: one stage whose
/// three ports declare three different contracts, all of them checked here against what the author says
/// they are. Nothing about it needs an occurrence to override a specification's port contract, which is
/// what closes the question the adapters raised — a provider that wants other contracts registers another
/// stage.
/// </para>
/// <para>
/// First and second are the specification's own canonical port order, ordinal by port name, so which leg is
/// which is the catalog's statement rather than the author's.
/// </para>
/// </remarks>
public sealed class RegisteredFanOut<TIn, TLeft, TRight>
{
    /// <summary>Initializes a new instance of the <see cref="RegisteredFanOut{TIn, TLeft, TRight}"/> class.</summary>
    /// <param name="specification">The resolved, checked specification.</param>
    /// <param name="input">The element contract the junction's input port accepts.</param>
    /// <param name="left">The element contract the first leg carries.</param>
    /// <param name="right">The element contract the second leg carries.</param>
    internal RegisteredFanOut(
        StageSpecification specification,
        ElementContract<TIn> input,
        ElementContract<TLeft> left,
        ElementContract<TRight> right)
    {
        Specification = specification;
        Input = input;
        Left = left;
        Right = right;
    }

    /// <summary>Gets the specification this handle resolved to.</summary>
    /// <value>The catalog entry, whose ports and parameter contract every occurrence declares.</value>
    public StageSpecification Specification { get; }

    /// <summary>Gets the reference every occurrence of this handle names in a document.</summary>
    public StageRef Stage => Specification.Stage;

    /// <summary>Gets the contract of the elements this junction consumes.</summary>
    public ElementContract<TIn> Input { get; }

    /// <summary>Gets the contract of the elements the first leg carries.</summary>
    public ElementContract<TLeft> Left { get; }

    /// <summary>Gets the contract of the elements the second leg carries.</summary>
    public ElementContract<TRight> Right { get; }

    /// <summary>Returns a one-line diagnostic summary of this handle.</summary>
    /// <returns>Text naming the stage and the three contracts.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"registered fan-out {Stage}: {Input} -> {Left}, {Right}";
}
