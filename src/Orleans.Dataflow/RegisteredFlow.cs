using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// A registered stage that transforms elements, paired with the CLR types its ports carry here.
/// </summary>
/// <typeparam name="TIn">The element type the stage consumes in this process.</typeparam>
/// <typeparam name="TOut">The element type the stage produces in this process.</typeparam>
/// <remarks>
/// <para>
/// The <see cref="Specification"/> was resolved and checked when the handle was created: the stage is
/// registered, it declares exactly one input port and one output port and no result port, and those two
/// ports carry the element contracts <see cref="Input"/> and <see cref="Output"/> declare.
/// </para>
/// <para>
/// The two type arguments are what make a registered stage compose in the same chain as a lambda one: the
/// C# compiler rejects a handle whose input type does not match the elements currently flowing, exactly as
/// it does for <see cref="Flow{TIn, TOut}"/>, and the contract equality checked at handle creation is what
/// ties that CLR-level guarantee to the document-level one.
/// </para>
/// </remarks>
public sealed class RegisteredFlow<TIn, TOut>
{
    /// <summary>Initializes a new instance of the <see cref="RegisteredFlow{TIn, TOut}"/> class.</summary>
    /// <param name="specification">The resolved, checked specification.</param>
    /// <param name="input">The element contract the stage's input port accepts.</param>
    /// <param name="output">The element contract the stage's output port carries.</param>
    internal RegisteredFlow(
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

    /// <summary>Gets the contract of the elements this stage consumes.</summary>
    public ElementContract<TIn> Input { get; }

    /// <summary>Gets the contract of the elements this stage produces.</summary>
    public ElementContract<TOut> Output { get; }

    /// <summary>Returns a one-line diagnostic summary of this handle.</summary>
    /// <returns>
    /// Text of the form <c>registered flow orleans-test/normalize@v1: order-created@v1 as OrderCreated -&gt;
    /// order-document@v1 as OrderDocument</c>.
    /// </returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"registered flow {Stage}: {Input} -> {Output}";
}
