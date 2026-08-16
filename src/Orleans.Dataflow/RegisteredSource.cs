using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// A registered stage that starts a graph, paired with the CLR type its output elements have here.
/// </summary>
/// <typeparam name="TOut">The element type the stage produces in this process.</typeparam>
/// <remarks>
/// <para>
/// The handle exists so that a catalog entry can be attached with the same type safety a lambda stage
/// has. Its <see cref="Specification"/> was resolved and checked when the handle was created: the stage is
/// registered, it declares no input port and exactly one output port, and that port's element contract is
/// the one <see cref="Output"/> declares.
/// </para>
/// <para>
/// The handle carries the specification and not the catalog it came from. Everything an attachment needs
/// is on the specification, and a handle that held a catalog would suggest it can answer whether a graph
/// is valid — which is the graph compiler's question, asked against the catalog the host registered, not
/// the one the author happened to build the handle from.
/// </para>
/// <para>
/// A handle has no position and no name. Attaching it is what names an occurrence, and attaching the same
/// handle twice under two names is two occurrences of one stage.
/// </para>
/// </remarks>
public sealed class RegisteredSource<TOut>
{
    /// <summary>Initializes a new instance of the <see cref="RegisteredSource{TOut}"/> class.</summary>
    /// <param name="specification">The resolved, checked specification.</param>
    /// <param name="output">The element contract the stage's output port carries.</param>
    internal RegisteredSource(StageSpecification specification, ElementContract<TOut> output)
    {
        Specification = specification;
        Output = output;
    }

    /// <summary>Gets the specification this handle resolved to.</summary>
    /// <value>The catalog entry, whose ports and parameter contract every occurrence declares.</value>
    public StageSpecification Specification { get; }

    /// <summary>Gets the reference every occurrence of this handle names in a document.</summary>
    public StageRef Stage => Specification.Stage;

    /// <summary>Gets the contract of the elements this stage produces.</summary>
    public ElementContract<TOut> Output { get; }

    /// <summary>Returns a one-line diagnostic summary of this handle.</summary>
    /// <returns>Text of the form <c>registered source orleans-test/order-source@v1 -&gt; order-created@v1</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"registered source {Stage} -> {Output}";
}
