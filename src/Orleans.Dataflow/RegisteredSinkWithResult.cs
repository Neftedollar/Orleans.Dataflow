using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// A registered stage that terminates a graph and declares one result.
/// </summary>
/// <typeparam name="TIn">The element type the stage consumes in this process.</typeparam>
/// <typeparam name="TResult">The type of the result the stage yields in this process.</typeparam>
/// <remarks>
/// <para>
/// The <see cref="Specification"/> was resolved and checked when the handle was created: the stage is
/// registered, it declares exactly one input port, no output port, and exactly one result port, and those
/// ports carry the contracts <see cref="Input"/> and <see cref="Result"/> declare.
/// </para>
/// <para>
/// Attaching this handle takes two names and they mean different things. The occurrence name is the node's
/// durable identity in the graph; the slot name is what a run handle resolves the result under. Both are
/// author-stable identities and neither is derivable from the other, so both are required and they are
/// separate parameters.
/// </para>
/// <para>
/// There is no spelling for attaching this handle and discarding its result, which is the one thing the
/// lambda surface's <see cref="SinkWithResult{TIn, TResult}.ToSink"/> can do and this one cannot. A
/// conversion to <see cref="RegisteredSink{TIn}"/> would produce a handle whose stage declares a result
/// port, contradicting the shape that handle is checked for, so the omission is deliberate rather than an
/// oversight; a graph that does not want the result names it and ignores it.
/// </para>
/// </remarks>
public sealed class RegisteredSinkWithResult<TIn, TResult>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RegisteredSinkWithResult{TIn, TResult}"/> class.
    /// </summary>
    /// <param name="specification">The resolved, checked specification.</param>
    /// <param name="input">The element contract the stage's input port accepts.</param>
    /// <param name="result">The contract the stage's result port yields.</param>
    internal RegisteredSinkWithResult(
        StageSpecification specification,
        ElementContract<TIn> input,
        ResultContract<TResult> result)
    {
        Specification = specification;
        Input = input;
        Result = result;
    }

    /// <summary>Gets the specification this handle resolved to.</summary>
    /// <value>The catalog entry, whose ports and parameter contract every occurrence declares.</value>
    public StageSpecification Specification { get; }

    /// <summary>Gets the reference every occurrence of this handle names in a document.</summary>
    public StageRef Stage => Specification.Stage;

    /// <summary>Gets the contract of the elements this stage consumes.</summary>
    public ElementContract<TIn> Input { get; }

    /// <summary>Gets the contract of the result this stage yields.</summary>
    public ResultContract<TResult> Result { get; }

    /// <summary>Returns a one-line diagnostic summary of this handle.</summary>
    /// <returns>
    /// Text of the form <c>registered sink with result orleans-test/count-sink@v1 &lt;- order-document@v1
    /// as OrderDocument =&gt; order-count@v1 as Int64</c>.
    /// </returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"registered sink with result {Stage} <- {Input} => {Result}";
}
