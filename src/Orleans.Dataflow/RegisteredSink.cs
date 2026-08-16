using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// A registered stage that terminates a graph and declares no result.
/// </summary>
/// <typeparam name="TIn">The element type the stage consumes in this process.</typeparam>
/// <remarks>
/// <para>
/// The <see cref="Specification"/> was resolved and checked when the handle was created: the stage is
/// registered, it declares exactly one input port, no output port, and no result port, and the input port
/// carries the element contract <see cref="Input"/> declares.
/// </para>
/// <para>
/// A stage that does declare a result port is not this handle and is rejected as one, the same way a
/// <see cref="SinkWithResult{TIn, TResult}"/> is not a <see cref="Sink{TIn}"/>: a result a graph does not
/// name is a result nothing can read, and making it easy to end up with one is the accident ADR 0004
/// section 3 exists to prevent. Attach such a stage as a
/// <see cref="RegisteredSinkWithResult{TIn, TResult}"/> and name its slot.
/// </para>
/// </remarks>
public sealed class RegisteredSink<TIn>
{
    /// <summary>Initializes a new instance of the <see cref="RegisteredSink{TIn}"/> class.</summary>
    /// <param name="specification">The resolved, checked specification.</param>
    /// <param name="input">The element contract the stage's input port accepts.</param>
    internal RegisteredSink(StageSpecification specification, ElementContract<TIn> input)
    {
        Specification = specification;
        Input = input;
    }

    /// <summary>Gets the specification this handle resolved to.</summary>
    /// <value>The catalog entry, whose ports and parameter contract every occurrence declares.</value>
    public StageSpecification Specification { get; }

    /// <summary>Gets the reference every occurrence of this handle names in a document.</summary>
    public StageRef Stage => Specification.Stage;

    /// <summary>Gets the contract of the elements this stage consumes.</summary>
    public ElementContract<TIn> Input { get; }

    /// <summary>Returns a one-line diagnostic summary of this handle.</summary>
    /// <returns>Text of the form <c>registered sink orleans-test/index-sink@v1 &lt;- order-document@v1</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"registered sink {Stage} <- {Input}";
}
