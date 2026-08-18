using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// A registered junction that joins several streams of one contract into one, paired with the CLR types its
/// ports carry here.
/// </summary>
/// <typeparam name="TIn">The element type every input of the junction consumes in this process.</typeparam>
/// <typeparam name="TOut">The element type the junction produces in this process.</typeparam>
/// <remarks>
/// <para>
/// The <see cref="Specification"/> was resolved and checked when the handle was created: the stage is
/// registered, it declares exactly <see cref="Inputs"/> input ports, exactly one output port, and no result
/// port, and every one of those ports carries the element contract this handle declares for it.
/// </para>
/// <para>
/// Input order is the specification's own canonical port order, ordinal by port name: the receiver of the
/// join reaches the first port, the first argument the second, and so on. That is what a concat consumes
/// in, an interleave rotates in, and a row-building junction builds its rows in, and it is fixed by the
/// catalog rather than by the author.
/// </para>
/// <para>
/// What the junction <em>does</em> with what it reads — emit whichever input has an element, read one input
/// to its end before the next, build a row — is the provider's, stated by the runtime its factory builds
/// and never by this handle.
/// </para>
/// </remarks>
public sealed class RegisteredFanIn<TIn, TOut>
{
    /// <summary>Initializes a new instance of the <see cref="RegisteredFanIn{TIn, TOut}"/> class.</summary>
    /// <param name="specification">The resolved, checked specification.</param>
    /// <param name="input">The element contract every input port accepts.</param>
    /// <param name="output">The element contract the junction's output port carries.</param>
    internal RegisteredFanIn(
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

    /// <summary>Gets the contract of the elements every input of this junction consumes.</summary>
    public ElementContract<TIn> Input { get; }

    /// <summary>Gets the contract of the elements this junction produces.</summary>
    public ElementContract<TOut> Output { get; }

    /// <summary>Gets how many streams this junction joins.</summary>
    /// <value>
    /// The number of input ports the specification declares, which is the receiver of a join plus the
    /// sources a call supplies.
    /// </value>
    public int Inputs => Specification.InputPorts.Count;

    /// <summary>Returns a one-line diagnostic summary of this handle.</summary>
    /// <returns>
    /// Text of the form <c>registered fan-in orleans-test/join@v1: 2 inputs of order-document@v1 as
    /// OrderDocument -&gt; order-document@v1 as OrderDocument</c>.
    /// </returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"registered fan-in {Stage}: {Inputs} inputs of {Input} -> {Output}");
}

/// <summary>
/// A registered junction that joins two streams of different contracts into one, paired with the CLR types
/// its ports carry here.
/// </summary>
/// <typeparam name="TFirst">The element type the first input consumes in this process.</typeparam>
/// <typeparam name="TSecond">The element type the second input consumes in this process.</typeparam>
/// <typeparam name="TOut">The element type the junction produces in this process.</typeparam>
/// <remarks>
/// <para>
/// The zip-shaped fan-in: two different things in, one row out. It is a separate handle rather than an
/// option on <see cref="RegisteredFanIn{TIn, TOut}"/> because the two inputs have two element types, and a
/// type is not something a parameter can carry.
/// </para>
/// <para>
/// First and second are the specification's own canonical port order, ordinal by port name: the receiver of
/// the join reaches the first port and the argument reaches the second.
/// </para>
/// </remarks>
public sealed class RegisteredFanIn<TFirst, TSecond, TOut>
{
    /// <summary>Initializes a new instance of the <see cref="RegisteredFanIn{TFirst, TSecond, TOut}"/> class.</summary>
    /// <param name="specification">The resolved, checked specification.</param>
    /// <param name="first">The element contract the first input port accepts.</param>
    /// <param name="second">The element contract the second input port accepts.</param>
    /// <param name="output">The element contract the junction's output port carries.</param>
    internal RegisteredFanIn(
        StageSpecification specification,
        ElementContract<TFirst> first,
        ElementContract<TSecond> second,
        ElementContract<TOut> output)
    {
        Specification = specification;
        First = first;
        Second = second;
        Output = output;
    }

    /// <summary>Gets the specification this handle resolved to.</summary>
    /// <value>The catalog entry, whose ports and parameter contract every occurrence declares.</value>
    public StageSpecification Specification { get; }

    /// <summary>Gets the reference every occurrence of this handle names in a document.</summary>
    public StageRef Stage => Specification.Stage;

    /// <summary>Gets the contract of the elements the first input consumes.</summary>
    public ElementContract<TFirst> First { get; }

    /// <summary>Gets the contract of the elements the second input consumes.</summary>
    public ElementContract<TSecond> Second { get; }

    /// <summary>Gets the contract of the elements this junction produces.</summary>
    public ElementContract<TOut> Output { get; }

    /// <summary>Returns a one-line diagnostic summary of this handle.</summary>
    /// <returns>Text naming the stage and the three contracts.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"registered fan-in {Stage}: {First}, {Second} -> {Output}";
}
