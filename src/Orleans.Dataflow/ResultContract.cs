using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// The process-local association between one result contract and the CLR type that carries it here.
/// </summary>
/// <typeparam name="TResult">The CLR type this process binds to the contract.</typeparam>
/// <remarks>
/// <para>
/// The result-port counterpart of <see cref="ElementContract{T}"/>, and separate from it for the reason
/// the definition plane keeps element ports and result ports separate: a result is what a stage yields
/// once to whoever runs the graph, not what flows along an edge, and the two are declared by different
/// port kinds and checked by different compiler rules. A value of one kind is therefore never accepted
/// where the other is required.
/// </para>
/// <para>
/// Everything <see cref="ElementContract{T}"/> says about the CLR binding holds here too: the document
/// stores only <see cref="Reference"/>, the type argument is part of this type's identity, and two
/// processes agreeing on the reference while binding different CLR types have a deployment error the
/// definition plane cannot see.
/// </para>
/// </remarks>
public readonly record struct ResultContract<TResult>
{
    /// <summary>The diagnostic text <see cref="ToString"/> renders for the default value.</summary>
    private const string DefaultText = "(default ResultContract)";

    private readonly ContractReference _reference;

    /// <summary>Initializes a new instance of the <see cref="ResultContract{TResult}"/> struct.</summary>
    /// <param name="reference">The validated contract reference.</param>
    private ResultContract(ContractReference reference) => _reference = reference;

    /// <summary>Gets the reference a document stores for results of this contract.</summary>
    /// <value>A created <see cref="ContractReference"/>.</value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    public ContractReference Reference => IsDefault ? throw DefaultAccess() : _reference;

    /// <summary>Gets the CLR type this process binds to <see cref="Reference"/>.</summary>
    /// <value>Always <c>typeof(<typeparamref name="TResult"/>)</c>.</value>
    /// <remarks>
    /// Authoring-side metadata that never reaches a document, exposed for the same reason
    /// <see cref="ElementContract{T}.ElementType"/> is: the assertion should be readable in a diagnostic.
    /// </remarks>
    public Type ResultType => typeof(TResult);

    /// <summary>Gets a value indicating whether this instance is the uninitialized default.</summary>
    /// <value><see langword="true"/> when the instance names no contract.</value>
    public bool IsDefault => _reference.IsDefault;

    /// <summary>Creates a contract declaration over a validated reference.</summary>
    /// <param name="reference">The validated reference.</param>
    /// <returns>The declaration.</returns>
    /// <remarks>
    /// Internal because <see cref="ResultContract.For{TResult}(string, int)"/> is the one supported
    /// spelling, for the reason given on <see cref="ElementContract{T}.Of"/>.
    /// </remarks>
    internal static ResultContract<TResult> Of(ContractReference reference) => new(reference);

    /// <summary>Returns a diagnostic summary of this declaration.</summary>
    /// <returns>
    /// Text of the form <c>order-count@v1 as Int64</c>, or <c>"(default ResultContract)"</c> when
    /// <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => IsDefault ? DefaultText : $"{_reference} as {typeof(TResult).Name}";

    /// <summary>Builds the exception for reading the reference of the default instance.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException DefaultAccess() =>
        new($"The default {nameof(ResultContract<TResult>)} names no contract. Declare one with {nameof(ResultContract)}.{nameof(ResultContract.For)}, such as ResultContract.For<{typeof(TResult).Name}>(\"order-count\", 1).");
}

/// <summary>
/// The factory that declares a result contract.
/// </summary>
/// <remarks>
/// The non-generic companion of <see cref="ResultContract{TResult}"/>, spelled to read beside
/// <see cref="ElementContract.For{T}(string, int)"/>.
/// </remarks>
public static class ResultContract
{
    /// <summary>Declares that a result contract is carried by one CLR type in this process.</summary>
    /// <typeparam name="TResult">The CLR type carrying the contract.</typeparam>
    /// <param name="contractId">The contract identifier segment, such as <c>order-count</c>.</param>
    /// <param name="majorVersion">
    /// The compatibility major version, which must be at least
    /// <see cref="ContractReference.FirstMajorVersion"/>.
    /// </param>
    /// <returns>The declaration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="contractId"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="contractId"/> is not a valid identifier segment.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="majorVersion"/> is below <see cref="ContractReference.FirstMajorVersion"/>.
    /// </exception>
    public static ResultContract<TResult> For<TResult>(string contractId, int majorVersion)
    {
        ArgumentNullException.ThrowIfNull(contractId);

        ContractId contract;

        try
        {
            contract = ContractId.Create(contractId);
        }
        catch (ArgumentException failure)
        {
            throw new ArgumentException(failure.Message, nameof(contractId), failure);
        }

        return ResultContract<TResult>.Of(ContractReference.Create(contract, majorVersion));
    }
}
