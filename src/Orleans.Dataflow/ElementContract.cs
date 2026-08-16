using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// The process-local association between one element contract and the CLR type that carries it here.
/// </summary>
/// <typeparam name="T">The CLR type this process binds to the contract.</typeparam>
/// <remarks>
/// <para>
/// The definition plane forbids CLR type names as contract identity, so a document stores only
/// <see cref="Reference"/> and nothing about <typeparamref name="T"/>. Declaring an
/// <see cref="ElementContract{T}"/> is deployment code's assertion that, in this process, the contract
/// named by the reference is carried by <typeparamref name="T"/>. Two processes that agree on the
/// reference and bind different CLR types have a deployment error the definition plane cannot see; that
/// limit is stated here rather than hidden, and the cross-silo check is the M3 catalog fingerprint plus
/// the serializer contracts.
/// </para>
/// <para>
/// <typeparamref name="T"/> is part of the type's identity, which is exactly what makes the assertion
/// enforceable at the authoring boundary: two contracts naming one reference but binding different CLR
/// types are values of two different types and are never equal, so a handle built for
/// <c>ElementContract&lt;OrderCreated&gt;</c> cannot be attached where
/// <c>ElementContract&lt;OrderDocument&gt;</c> is required even when both name <c>order@v1</c>.
/// </para>
/// <para>
/// The type is a readonly record struct because equality over the reference is its whole contract, the
/// reference is itself a readonly record struct whose value equality it composes, and that is how every
/// other small identity in this codebase is modeled. The default instance names no contract and says so.
/// </para>
/// </remarks>
public readonly record struct ElementContract<T>
{
    /// <summary>The diagnostic text <see cref="ToString"/> renders for the default value.</summary>
    private const string DefaultText = "(default ElementContract)";

    private readonly ContractReference _reference;

    /// <summary>Initializes a new instance of the <see cref="ElementContract{T}"/> struct.</summary>
    /// <param name="reference">The validated contract reference.</param>
    private ElementContract(ContractReference reference) => _reference = reference;

    /// <summary>Gets the reference a document stores for elements of this contract.</summary>
    /// <value>A created <see cref="ContractReference"/>.</value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    public ContractReference Reference => IsDefault ? throw DefaultAccess() : _reference;

    /// <summary>Gets the CLR type this process binds to <see cref="Reference"/>.</summary>
    /// <value>Always <c>typeof(<typeparamref name="T"/>)</c>.</value>
    /// <remarks>
    /// The type is authoring-side metadata and never reaches a document. It is exposed so that the
    /// assertion this value makes is readable in a diagnostic, and because a binding nothing can observe
    /// is a binding nothing can check. Reading it is defined for the default instance too, because the
    /// CLR type is a property of the declaration's static type rather than of its contents.
    /// </remarks>
    public Type ElementType => typeof(T);

    /// <summary>Gets a value indicating whether this instance is the uninitialized default.</summary>
    /// <value><see langword="true"/> when the instance names no contract.</value>
    public bool IsDefault => _reference.IsDefault;

    /// <summary>Creates a contract declaration over a validated reference.</summary>
    /// <param name="reference">The validated reference.</param>
    /// <returns>The declaration.</returns>
    /// <remarks>
    /// Internal because <see cref="ElementContract.For{T}(string, int)"/> is the one supported spelling:
    /// a declaration is written by deployment code from the contract's own name and version, and there is
    /// no second way to assert that some CLR type carries some contract.
    /// </remarks>
    internal static ElementContract<T> Of(ContractReference reference) => new(reference);

    /// <summary>Returns a diagnostic summary of this declaration.</summary>
    /// <returns>
    /// Text of the form <c>order-created@v1 as OrderCreated</c>, or <c>"(default ElementContract)"</c>
    /// when <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// Both halves of the assertion are rendered, because the reference alone would print one line for
    /// two declarations that are not equal. The method never throws.
    /// </remarks>
    public override string ToString() => IsDefault ? DefaultText : $"{_reference} as {typeof(T).Name}";

    /// <summary>Builds the exception for reading the reference of the default instance.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException DefaultAccess() =>
        new($"The default {nameof(ElementContract<T>)} names no contract. Declare one with {nameof(ElementContract)}.{nameof(ElementContract.For)}, such as ElementContract.For<{typeof(T).Name}>(\"order-created\", 1).");
}

/// <summary>
/// The factory that declares an element contract.
/// </summary>
/// <remarks>
/// The factory lives on a non-generic companion class so that the contract's name and version are written
/// beside the CLR type they are asserted of, per the same rule that puts <see cref="Source.From{T}"/> on a
/// companion of <see cref="Source{T}"/> (ADR 0004 section 1).
/// </remarks>
public static class ElementContract
{
    /// <summary>Declares that a contract is carried by one CLR type in this process.</summary>
    /// <typeparam name="T">The CLR type carrying the contract.</typeparam>
    /// <param name="contractId">The contract identifier segment, such as <c>order-created</c>.</param>
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
    /// <remarks>
    /// <see cref="ContractId"/> owns the segment grammar and the diagnostic for breaking it, so the message
    /// is reused verbatim rather than restated; only the parameter name is corrected, because the author
    /// wrote a contract identifier text and not a <see cref="ContractId"/> value.
    /// </remarks>
    public static ElementContract<T> For<T>(string contractId, int majorVersion)
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

        return ElementContract<T>.Of(ContractReference.Create(contract, majorVersion));
    }
}
