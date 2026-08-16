using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Tests.Identity;

/// <summary>
/// Runs the shared single-segment identifier contract against <see cref="ProviderId"/>.
/// </summary>
public sealed class ProviderIdTests : SimpleIdentifierContractTests<ProviderId>
{
    /// <inheritdoc/>
    protected override string IdentifierName => nameof(ProviderId);

    /// <inheritdoc/>
    protected override ProviderId Create(string value) => ProviderId.Create(value);

    /// <inheritdoc/>
    protected override bool TryCreate(string? value, out ProviderId identifier) => ProviderId.TryCreate(value, out identifier);

    /// <inheritdoc/>
    protected override string ValueOf(ProviderId identifier) => identifier.Value;

    /// <inheritdoc/>
    protected override bool IsDefaultOf(ProviderId identifier) => identifier.IsDefault;

    /// <inheritdoc/>
    protected override bool OperatorEquals(ProviderId left, ProviderId right) => left == right;

    /// <inheritdoc/>
    protected override bool OperatorNotEquals(ProviderId left, ProviderId right) => left != right;

    /// <inheritdoc/>
    protected override bool OperatorLess(ProviderId left, ProviderId right) => left < right;

    /// <inheritdoc/>
    protected override bool OperatorLessOrEqual(ProviderId left, ProviderId right) => left <= right;

    /// <inheritdoc/>
    protected override bool OperatorGreater(ProviderId left, ProviderId right) => left > right;

    /// <inheritdoc/>
    protected override bool OperatorGreaterOrEqual(ProviderId left, ProviderId right) => left >= right;
}

/// <summary>
/// Runs the shared single-segment identifier contract against <see cref="StageId"/>.
/// </summary>
public sealed class StageIdTests : SimpleIdentifierContractTests<StageId>
{
    /// <inheritdoc/>
    protected override string IdentifierName => nameof(StageId);

    /// <inheritdoc/>
    protected override StageId Create(string value) => StageId.Create(value);

    /// <inheritdoc/>
    protected override bool TryCreate(string? value, out StageId identifier) => StageId.TryCreate(value, out identifier);

    /// <inheritdoc/>
    protected override string ValueOf(StageId identifier) => identifier.Value;

    /// <inheritdoc/>
    protected override bool IsDefaultOf(StageId identifier) => identifier.IsDefault;

    /// <inheritdoc/>
    protected override bool OperatorEquals(StageId left, StageId right) => left == right;

    /// <inheritdoc/>
    protected override bool OperatorNotEquals(StageId left, StageId right) => left != right;

    /// <inheritdoc/>
    protected override bool OperatorLess(StageId left, StageId right) => left < right;

    /// <inheritdoc/>
    protected override bool OperatorLessOrEqual(StageId left, StageId right) => left <= right;

    /// <inheritdoc/>
    protected override bool OperatorGreater(StageId left, StageId right) => left > right;

    /// <inheritdoc/>
    protected override bool OperatorGreaterOrEqual(StageId left, StageId right) => left >= right;
}

/// <summary>
/// Runs the shared single-segment identifier contract against <see cref="GraphId"/>.
/// </summary>
public sealed class GraphIdTests : SimpleIdentifierContractTests<GraphId>
{
    /// <inheritdoc/>
    protected override string IdentifierName => nameof(GraphId);

    /// <inheritdoc/>
    protected override GraphId Create(string value) => GraphId.Create(value);

    /// <inheritdoc/>
    protected override bool TryCreate(string? value, out GraphId identifier) => GraphId.TryCreate(value, out identifier);

    /// <inheritdoc/>
    protected override string ValueOf(GraphId identifier) => identifier.Value;

    /// <inheritdoc/>
    protected override bool IsDefaultOf(GraphId identifier) => identifier.IsDefault;

    /// <inheritdoc/>
    protected override bool OperatorEquals(GraphId left, GraphId right) => left == right;

    /// <inheritdoc/>
    protected override bool OperatorNotEquals(GraphId left, GraphId right) => left != right;

    /// <inheritdoc/>
    protected override bool OperatorLess(GraphId left, GraphId right) => left < right;

    /// <inheritdoc/>
    protected override bool OperatorLessOrEqual(GraphId left, GraphId right) => left <= right;

    /// <inheritdoc/>
    protected override bool OperatorGreater(GraphId left, GraphId right) => left > right;

    /// <inheritdoc/>
    protected override bool OperatorGreaterOrEqual(GraphId left, GraphId right) => left >= right;
}

/// <summary>
/// Runs the shared single-segment identifier contract against <see cref="PortId"/>.
/// </summary>
public sealed class PortIdTests : SimpleIdentifierContractTests<PortId>
{
    /// <inheritdoc/>
    protected override string IdentifierName => nameof(PortId);

    /// <inheritdoc/>
    protected override PortId Create(string value) => PortId.Create(value);

    /// <inheritdoc/>
    protected override bool TryCreate(string? value, out PortId identifier) => PortId.TryCreate(value, out identifier);

    /// <inheritdoc/>
    protected override string ValueOf(PortId identifier) => identifier.Value;

    /// <inheritdoc/>
    protected override bool IsDefaultOf(PortId identifier) => identifier.IsDefault;

    /// <inheritdoc/>
    protected override bool OperatorEquals(PortId left, PortId right) => left == right;

    /// <inheritdoc/>
    protected override bool OperatorNotEquals(PortId left, PortId right) => left != right;

    /// <inheritdoc/>
    protected override bool OperatorLess(PortId left, PortId right) => left < right;

    /// <inheritdoc/>
    protected override bool OperatorLessOrEqual(PortId left, PortId right) => left <= right;

    /// <inheritdoc/>
    protected override bool OperatorGreater(PortId left, PortId right) => left > right;

    /// <inheritdoc/>
    protected override bool OperatorGreaterOrEqual(PortId left, PortId right) => left >= right;
}

/// <summary>
/// Runs the shared single-segment identifier contract against <see cref="ResultSlotId"/>.
/// </summary>
public sealed class ResultSlotIdTests : SimpleIdentifierContractTests<ResultSlotId>
{
    /// <inheritdoc/>
    protected override string IdentifierName => nameof(ResultSlotId);

    /// <inheritdoc/>
    protected override ResultSlotId Create(string value) => ResultSlotId.Create(value);

    /// <inheritdoc/>
    protected override bool TryCreate(string? value, out ResultSlotId identifier) => ResultSlotId.TryCreate(value, out identifier);

    /// <inheritdoc/>
    protected override string ValueOf(ResultSlotId identifier) => identifier.Value;

    /// <inheritdoc/>
    protected override bool IsDefaultOf(ResultSlotId identifier) => identifier.IsDefault;

    /// <inheritdoc/>
    protected override bool OperatorEquals(ResultSlotId left, ResultSlotId right) => left == right;

    /// <inheritdoc/>
    protected override bool OperatorNotEquals(ResultSlotId left, ResultSlotId right) => left != right;

    /// <inheritdoc/>
    protected override bool OperatorLess(ResultSlotId left, ResultSlotId right) => left < right;

    /// <inheritdoc/>
    protected override bool OperatorLessOrEqual(ResultSlotId left, ResultSlotId right) => left <= right;

    /// <inheritdoc/>
    protected override bool OperatorGreater(ResultSlotId left, ResultSlotId right) => left > right;

    /// <inheritdoc/>
    protected override bool OperatorGreaterOrEqual(ResultSlotId left, ResultSlotId right) => left >= right;
}

/// <summary>
/// Runs the shared single-segment identifier contract against <see cref="ContractId"/>.
/// </summary>
public sealed class ContractIdTests : SimpleIdentifierContractTests<ContractId>
{
    /// <inheritdoc/>
    protected override string IdentifierName => nameof(ContractId);

    /// <inheritdoc/>
    protected override ContractId Create(string value) => ContractId.Create(value);

    /// <inheritdoc/>
    protected override bool TryCreate(string? value, out ContractId identifier) => ContractId.TryCreate(value, out identifier);

    /// <inheritdoc/>
    protected override string ValueOf(ContractId identifier) => identifier.Value;

    /// <inheritdoc/>
    protected override bool IsDefaultOf(ContractId identifier) => identifier.IsDefault;

    /// <inheritdoc/>
    protected override bool OperatorEquals(ContractId left, ContractId right) => left == right;

    /// <inheritdoc/>
    protected override bool OperatorNotEquals(ContractId left, ContractId right) => left != right;

    /// <inheritdoc/>
    protected override bool OperatorLess(ContractId left, ContractId right) => left < right;

    /// <inheritdoc/>
    protected override bool OperatorLessOrEqual(ContractId left, ContractId right) => left <= right;

    /// <inheritdoc/>
    protected override bool OperatorGreater(ContractId left, ContractId right) => left > right;

    /// <inheritdoc/>
    protected override bool OperatorGreaterOrEqual(ContractId left, ContractId right) => left >= right;
}

/// <summary>
/// Runs the shared single-segment identifier contract against <see cref="RunId"/>.
/// </summary>
public sealed class RunIdTests : SimpleIdentifierContractTests<RunId>
{
    /// <inheritdoc/>
    protected override string IdentifierName => nameof(RunId);

    /// <inheritdoc/>
    protected override RunId Create(string value) => RunId.Create(value);

    /// <inheritdoc/>
    protected override bool TryCreate(string? value, out RunId identifier) => RunId.TryCreate(value, out identifier);

    /// <inheritdoc/>
    protected override string ValueOf(RunId identifier) => identifier.Value;

    /// <inheritdoc/>
    protected override bool IsDefaultOf(RunId identifier) => identifier.IsDefault;

    /// <inheritdoc/>
    protected override bool OperatorEquals(RunId left, RunId right) => left == right;

    /// <inheritdoc/>
    protected override bool OperatorNotEquals(RunId left, RunId right) => left != right;

    /// <inheritdoc/>
    protected override bool OperatorLess(RunId left, RunId right) => left < right;

    /// <inheritdoc/>
    protected override bool OperatorLessOrEqual(RunId left, RunId right) => left <= right;

    /// <inheritdoc/>
    protected override bool OperatorGreater(RunId left, RunId right) => left > right;

    /// <inheritdoc/>
    protected override bool OperatorGreaterOrEqual(RunId left, RunId right) => left >= right;
}

/// <summary>
/// Runs the shared single-segment identifier contract against <see cref="AttemptId"/>.
/// </summary>
public sealed class AttemptIdTests : SimpleIdentifierContractTests<AttemptId>
{
    /// <inheritdoc/>
    protected override string IdentifierName => nameof(AttemptId);

    /// <inheritdoc/>
    protected override AttemptId Create(string value) => AttemptId.Create(value);

    /// <inheritdoc/>
    protected override bool TryCreate(string? value, out AttemptId identifier) => AttemptId.TryCreate(value, out identifier);

    /// <inheritdoc/>
    protected override string ValueOf(AttemptId identifier) => identifier.Value;

    /// <inheritdoc/>
    protected override bool IsDefaultOf(AttemptId identifier) => identifier.IsDefault;

    /// <inheritdoc/>
    protected override bool OperatorEquals(AttemptId left, AttemptId right) => left == right;

    /// <inheritdoc/>
    protected override bool OperatorNotEquals(AttemptId left, AttemptId right) => left != right;

    /// <inheritdoc/>
    protected override bool OperatorLess(AttemptId left, AttemptId right) => left < right;

    /// <inheritdoc/>
    protected override bool OperatorLessOrEqual(AttemptId left, AttemptId right) => left <= right;

    /// <inheritdoc/>
    protected override bool OperatorGreater(AttemptId left, AttemptId right) => left > right;

    /// <inheritdoc/>
    protected override bool OperatorGreaterOrEqual(AttemptId left, AttemptId right) => left >= right;
}
