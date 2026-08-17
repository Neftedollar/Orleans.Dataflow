using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Hosting;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// The probe that answers what <c>ReminderOptions.MinimumReminderPeriod</c> is and what a cluster does with
/// a period below it.
/// </summary>
/// <remarks>
/// <para>
/// This was the last named unknown in the M3 research notes: the option is documented to exist and its
/// default and enforcement mode are not, so nothing was allowed to claim either until a cluster had been
/// asked. These tests are that question, and they stay in the suite so the answer is re-checked against
/// every Orleans version this repository builds against rather than remembered from one.
/// </para>
/// <para>
/// The answer, recorded here and depended on by the reminder trigger: the default is one minute, and the
/// floor is enforced by throwing rather than by rounding a short period up. That is why the trigger adapter
/// refuses a short period at materialization — a clamp could have been left to Orleans, but a throw at the
/// first registration would surface long after the run was accepted.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class ReminderOptionsProbeTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting it block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task TheDefaultMinimumReminderPeriodIsOneMinute()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) => _ = silo.UseInMemoryReminderService());

        await using InProcessTestCluster plain = builder.Build();

        await plain.DeployAsync();

        string configured = await plain.Client.GetGrain<IReminderProbeGrain>("default-probe").MinimumPeriodAsync();

        Assert.Equal(
            TimeSpan.FromMinutes(1).ToString(),
            configured);
    }

    [Fact]
    public async Task AClusterEnforcesItsFloorByThrowingRatherThanByClamping()
    {
        IReminderProbeGrain probe = cluster.Cluster.Client.GetGrain<IReminderProbeGrain>("floor-probe");

        try
        {
            Assert.Equal(DataflowCluster.MinimumReminderPeriod.ToString(), await probe.MinimumPeriodAsync());

            string refused = await probe.RegisterAsync(
                (long)DataflowCluster.MinimumReminderPeriod.TotalMilliseconds / 2);

            // Throw and not clamp: the registration is refused outright, so no reminder exists afterwards.
            // A clamping runtime would have answered "registered" and left one behind at the floor.
            Assert.StartsWith("threw:System.ArgumentException:", refused, StringComparison.Ordinal);
            Assert.Contains("less than minimum allowed reminder period", refused, StringComparison.Ordinal);
            Assert.Equal(0, await probe.ReminderCountAsync());

            Assert.Equal(
                "registered",
                await probe.RegisterAsync((long)DataflowCluster.MinimumReminderPeriod.TotalMilliseconds));
            Assert.Equal(1, await probe.ReminderCountAsync());
        }
        finally
        {
            await probe.UnregisterAllAsync();
        }

        _ = Token;
    }
}
