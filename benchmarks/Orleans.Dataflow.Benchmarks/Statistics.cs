namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// The one summary this harness computes.
/// </summary>
/// <remarks>
/// The median and not the mean, because the distribution a benchmark on a shared machine produces is a
/// clean run with a tail of interrupted ones: a mean follows the tail, a median follows the machine when
/// it was doing what was asked of it. The harness reports the median of every measurement and never a
/// spread, which is a deliberate limit of the grade — a number here says "about this", and a run-to-run
/// spread would invite reading it as "no worse than this".
/// </remarks>
internal static class Statistics
{
    /// <summary>Takes the median of a sample.</summary>
    /// <param name="values">The sample, which is not modified.</param>
    /// <returns>The median.</returns>
    /// <exception cref="ArgumentException"><paramref name="values"/> is empty.</exception>
    internal static double Median(IReadOnlyList<double> values)
    {
        ArgumentNullException.ThrowIfNull(values);

        if (values.Count == 0)
        {
            throw new ArgumentException("A median needs at least one measurement.", nameof(values));
        }

        double[] ordered = [.. values];

        Array.Sort(ordered);

        int middle = ordered.Length / 2;

        return ordered.Length % 2 == 1
            ? ordered[middle]
            : (ordered[middle - 1] + ordered[middle]) / 2.0;
    }

    /// <summary>Takes the median of a sample of counts.</summary>
    /// <param name="values">The sample, which is not modified.</param>
    /// <returns>The median, rounded to the nearest whole count.</returns>
    internal static long Median(IReadOnlyList<long> values)
    {
        ArgumentNullException.ThrowIfNull(values);

        return (long)Math.Round(Median([.. values.Select(static value => (double)value)]));
    }
}
