using System.Globalization;

namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// What the harness was asked to do.
/// </summary>
/// <remarks>
/// Parsed by hand rather than by a library, and that is the whole point of the choice: a benchmark harness
/// whose command line pulls in a dependency has a dependency in every measurement it takes. The surface is
/// eight switches and one of them is <c>--smoke</c>.
/// </remarks>
internal sealed record BenchmarkOptions
{
    /// <summary>The element count a full run measures the graph scenarios at.</summary>
    /// <remarks>
    /// A million: large enough that a per-element cost is what the numbers are made of rather than
    /// materialization, and that a graph holding on to elements would be visible from orbit; small enough
    /// that the seven scenarios and their three passes finish in a coffee break. Raise it with
    /// <c>--elements</c> when the question is where throughput settles.
    /// </remarks>
    internal const long DefaultElements = 1_000_000;

    /// <summary>The element count <c>--smoke</c> measures at.</summary>
    /// <remarks>
    /// Two thousand, which measures nothing worth quoting and proves the one thing the smoke run exists to
    /// prove: every scenario still runs to completion.
    /// </remarks>
    internal const long SmokeElements = 2_000;

    /// <summary>Gets how many elements each graph scenario carries.</summary>
    internal long Elements { get; init; } = DefaultElements;

    /// <summary>Gets how many runs each pass of each graph scenario performs after its warmup.</summary>
    internal int Runs { get; init; } = 3;

    /// <summary>Gets how many elements the recovery run delivers before its silo is killed.</summary>
    internal long RecoveryElements { get; init; } = 20_000;

    /// <summary>Gets how many elements the recovery run admits between checkpoints.</summary>
    /// <remarks>
    /// Deliberately not a divisor of <see cref="RecoveryElements"/>: the run has to have delivered past its
    /// last checkpoint for the resumed attempt to owe anybody an element. See
    /// <see cref="RecoveryBenchmark"/>, which explains what happens when it does not.
    /// </remarks>
    internal int RecoveryEveryElements { get; init; } = 3_000;

    /// <summary>Gets how many kills the recovery scenario measures.</summary>
    /// <remarks>
    /// Five rather than three, and the reason is the shape of the distribution rather than a wish for
    /// precision. The recovery latency is bimodal: a client poll that was airborne when its target's silo
    /// died is answered by nobody and waits out the whole response timeout — five seconds here — before the
    /// loop retries, so a run either recovers in tens of milliseconds or in about five seconds. Measured
    /// over four smoke runs: 34, 40, 34, and 5889 milliseconds. A median of three lands on the slow mode
    /// whenever two of the three are unlucky, which is often enough to matter; five makes that need three.
    /// </remarks>
    internal int RecoveryRepetitions { get; init; } = 5;

    /// <summary>Gets the substring a scenario's name must contain to be run, or null to run all of them.</summary>
    internal string? Only { get; init; }

    /// <summary>Gets how long the whole harness may take before it gives up.</summary>
    internal TimeSpan Timeout { get; init; } = TimeSpan.FromHours(1);

    /// <summary>Gets whether this is the smoke run.</summary>
    internal bool Smoke { get; init; }

    /// <summary>Gets what to print instead of running, or null to run.</summary>
    /// <remarks>
    /// Set only when the caller asked for help, which is a request and not a mistake. A command line that
    /// is wrong raises instead, so that the process exits non-zero.
    /// </remarks>
    internal string? Usage { get; init; }

    /// <summary>Reads the command line.</summary>
    /// <param name="arguments">The arguments, as the runtime handed them over.</param>
    /// <returns>The options, or options carrying a <see cref="Usage"/> to print instead.</returns>
    /// <exception cref="ArgumentException">The command line is not one this harness understands.</exception>
    internal static BenchmarkOptions Parse(string[] arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);

        BenchmarkOptions options = new();

        for (int index = 0; index < arguments.Length; index++)
        {
            string argument = arguments[index];

            switch (argument)
            {
                case "--help" or "-h":
                    return options with { Usage = Help };

                case "--smoke":
                    options = options with
                    {
                        Smoke = true,
                        Elements = SmokeElements,
                        Runs = 1,
                        RecoveryElements = 500,
                        RecoveryEveryElements = 200,
                        RecoveryRepetitions = 1,
                        Timeout = TimeSpan.FromMinutes(5),
                    };

                    break;

                case "--elements":
                    options = options with { Elements = Count(arguments, ref index) };

                    break;

                case "--runs":
                    options = options with { Runs = (int)Count(arguments, ref index) };

                    break;

                case "--recovery-elements":
                    options = options with { RecoveryElements = Count(arguments, ref index) };

                    break;

                case "--recovery-every":
                    options = options with { RecoveryEveryElements = (int)Count(arguments, ref index) };

                    break;

                case "--recovery-repetitions":
                    options = options with { RecoveryRepetitions = (int)Count(arguments, ref index) };

                    break;

                case "--timeout-seconds":
                    options = options with { Timeout = TimeSpan.FromSeconds(Count(arguments, ref index)) };

                    break;

                case "--only":
                    options = options with { Only = Text(arguments, ref index) };

                    break;

                default:
                    // Raised rather than printed, so that the process exits non-zero. A harness whose CI
                    // step has a typo in it must fail rather than print a usage banner and report success:
                    // the whole value of that step is that it notices when the harness stops running.
                    throw new ArgumentException(
                        $"Unrecognized argument '{argument}'.{Environment.NewLine}{Help}",
                        nameof(arguments));
            }
        }

        return options;
    }

    /// <summary>Reads the count following a switch.</summary>
    /// <param name="arguments">The arguments.</param>
    /// <param name="index">The switch's position, advanced past its value.</param>
    /// <returns>The count.</returns>
    /// <exception cref="ArgumentException">The value is missing or is not a positive count.</exception>
    private static long Count(string[] arguments, ref int index)
    {
        string text = Text(arguments, ref index);

        return long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out long value) && value > 0
            ? value
            : throw new ArgumentException(
                $"'{arguments[index - 1]}' takes a positive whole number and was given '{text}'.",
                nameof(arguments));
    }

    /// <summary>Reads the text following a switch.</summary>
    /// <param name="arguments">The arguments.</param>
    /// <param name="index">The switch's position, advanced past its value.</param>
    /// <returns>The text.</returns>
    /// <exception cref="ArgumentException">The value is missing.</exception>
    private static string Text(string[] arguments, ref int index)
    {
        if (index + 1 >= arguments.Length)
        {
            throw new ArgumentException($"'{arguments[index]}' takes a value and was given none.", nameof(arguments));
        }

        index++;

        return arguments[index];
    }

    /// <summary>What to print when the command line is not understood, or is <c>--help</c>.</summary>
    private const string Help = """
        Orleans.Dataflow benchmarks — bounded memory, throughput, and recovery evidence.

          --smoke                    Tiny sizes and one run of everything. Asserts nothing about
                                     timing; proves every scenario still runs to completion.
          --elements N               Elements per graph scenario (default 1000000).
          --runs N                   Runs per pass after the warmup (default 3).
          --recovery-elements N      Elements a recovery run delivers before the kill (default 20000).
          --recovery-every N         Elements between checkpoints in a recovery run (default 3000).
          --recovery-repetitions N   Kills to measure (default 5).
          --timeout-seconds N        Give up after this long (default 3600).
          --only TEXT                Run only scenarios whose name contains TEXT.
          --help                     Print this.

        Output is tab-separated, one section per measurement kind. Lines beginning with '#' are
        provenance. The process exits non-zero if any scenario fails to complete.
        """;
}
