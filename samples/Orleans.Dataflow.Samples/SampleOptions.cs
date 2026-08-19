using System.Globalization;

namespace Orleans.Dataflow.Samples;

/// <summary>
/// What the sample application was asked to do.
/// </summary>
/// <remarks>
/// Parsed by hand and shaped after <c>benchmarks/Orleans.Dataflow.Benchmarks/BenchmarkOptions.cs</c>,
/// deliberately: two harnesses in one repository that parse arguments, report, and fail in two different
/// styles cost a reader twice for no reason. The surface is five switches and one of them is
/// <c>--smoke</c>.
/// </remarks>
internal sealed record SampleOptions
{
    /// <summary>Gets how big the run is.</summary>
    internal SampleScale Scale { get; init; } = SampleScale.Full;

    /// <summary>Gets the substring a scenario's name must contain to be run, or null to run all of them.</summary>
    internal string? Only { get; init; }

    /// <summary>Gets whether to name the scenarios instead of running them.</summary>
    internal bool List { get; init; }

    /// <summary>Gets how long the whole application may take before it gives up.</summary>
    internal TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(15);

    /// <summary>Gets what to print instead of running, or null to run.</summary>
    /// <remarks>
    /// Set only when the caller asked for help, which is a request and not a mistake. A command line that is
    /// wrong raises instead, so that the process exits non-zero.
    /// </remarks>
    internal string? Usage { get; init; }

    /// <summary>Gets whether this is the smoke run.</summary>
    internal bool Smoke => Scale.IsSmoke;

    /// <summary>Reads the command line.</summary>
    /// <param name="arguments">The arguments, as the runtime handed them over.</param>
    /// <returns>The options, or options carrying a <see cref="Usage"/> to print instead.</returns>
    /// <exception cref="ArgumentException">The command line is not one this application understands.</exception>
    internal static SampleOptions Parse(string[] arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);

        SampleOptions options = new();

        for (int index = 0; index < arguments.Length; index++)
        {
            string argument = arguments[index];

            switch (argument)
            {
                case "--help" or "-h":
                    return options with { Usage = Help };

                case "--list":
                    options = options with { List = true };

                    break;

                case "--smoke":
                    options = options with { Scale = SampleScale.Smoke, Timeout = TimeSpan.FromMinutes(5) };

                    break;

                case "--only":
                    options = options with { Only = Text(arguments, ref index) };

                    break;

                case "--timeout-seconds":
                    options = options with { Timeout = TimeSpan.FromSeconds(Count(arguments, ref index)) };

                    break;

                default:
                    // Raised rather than printed, so that the process exits non-zero. A CI step with a typo
                    // in it must fail rather than print a usage banner and report success: the whole value
                    // of that step is that it notices when the samples stop running.
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
        Orleans.Dataflow samples — every scenario authored twice, in C# and in F#, and compared.

          (no arguments)             Run every scenario in order.
          --list                     Name each scenario and what it teaches.
          --only TEXT                Run only scenarios whose name contains TEXT.
          --smoke                    Run everything at the smallest sizes that still exercise it.
          --timeout-seconds N        Give up after this long (default 900).
          --help                     Print this.

        Output is tab-separated, one section per kind of reading. Lines beginning with '#' are
        provenance and commentary. The process exits non-zero if any scenario fails, or if any
        scenario's two authorings disagree about a fingerprint or about what their runs produced.
        """;
}
