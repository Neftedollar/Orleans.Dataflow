# Benchmarks

What Orleans.Dataflow costs to run, and what it holds while running. This page
is the evidence behind [GOAL.md](project/GOAL.md)'s seventh definition-of-done point —
*runtime resources are bounded by default and measured under representative
load* — together with the recovery half of the same claim: how long a durable
run takes to start delivering again after the silo hosting it stops existing.

Three things produce what is written here, and they are not interchangeable:

- **`benchmarks/Orleans.Dataflow.Benchmarks`** measures and prints. It asserts
  nothing. Its output is the source of every number in [Results](#results).
- **`tests/Orleans.Dataflow.Tests/Runtime/BoundedMemoryTests.cs`** asserts. It
  is where boundedness is a contract rather than an observation: it fails the
  build when a graph starts holding what its author did not declare.
- **`tests/Orleans.Dataflow.OrleansTests/Cluster/`** holds the cluster
  behaviour these numbers describe — failover, durable resume, rolling upgrade
  — as executable claims. The harness measures a *latency* over that behaviour
  and deliberately does not re-prove it.

## The honesty grade

**Bounds to within a factor. Throughput to within an order of magnitude.**

That is a real grade and not a disclaimer, and the harness prints it at the top
of every report so a number cannot be quoted away from it.

The questions this harness is built to answer are *does the runtime hold a
bounded amount of memory under a stream far longer than any bound it declares*
and *roughly what does it cost to push an element through*. It answers the
first well: the peak live heap is read after a forced, compacting collection,
so what it reports is retention rather than garbage, and a graph that retained
even a tenth of its stream would be visible immediately.

It answers the second roughly. Each scenario is timed over a handful of whole
runs and reported as a median, with no statistical treatment of the spread, on
whatever machine happened to run it. It is **not** built to compare two
implementations of the same stage, to detect a ten-percent regression, or to
support a "faster than X" claim. If that is ever needed, it wants a different
instrument, and this one should not be stretched to pretend.

This is why the harness is bespoke rather than BenchmarkDotNet. The claims that
matter here — boundedness and recovery — need controlled arrangements: a stream
far longer than any declared bound, a heap read at known positions in it, a
silo killed underneath a live durable run. A microbenchmark runner is built for
the opposite job, many short iterations of a small body resolved statistically,
and would make every one of those arrangements harder while answering none of
them.

## What is measured

### Memory: the peak live heap of seven shapes

The reported number is **how far the live heap rises during a run**: the graph
is sampled at eight even positions plus once before the first element, every
sample is read straight after a blocking compacting collection of every
generation, and the peak is the largest reading minus the smallest. Whatever
else the process holds steadily is in every sample and subtracts out.

Six shapes that between them cover every way this runtime is allowed to hold
more than one element at a time, plus one control that is meant to grow:

| Scenario | The bound it is measured against |
| --- | --- |
| `fused-chain` | one element in flight; the chain declares no boundary |
| `buffered-boundary` | 1024 elements; the capacity the author declared |
| `async-map-parallelism-4` | 4 calls in flight; the concurrency the author declared |
| `broadcast-two-sinks` | one element per leg; a junction holds an element until every leg has taken it |
| `bounded-group-by` | 16 live keys; the maximum the author declared |
| `grain-call-sink-shape` | 8 calls in flight; a grain-call sink's shape, with the call faked locally |
| `declared-collect-control` | the declared maximum, which is the run length: **this one is meant to grow** |

Every shape but the control ends in something that discards — a fold to a
number, a count, a callback that returns — so what the peak measures is what
the runtime holds and never what the author asked to keep.

The control is the reason to believe the other six. Every claim here is of the
form *the peak did not grow*, and an instrument that could not see growth would
make all of them pass while measuring nothing. `declared-collect-control` is
built to grow, and the same instrument on the same machine has to report it. It
is also the other half of the claim proper: **memory follows what an author
declared.** Declare a bound of a thousand and a thousand is held; declare a
bound of a million and the runtime will hold a million. Nothing here promises
that a graph cannot be written to use memory.

### Throughput: elements per second, and allocation per element

Timed over whole runs — materialization included, because compiling a document
into an execution plan is part of what a caller pays for — with one warmup run
discarded and the median of the rest reported.

Timing and weighing happen in **separate passes**, and this is not a detail: the
memory pass stops the world at every sample, and a run timed with that in it
would report the collector's cost as the graph's.

### Recovery: from a silo dying to the first element delivered again

One durable run on a three-silo in-process cluster. The run's source emits its
whole sequence and then parks on the run's stop token, so the run is alive and
its position is committed at a moment the harness chose rather than one it
caught. The silo hosting the run grain is then killed — torn down, not drained
— and the clock runs from the moment the harness *asks* for that destruction to
the moment the recording sink is handed the resumed attempt's first element.

The clock starts at the request rather than at the kill call's return because
the other way round produced a **negative** latency: a dying in-process silo
writes its own death into the membership table early in a teardown that then
takes milliseconds to finish, so the client's poll had already re-addressed the
run, a surviving silo had already resumed it, and the first replayed element
had already arrived before `KillSiloAsync` came back. There is no single instant
at which an in-process host stops existing, so the reported number starts from
the only reproducible reference the harness has — and consequently contains
this cluster's teardown, which makes it an upper bound on the part being
measured.

The sink stamps that delivery itself, so the harness's own polling interval
never enters the number. The resume is triggered by the client's ordinary
completion poll and by nothing else: the handle's `Completion` is read straight
after materialization, which is what a client intending to wait for a run does,
so no harness action stands between the death and the recovery.

**The latency is bimodal, and one number hides that.** The client's poll is
what notices the run is gone. A poll that was already airborne when its target's
silo died is answered by nobody and waits out the whole response timeout — five
seconds, as this harness configures its client — before the loop retries. So a
recovery takes either tens of milliseconds or about five seconds, with nothing
much in between. Four runs of the same arrangement measured 34, 40, 34, and
5889 milliseconds. That is not a defect; it is the cost of one unlucky poll,
and it is why the harness kills five times by default and reports the median.
When you quote the number, quote the mode it came from.

The same measurement reports the **replay window**: how many elements the
resumed attempt delivered a second time. That is the at-least-once cost of the
checkpoint cadence, in elements, and it is the number an operator trades
against checkpoint frequency.

## What is *not* measured

Stated plainly, because a benchmark page is read by people looking for a number
to rely on:

- **Nothing crosses a network.** The recovery cluster is `InProcessTestCluster`:
  three silos in one process, talking through the loopback of the Orleans
  runtime. Real inter-silo latency is absent from every number here.
- **One machine.** No number here says anything about how the runtime scales
  across hosts.
- **No real persistence provider.** Checkpoints go to the shipped in-memory
  store and coordinator state to an in-process store. A deployment's recovery
  time includes its store's read latency, which is not in these numbers.
- **The recovery latency excludes failure detection.** An in-process silo
  writes its own death into the membership table on the way out, so the cluster
  knows immediately. A silo that *cannot* announce its death — a power cut, a
  partition — is discovered by probes, and that discovery is bounded by the
  membership options, not by anything measured here. **A real deployment's
  recovery is that discovery plus something like this number, never less than
  it.** The number also carries the client's 20 ms poll interval, which is the
  resume trigger, and this cluster's own teardown; within the in-process
  arrangement it is therefore an over-estimate, and outside it a floor.
- **No cluster memory.** Every memory number comes from the local runtime. A
  run hosted on a silo holds what these shapes hold *plus* whatever Orleans
  holds for it, and separating the two needs a cluster harness rather than an
  assertion. Cluster memory is deliberately out of scope for 1.0's evidence.
- **The grain-call sink is a shape, not a call.** The scenario keeps a declared
  number of calls in flight and awaits each; the call itself is local. What an
  Orleans call costs is a measurement of Orleans.
- **Rolling upgrade is not re-measured.** It is proven, not timed:
  `tests/Orleans.Dataflow.OrleansTests/Cluster/RollingUpgradeTests.cs` (M5.4)
  is the evidence, and the harness prints a pointer to it rather than a number.

## Three things the instrument had to be taught

Every one of them was found by measurement rather than by reasoning, and all
three are worth knowing to anyone writing a heap benchmark on .NET.

**`GC.GetTotalMemory(true)` is not precise enough to use.** It collects
repeatedly until two readings agree to within about five percent and then
returns. Five percent of a live set of a few megabytes is hundreds of
kilobytes — larger than everything these graphs retain put together. With that
call, `declared-collect-control`, which retains a megabyte by construction,
reported a peak of **zero**. The instrument therefore performs the collection
itself — blocking, compacting, twice, with a finalizer wait between — and then
reads with `forceFullCollection: false`, which has no tolerance in it.

**A finished run is still holding its last accumulator.** After the run has
completed and its handle has been disposed, the async machinery that carried it
keeps the terminal's state reachable until the thread-pool threads pick up
other work. A reading taken at that moment belongs to the *previous* run: the
deltas that followed were measured at minus ten megabytes, drifting upward
through the next run as the old state was finally let go. The fix is to hand
the pool a burst of trivial work and collect again, three rounds, before a
weighed run starts.

**A peak is a range within one run, not a delta against a baseline before it.**
Even with the churn, a residue that is reachable *throughout* a run but was not
reachable when the baseline was taken is indistinguishable from retention — it
was caught adding six megabytes to a shape that holds twelve kilobytes. So the
instrument takes the fullest sample minus the quietest, with the first element
always a sample point so that the quietest is normally the graph before
anything has flowed through it. Anything the process holds steadily is in both
terms and subtracts out. The effect on the numbers is large: the six bounded
shapes' movement between a twenty-thousand-element run and a two-hundred-
thousand-element one fell from 72 KB to 4.8 KB, while the control's stayed at
10.5 MB.

## Running them

The harness is a plain console application with no dependencies beyond the
library and its Orleans host.

```bash
dotnet build Orleans.Dataflow.slnx --configuration Release

# The full run: seven shapes at a million elements, three runs each,
# then five silo kills. A few minutes.
dotnet run --project benchmarks/Orleans.Dataflow.Benchmarks \
  --no-build --configuration Release

# What CI runs: tiny sizes, one run of everything, well under a minute.
dotnet run --project benchmarks/Orleans.Dataflow.Benchmarks \
  --no-build --configuration Release -- --smoke

# Everything it takes.
dotnet run --project benchmarks/Orleans.Dataflow.Benchmarks \
  --no-build --configuration Release -- --help
```

Useful switches: `--elements N` for the stream length, `--runs N` for how many
runs each median is over, `--only TEXT` to run one scenario, and
`--recovery-elements` / `--recovery-every` / `--recovery-repetitions` for the
cluster half. Note that `declared-collect-control` has a ceiling of a million
elements whatever `--elements` says — it is the one shape that keeps what it is
given, and at sixty-odd bytes an element a ten-million run would be a
six-hundred-megabyte heap proving something a one-million run already proves.
Each row prints the count it actually ran.

Two things exit non-zero besides a scenario failing: a command line the harness
does not understand, and an `--only` that matches nothing. Both would otherwise
measure nothing and report success, which is indistinguishable from a clean run
to whatever reads the exit code.

Output is tab-separated, one section per measurement kind, with every line that
is not data beginning with `#`. The provenance block at the top — machine,
architecture, runtime version, collector mode, build configuration — is part of
the result: a throughput number without it is a rumour.

**The harness exits non-zero if any scenario fails to complete.** That is what
makes the CI smoke step worth having: it asserts nothing about timing and
everything about the harness still working, so a benchmark nobody runs on a
schedule cannot quietly rot.

The assertions run with the rest of the suite:

```bash
dotnet test tests/Orleans.Dataflow.Tests/Orleans.Dataflow.Tests.csproj \
  --no-build --configuration Release
```

## Results

> Filled from one full harness run on the machine the provenance block names
> (2026-08-19, an otherwise quiet development machine). To refresh: build the
> solution Release, run the harness with no arguments, and replace the
> provenance block and the three tables with its output. A result is a result
> *on a stated machine*; quote nothing from this page without the block.

### Provenance

```text
# grade: honesty-grade: bounds to within a factor, throughput to within an order of magnitude
# utc: 2026-08-19T01:41:38Z
# os: macOS 26.5.2 (Arm64)
# cpu: 10 logical processors
# runtime: .NET 10.0.11
# gc: workstation, non-concurrent, latency Batch
# build: Release
# mode: full
# elements: 1000000, runs: 3
# recovery: 20000 elements, every 3000, 5 repetitions
```

### Throughput

| Scenario | Elements | Runs | Median ms | Elements/second | Allocated bytes/element |
| --- | --- | --- | --- | --- | --- |
| `fused-chain` | 1,000,000 | 3 | 79.4 | 12,598,679 | 96.0 |
| `buffered-boundary` | 1,000,000 | 3 | 154.3 | 6,481,495 | 99.1 |
| `async-map-parallelism-4` | 1,000,000 | 3 | 2,426.6 | 412,097 | 589.9 |
| `broadcast-two-sinks` | 1,000,000 | 3 | 11,411.0 | 87,635 | 827.0 |
| `bounded-group-by` | 1,000,000 | 3 | 100.0 | 10,004,602 | 96.0 |
| `grain-call-sink-shape` | 1,000,000 | 3 | 2,228.9 | 448,653 | 390.6 |
| `declared-collect-control` | 1,000,000 | 3 | 46.7 | 21,392,571 | 48.8 |

### Memory

| Scenario | Elements | Runs | Peak live heap (bytes) | Per element | Bound |
| --- | --- | --- | --- | --- | --- |
| `fused-chain` | 1,000,000 | 3 | 144 | 0.000 | one element in flight |
| `buffered-boundary` | 1,000,000 | 3 | 17,184 | 0.017 | 1024 elements |
| `async-map-parallelism-4` | 1,000,000 | 3 | 4,016 | 0.004 | 4 calls in flight |
| `broadcast-two-sinks` | 1,000,000 | 3 | 4,816 | 0.005 | one element per leg |
| `bounded-group-by` | 1,000,000 | 3 | 7,200 | 0.007 | 16 live keys |
| `grain-call-sink-shape` | 1,000,000 | 3 | 4,392 | 0.004 | 8 calls in flight |
| `declared-collect-control` | 1,000,000 | 3 | 56,388,584 | 56.389 | the declared maximum |

### Recovery

| Scenario | Elements | Every | Kills | Median latency (ms) | Median replayed elements |
| --- | --- | --- | --- | --- | --- |
| `durable-run-silo-kill` | 20,000 | 3,000 | 5 | 16.1 | 2,000 |

### Reading these tables

Read them against three things and not against
intuition:

- **A peak beside its bound, not on its own.** A megabyte means nothing;
  a megabyte where the author declared a bound of one element means a defect.
- **The control against the rest.** If `declared-collect-control` is not
  dramatically larger than every other row, the instrument was not working and
  no other row on the page is evidence of anything.
- **Throughput as a magnitude.** The gap between the fastest and slowest shape
  here is two orders of magnitude, and *that* is the finding: what costs is
  crossing a boundary — an async stage, a junction, a terminal that awaits —
  and not the length of a fused chain. Which of two runtimes is faster is a
  question this page does not answer.
