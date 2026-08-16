# ADR 0003: Canonical JSON serialization for graph documents

- Status: Accepted for M0
- Date: 2026-08-16

## Context

The M0 exit criteria require that the same logical graph inputs produce
byte-for-byte equivalent graph documents, that golden fixtures pin
compatibility, and that documents remain reviewable. The document format must
never depend on CLR type identity, hash-table iteration order, culture,
runtime version, or construction order.

## Decision

Graph documents serialize to **canonical JSON** with the following rules.

### Encoding

- UTF-8 without BOM, minified (no insignificant whitespace);
- strings use minimal JSON escaping with a fixed escape table: `"` `\` and
  control characters only, control characters as lowercase `\u00xx`; no
  escaping of non-ASCII (raw UTF-8 bytes);
- no Unicode normalization of user-provided strings: the bytes the author
  supplied are the bytes stored; determinism means same input, same output,
  not semantic string folding;
- numbers are integers only, in minimal decimal form with no leading zeros,
  no sign for zero, no fraction, no exponent. The document schema and all
  parameter payloads must model fractional quantities explicitly (for
  example, integer milliseconds or permille) rather than using floating
  point. This rule can be relaxed by a future format version; it cannot be
  tightened retroactively.

### Structure

- every object type has a fixed, documented property order defined by the
  format version (schema order, not alphabetical), and properties are always
  written, in that order, with no omitted defaults;
- collections have canonical order: nodes sort by `NodeId`, edges by
  (`From.Node`, `From.Port`, `To.Node`, `To.Port`), result slots by
  `ResultSlotId`, capabilities by token, all ordinal. Ordinal means ordinal
  comparison of the canonical string form; for `NodeId` that is the full
  `/`-joined path text, not a segment-wise comparison (`a-b` sorts before
  `a/b` because `-` precedes `/` in code-point order, and that is fine —
  canonical order only has to be total, deterministic, and documented);
- parameter and execution-policy payloads are embedded JSON values written by
  the same canonical writer and constrained to the same rules (object keys in
  ordinal order for payloads, since payload schemas are provider-defined);
- the document carries `formatVersion` (starting at 1) as its first property.

### Identity of bytes

- `GraphFingerprint` is the SHA-256 of the canonical bytes;
- golden tests store canonical fixture bytes in the repository and fail on
  any byte difference;
- readers accept exactly the format versions they declare and reject unknown
  versions with a diagnostic, never with a best-effort parse.

## Rationale

- Reviewability: canonical JSON diffs cleanly in pull requests and golden
  fixtures, unlike an opaque binary format.
- Determinism: fixed property order, canonical collection order, and an
  integer-only number model remove every known JSON nondeterminism source
  (key order, float formatting, culture).
- Ecosystem: `System.Text.Json` provides the low-level writer; the canonical
  discipline is a thin layer above it, not a new parser.

## Consequences

- Provider parameter contracts must avoid floats and model units explicitly;
  the contract validator enforces this at registration time.
- Payload canonical key order (ordinal) differs from the schema-order rule of
  the envelope; this is deliberate, because provider payload schemas are not
  known to the core format.
- A format change is a `formatVersion` bump with explicit reader support and
  new golden fixtures, never an in-place mutation of version 1 semantics.

## Rejected alternatives

### Custom canonical binary format

Rejected for M0: golden fixtures and document review become opaque, and the
performance of serializing kilobyte-scale documents does not justify it. A
binary encoding can be added later as an additional representation with its
own fingerprint domain if evidence demands it.

### Alphabetical property order for the envelope

Rejected: schema order groups related fields for human review, and the order
is versioned with the format anyway. Payloads use ordinal key order because
their schemas are provider-defined.

### General JSON numbers

Rejected: floating-point formatting is a classic determinism trap across
runtimes and cultures, and fractional configuration is better modeled in
explicit units.
