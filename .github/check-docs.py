#!/usr/bin/env python3
"""Guards the documentation against the two ways it decays.

The first is vocabulary. This documentation is written about the library rather
than about the work that produced it, so a milestone name, a decision-record
number, or a phrase like "as implemented" appearing in it is a defect: the
reader is being handed a piece of the project's history instead of an answer.
The engineering records under docs/internal, the project tracking under
docs/project, and the decision records themselves are exempt, because being
about the history is what those are for.

The second is links. Documentation moves, and a link that silently stops
resolving is worse than no link, so every relative link and every anchor inside
one is resolved against the files and headings that actually exist.

Run it from the repository root. It prints every problem it finds and exits
non-zero when there is one.
"""

import pathlib
import re
import sys
import unicodedata

ROOT = pathlib.Path(__file__).resolve().parent.parent

# The pages the rule applies to: everything a reader of the library is meant to
# read, and nothing that is deliberately a record of the project.
GUARDED = [
    "README.md",
    "samples/README.md",
    "docs/index.md",
    "docs/start",
    "docs/concepts",
    "docs/guides",
    "docs/operations",
    "docs/reference",
]

# Every link is resolved, including ones from the exempt areas, because a
# broken link is a defect wherever it lives.
LINKED = GUARDED + ["docs/internal", "docs/project", "docs/BENCHMARKS.md"]

FORBIDDEN = [
    (re.compile(r"\bM[0-9](\.[0-9])?\b"), "a milestone name"),
    (re.compile(r"\bADR[ -][0-9]{3,4}\b", re.IGNORECASE), "a decision-record number"),
    (re.compile(r"—\s*historical\b"), "a milestone marker"),
    (re.compile(r"\bdesign ahead of code\b"), "a milestone marker"),
    (re.compile(r"\bas implemented\b"), "a milestone marker"),
    (re.compile(r"\brecorded deferral\b"), "planning vocabulary"),
    (re.compile(r"docs/architecture/"), "a link into the decision records"),
]

LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
FENCE = re.compile(r"^\s*```")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")


def pages(roots):
    """Every markdown file under the given files and directories."""
    for name in roots:
        path = ROOT / name
        if path.is_dir():
            yield from sorted(path.rglob("*.md"))
        elif path.exists():
            yield path


def outside_code(text):
    """The lines of a file that are not inside a fenced code block.

    A code block may legitimately contain anything — an error message quoting a
    version, a JSON document — so the vocabulary rule applies to prose only.
    """
    fenced = False
    for number, line in enumerate(text.splitlines(), start=1):
        if FENCE.match(line):
            fenced = not fenced
            continue
        if not fenced:
            yield number, line


def anchor(heading):
    """The fragment a markdown heading is reachable by, GitHub's spelling of it."""
    text = unicodedata.normalize("NFKD", heading.lower())
    text = re.sub(r"[`*_\[\]()]", "", text)
    text = re.sub(r"[^\w\s-]", "", text)
    # Every space becomes its own hyphen rather than a run collapsing into one,
    # which is what makes "ordered / unordered" reachable as "ordered--unordered":
    # the slash is removed and the two spaces that surrounded it both remain.
    return re.sub(r"\s", "-", text.strip())


def anchors_of(path):
    """Every fragment the given page offers."""
    found = set()
    fenced = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if FENCE.match(line):
            fenced = not fenced
            continue
        if fenced:
            continue
        match = HEADING.match(line)
        if match:
            found.add(anchor(match.group(2)))
    return found


def main():
    problems = []

    for path in pages(GUARDED):
        relative = path.relative_to(ROOT)
        for number, line in outside_code(path.read_text(encoding="utf-8")):
            for pattern, what in FORBIDDEN:
                match = pattern.search(line)
                if match:
                    problems.append(
                        f"{relative}:{number}: {what} — {match.group(0)!r}\n"
                        f"    Documentation describes the library, not the work that produced it.\n"
                        f"    State the fact in the page's own words instead of citing where it was decided."
                    )

    known_anchors = {}
    for path in pages(LINKED):
        relative = path.relative_to(ROOT)
        for number, line in outside_code(path.read_text(encoding="utf-8")):
            for target in LINK.findall(line):
                target = target.split(" ")[0].strip()
                if target.startswith(("http://", "https://", "mailto:")):
                    continue

                file_part, _, fragment = target.partition("#")

                if not file_part:
                    destination = path
                else:
                    destination = (path.parent / file_part).resolve()

                if not destination.exists():
                    problems.append(f"{relative}:{number}: link does not resolve — {target!r}")
                    continue

                if fragment and destination.suffix == ".md":
                    if destination not in known_anchors:
                        known_anchors[destination] = anchors_of(destination)
                    if fragment.lower() not in known_anchors[destination]:
                        problems.append(
                            f"{relative}:{number}: anchor does not exist — {target!r}"
                        )

    if problems:
        print(f"Documentation check failed with {len(problems)} problem(s):\n")
        for problem in problems:
            print(f"  {problem}")
        return 1

    checked = len(list(pages(LINKED)))
    print(f"Documentation check passed: {checked} pages, vocabulary and links clean.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
