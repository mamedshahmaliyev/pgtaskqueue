"""The version on the web UI's static URLs, and the command that bumps it.

Pages load `/ui/static/tasks.js?v=<version>`, and browsers may keep a file fetched with the
current version for a year without asking again. The templates add the `?v=` from
`static_version.json`. Imports between modules carry it too (`from "./core.js?v=<version>"`),
written into the files by `bump`: there is no import map to add it, because the
Content-Security-Policy refuses inline scripts and an import map is one.

After changing anything in `static/`:

    uv run python -m pgtaskqueue.api.assets bump

`test_static_version_matches_the_files` fails until that has been run, because a file
changed under an old version would reach no browser that cached the old one.

This module imports nothing from FastAPI, so the command runs without the api extra.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from functools import cache
from pathlib import Path

HERE = Path(__file__).parent
STATIC_DIR = HERE / "static"
VERSION_FILE = HERE / "static_version.json"

# A relative module specifier in `import … from "./x.js"`, `import "./x.js"` or
# `import("./x.js")`, with the `?v=` a bump wrote, if any.
_IMPORT = re.compile(r"""(\b(?:from|import)\s*\(?\s*)(["'])(\.{1,2}/[^"'?]+?\.js)(\?v=\d+)?\2""")
_STAMP = re.compile(r"\?v=\d+")


def files() -> list[Path]:
    """What `static/` serves. Dot files (.DS_Store) are left out: no page loads them."""
    return sorted(
        path
        for path in STATIC_DIR.rglob("*")
        if path.is_file() and not any(part.startswith(".") for part in path.parts)
    )


def digest() -> str:
    """One hash over every file's path and content, so a rename counts as a change too.

    The `?v=` stamps are left out, so the bump that writes them does not change the digest.
    """
    sha = hashlib.sha256()
    for path in files():
        content = path.read_bytes()
        if path.suffix == ".js":
            content = _STAMP.sub("", content.decode()).encode()
        sha.update(path.relative_to(STATIC_DIR).as_posix().encode() + b"\0" + content + b"\0")
    return sha.hexdigest()


def unstamped(current: str) -> list[str]:
    """Imports that do not carry `?v=<current>`.

    Every module must import another by the same URL: the browser keeps one instance per
    URL, so `core.js?v=1` and `core.js?v=2` would be two copies of its state.
    """
    return [
        f"{path.name}: {match[0]}"
        for path in files()
        if path.suffix == ".js"
        for match in _IMPORT.finditer(path.read_text())
        if match[4] != f"?v={current}"
    ]


def recorded() -> dict[str, str]:
    return json.loads(VERSION_FILE.read_text())


@cache
def version() -> str:
    """Read once per process: an image's files do not change while it runs."""
    return recorded()["version"]


def bump() -> tuple[str, str]:
    old = recorded()["version"] if VERSION_FILE.exists() else "none"
    # Milliseconds since the epoch: always increases, and needs no one to pick a number.
    new = str(int(time.time() * 1000))
    for path in files():
        if path.suffix != ".js":
            continue
        text = path.read_text()
        stamped = _IMPORT.sub(lambda m: f"{m[1]}{m[2]}{m[3]}?v={new}{m[2]}", text)
        if stamped != text:
            path.write_text(stamped)
    VERSION_FILE.write_text(json.dumps({"version": new, "digest": digest()}, indent=2) + "\n")
    return old, new


def problems() -> list[str]:
    """Why the files and the recorded version disagree; empty when they agree."""
    current = recorded()
    found = []
    if current["digest"] != digest():
        found.append("The UI's static files changed since the version was set.")
    if missing := unstamped(current["version"]):
        found.append(f"Imports without ?v={current['version']}: " + "; ".join(missing))
    return found


def main(argv: list[str]) -> int:
    command = argv[0] if argv else ""
    if command == "bump":
        old, new = bump()
        print(f"static version {old} -> {new}")
        return 0
    if command == "check":
        if found := problems():
            print(
                *found,
                "Run: uv run python -m pgtaskqueue.api.assets bump",
                sep="\n",
                file=sys.stderr,
            )
            return 1
        print(f"static version {recorded()['version']} matches the files")
        return 0
    print("usage: python -m pgtaskqueue.api.assets bump|check", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
