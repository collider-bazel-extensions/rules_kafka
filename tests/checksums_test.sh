#!/usr/bin/env bash
# Every Kafka pin in private/versions.bzl has a 128-hex sha512, its
# strip_prefix, and both Apache URLs (downloads, then archive). No network:
# tools/update_versions.sh --check compares against upstream.
set -euo pipefail
f="${TEST_SRCDIR}/_main/private/versions.bzl"
python3 - "$f" <<'PY'
import ast, re, sys
src = open(sys.argv[1]).read()
m = re.search(r"^KAFKA_VERSIONS = (\{.*\n\})", src, re.S | re.M)
versions = ast.literal_eval(m.group(1))
default = re.search(r'^DEFAULT_VERSION = "([^"]+)"', src, re.M).group(1)
assert default in versions, f"DEFAULT_VERSION {default} not pinned"
for v, p in versions.items():
    assert re.fullmatch(r"[0-9a-f]{128}", p["sha512"]), f"{v}: sha512 {p['sha512']!r}"
    tgz = f"kafka_2.13-{v}.tgz"
    assert p["urls"] == [f"https://downloads.apache.org/kafka/{v}/{tgz}",
                         f"https://archive.apache.org/dist/kafka/{v}/{tgz}"], f"{v}: urls {p['urls']}"
    assert p["strip_prefix"] == f"kafka_2.13-{v}", f"{v}: strip_prefix"
    assert int(v.split(".")[0]) >= 4, f"{v}: kafka_server needs Kafka 4.x (KRaft only)"
    print(f"ok: {v} sha512 {p['sha512'][:16]}...")
print("PASS")
PY
