#!/usr/bin/env bash
# Consumer checks against the commit's release archive (git archive honours
# .gitattributes export-ignore, as GitHub's tag tarballs do):
#   1. the export-ignore'd directories are not shipped, and @rules_kafka//...
#      loads for a consumer;
#   2. a consumer that only uses kafka_install builds it without fetching
#      Kafka or a JDK (the download is lazy);
#   3. a consumer kafka_test (pre-created topic, produce/consume through the
#      helpers) passes with the network blocked, on rules_kafka's own pin
#      (the root sets no kafka.version());
#   4. an unpinned kafka.version() without sha512 fails, naming the pins.
# Usage: .github/consumer/check.sh <scratch dir> [bazel flags for step 3...]
set -euo pipefail
module="rules_kafka"
ignored=(tests tools)
repo="$(cd "$(dirname "$0")/../.." && pwd)"
work="$1"; shift
src="$work/$module"
c="$work/consumer"
rm -rf "$work"; mkdir -p "$src" "$c"
git -C "$repo" archive HEAD | tar -x -C "$src"
for d in "${ignored[@]}"; do
    if [[ -e "$src/$d" ]]; then echo "FAIL: archive contains $d/" >&2; exit 1; fi
done
# This module's non-dev git_overrides apply only when it is the root, so the
# consumer repeats them (e.g. rules_kubectl).
overrides="$(python3 - "$src/MODULE.bazel" <<'PY'
import re, sys
s = open(sys.argv[1]).read()
out = []
for m in re.finditer(r'bazel_dep\(([^)]*)\)\s*\n\s*(git_override\([^)]*\))', s):
    if "dev_dependency" in m.group(1):
        continue
    out.append("bazel_dep(" + m.group(1) + ")\n" + m.group(2))
print("\n".join(out))
PY
)"
if [[ -f "$repo/.bazelversion" ]]; then cp "$repo/.bazelversion" "$c/"; fi
cat > "$c/MODULE.bazel" <<MOD
module(name = "consumer")
bazel_dep(name = "$module")
local_path_override(module_name = "$module", path = "$src")
$overrides
MOD
cd "$c"

# 1. The archive loads.
touch BUILD.bazel
bazel query "@$module//..." >/dev/null
echo "ok: ${ignored[*]} not shipped; @$module//... loads"

# 2. kafka_install only: nothing of Kafka's or the JDK's is fetched.
cat > BUILD.bazel <<'B'
load("@rules_kafka//:defs.bzl", "kafka_health_check", "kafka_install")

kafka_install(name = "kafka_install_bin")

kafka_health_check(name = "kafka_health_bin")
B
bazel build //:kafka_install_bin //:kafka_health_bin
ext="$(bazel info output_base)/external"
# (rules_java's *_toolchain_config_repo repos are its small toolchain
# declarations, present in any build with rules_java; the JDKs themselves
# are the remotejdk<N>_<platform> repos.)
fetched="$(ls "$ext" | grep -v '^@' | grep -E 'kafka_dist$|remotejdk[0-9]+_[a-z0-9_]+$' \
    | grep -v '_toolchain_config_repo$' || true)"
if [[ -n "$fetched" ]]; then
    echo "FAIL: a kafka_install-only build fetched: $fetched" >&2
    exit 1
fi
echo "ok: a kafka_install-only consumer fetches neither Kafka nor a JDK"

# 3. A consumer kafka_test, offline.
cat > BUILD.bazel <<'B'
load("@rules_kafka//:defs.bzl", "kafka_test")

kafka_test(
    name = "consumer_test",
    srcs = ["consumer_test.sh"],
    topics = ["events:2"],
    tags = ["block-network"],
)
B
cat > consumer_test.sh <<'S'
#!/usr/bin/env bash
set -euo pipefail
source "$KAFKA_TESTLIB"
[[ "$KAFKA_BOOTSTRAP_SERVERS" == "127.0.0.1:$KAFKA_PORT" ]]
[[ "$(kafka_partitions events)" == 2 ]]
printf 'from-a-consumer\n' | kafka_produce events
[[ "$(kafka_consume events 1)" == from-a-consumer ]]
echo "PASS: Kafka $KAFKA_VERSION at $KAFKA_BOOTSTRAP_SERVERS"
S
chmod +x consumer_test.sh
bazel test //:consumer_test --sandbox_default_allow_network=false --test_output=errors "$@"
grep -h -e "ready on" -e "PASS:" bazel-testlogs/consumer_test/test.log | sed 's/; env file.*//'
ls "$ext" | grep -q kafka_dist || { echo "FAIL: kafka_dist not fetched for a kafka_test?" >&2; exit 1; }

# 4. The root's kafka.version() wins; an unpinned one needs its sha512.
cp MODULE.bazel MODULE.bazel.orig
printf 'kafka = use_extension("@rules_kafka//:extensions.bzl", "kafka")\nkafka.version(version = "9.9.9")\n' >> MODULE.bazel
if bazel build //:consumer_test 2> build.log; then echo "FAIL: an unpinned kafka.version() built" >&2; exit 1; fi
grep -qF 'kafka.version(version = "9.9.9") is not pinned. Pinned: ' build.log || { cat build.log; echo "FAIL: no unpinned-version error" >&2; exit 1; }
echo "ok: an unpinned kafka.version() without sha512 fails, naming the pinned versions"
mv MODULE.bazel.orig MODULE.bazel
echo "PASS: consumer of the release archive"
