#!/usr/bin/env bash
# Nothing is written outside $TEST_TMPDIR by the broker, the readiness
# helper or the testlib's CLI tools:
#   - while the broker runs, every file it has open for writing is under
#     $TEST_TMPDIR (/proc/<pid>/fdinfo flags);
#   - no hsperfdata file for the JVM (/tmp/hsperfdata_<user>/<pid>);
#   - afterwards: nothing new under /tmp (outside $TEST_TMPDIR), in the
#     Kafka distribution (no logs/ dir) or in the JDK, and the broker's
#     data, logs and config are under $TEST_TMPDIR/<name>.
#
# Argv: <kafka_server>
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }
SERVER="$TEST_SRCDIR/_main/$1"
env_file="$TEST_TMPDIR/quiet_server.env"
tmpdir_real="$(cd "$TEST_TMPDIR" && pwd -P)"

marker="$TEST_TMPDIR/marker"
touch "$marker"
sleep 1.1   # mtime granularity

"$SERVER" & launcher=$!
trap 'kill -TERM "$launcher" 2>/dev/null || true; wait "$launcher" 2>/dev/null || true' EXIT
for _ in $(seq 1 1800); do
  [[ -f "$env_file" ]] && break
  kill -0 "$launcher" 2>/dev/null || fail "kafka_server exited before it was ready"
  sleep 0.1
done
set -a; source "$env_file"; set +a
source "$KAFKA_TESTLIB"

printf 'quiet\n' | kafka_produce q
[[ "$(kafka_consume q 1 --group quiet)" == quiet ]] || fail "round-trip"

# Open-for-writing files of the broker JVM.
bad=""
for fd in /proc/"$KAFKA_PID"/fd/*; do
  target="$(readlink "$fd" 2>/dev/null)" || continue
  [[ "$target" == /* && "$target" != /dev/* && "$target" != /proc/* ]] || continue
  flags="$(sed -n 's/^flags:[[:space:]]*//p' "/proc/$KAFKA_PID/fdinfo/${fd##*/}" 2>/dev/null)" || continue
  [[ -n "$flags" ]] || continue
  (( (8#$flags & 3) != 0 )) || continue   # O_WRONLY / O_RDWR
  t="${target% (deleted)}"
  [[ "$t" == "$TEST_TMPDIR"/* || "$t" == "$tmpdir_real"/* ]] || bad="$bad $t"
done
[[ -z "$bad" ]] || fail "the broker has files open for writing outside \$TEST_TMPDIR:$bad"
echo "ok: every file the broker writes is under \$TEST_TMPDIR"
[[ ! -e "/tmp/hsperfdata_$(id -un)/$KAFKA_PID" ]] || fail "hsperfdata file for $KAFKA_PID"
echo "ok: no hsperfdata"

kill -TERM "$launcher"; wait "$launcher" || fail "launcher exit $?"
trap - EXIT

new_files() {  # new_files DIR... : files newer than the marker, not under TEST_TMPDIR
  find "$@" -xdev -newer "$marker" ! -type d 2>/dev/null \
    | grep -vF -e "$TEST_TMPDIR/" -e "$tmpdir_real/" || true
}
kafka_real="$(cd "$(dirname "$(readlink -f "$KAFKA_HOME/bin/kafka-run-class.sh")")/.." && pwd)"
jdk_real="$(cd "$(dirname "$(readlink -f "$KAFKA_JAVA_HOME/bin/java")")/.." && pwd)"
[[ ! -e "$KAFKA_HOME/logs" && ! -e "$kafka_real/logs" ]] || fail "a logs/ dir in the Kafka distribution"
got="$(new_files "$KAFKA_HOME" "$kafka_real" "$jdk_real")"
[[ -z "$got" ]] || fail "new files in the distribution / JDK: $got"
echo "ok: nothing new in the Kafka distribution ($kafka_real) or the JDK"
got="$(new_files /tmp)"
[[ -z "$got" ]] || fail "new files in /tmp: $got"
echo "ok: nothing new in /tmp"
for d in data logs config tmp; do
  [[ -d "$TEST_TMPDIR/quiet_server/$d" ]] || fail "no $d dir in the state dir"
done
[[ -s "$TEST_TMPDIR/quiet_server/logs/server.log" ]] || fail "no server.log in the state dir"
ls "$TEST_TMPDIR/quiet_server/data" | grep -q '^q-0$' || fail "no q-0 partition dir in the data dir"
echo PASS
