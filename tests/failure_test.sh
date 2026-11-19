#!/usr/bin/env bash
# Start-up failures are errors that name the cause and show the broker's
# log tail, and leave nothing running:
#   - a broker that exits during start-up (an authorizer class that doesn't
#     exist): exit 1, the tail of server.log;
#   - a broker JVM that can't start (a bad -XX option): exit 1, the JVM's
#     console output;
#   - a kafka_test whose broker fails: the test body never runs.
#
# Argv: <bad_authorizer_server> <bad_jvm_server> <trampoline> <failing server> <failing inner>
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }
R="$TEST_SRCDIR/_main"

# No process may still run from this test's TEST_TMPDIR (read-only scan of
# /proc command lines; nothing is killed by pattern).
nothing_left() {
  local d
  for d in /proc/[0-9]*; do
    [[ "${d#/proc/}" == "$$" ]] && continue
    if tr '\0' ' ' < "$d/cmdline" 2>/dev/null | grep -qF "$TEST_TMPDIR/$1"; then
      fail "$1: process ${d#/proc/} still running: $(tr '\0' ' ' < "$d/cmdline" | cut -c1-200)"
    fi
  done
}

expect_fail() {  # expect_fail NAME MESSAGE BINARY
  local name="$1" msg="$2" bin="$3" rc
  "$bin" 2> "$TEST_TMPDIR/$name.err" && rc=0 || rc=$?
  [[ "$rc" == 1 ]] || { cat "$TEST_TMPDIR/$name.err"; fail "$name: exit $rc, want 1"; }
  grep -qF -- "$msg" "$TEST_TMPDIR/$name.err" || { cat "$TEST_TMPDIR/$name.err"; fail "$name: no '$msg' in the output"; }
  [[ ! -e "$TEST_TMPDIR/$name.env" ]] || fail "$name: env file written"
  nothing_left "$name"
  echo "ok: $name failed with: $(grep -m1 -F -- "$msg" "$TEST_TMPDIR/$name.err" | cut -c1-160)"
}

expect_fail bad_authorizer_server "last lines of $TEST_TMPDIR/bad_authorizer_server/logs/server.log" "$R/$1"
grep -q "no.such.Authorizer" "$TEST_TMPDIR/bad_authorizer_server.err" || fail "server.log tail lacks the class name"
expect_fail bad_jvm_server "Unrecognized VM option" "$R/$2"

# The kafka_test trampoline: broker fails -> exit 1, body (never_runs.sh) skipped.
out="$TEST_TMPDIR/trampoline.err"
"$R/$3" "$4" "$5" failing_kafka_test_kafka 2> "$out" && rc=0 || rc=$?
[[ "$rc" == 1 ]] || { cat "$out"; fail "failing kafka_test: exit $rc"; }
grep -q "the Kafka broker exited (status 1) before it was ready" "$out" || { cat "$out"; fail "trampoline message"; }
grep -q "the test body ran" "$out" && fail "the test body ran"
nothing_left failing_kafka_test_kafka
echo "ok: a kafka_test whose broker fails doesn't run its body"
echo PASS
