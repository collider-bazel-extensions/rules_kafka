#!/usr/bin/env bash
# kafka_test trampoline — starts the broker, then runs the user's test.
#
# Argv: <server> <inner> <server_name> [user args...]
set -euo pipefail

SERVER_SHORT="$1"; INNER_SHORT="$2"; SERVER_NAME="$3"
shift 3

: "${TEST_TMPDIR:?kafka_test: TEST_TMPDIR not set (expected under bazel test)}"
if [[ -z "${RUNFILES_DIR:-}" ]]; then
  RUNFILES_DIR="${TEST_SRCDIR:-$0.runfiles}"
  export RUNFILES_DIR
fi

resolve() {
  local p="$1"
  for c in "${RUNFILES_DIR}/_main/${p}" "${RUNFILES_DIR}/${p}" "${RUNFILES_DIR}/${p#../}"; do
    [[ -e "$c" ]] && { printf '%s\n' "$c"; return; }
  done
  echo "kafka_test: not in runfiles: ${p}" >&2
  exit 1
}

server_pid=""
test_pid=""

# The test's process tree (the test and everything it started, e.g. a
# console consumer), so a stopped test leaves nothing running.
# pgrep -P: exact parent pid, never a name pattern.
tree() {
  local p
  for p in $(pgrep -P "$1" 2>/dev/null); do tree "$p"; done
  echo "$1"
}
stop_test() {
  local pids p
  pids=$(tree "$test_pid")
  for p in $pids; do kill -TERM "$p" 2>/dev/null || true; done
  for _ in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$test_pid" 2>/dev/null || break; sleep 0.2; done
  for p in $pids; do kill -KILL "$p" 2>/dev/null || true; done
  wait "$test_pid" 2>/dev/null || true
  test_pid=""
}

cleanup() {
  if [[ -n "$test_pid" ]]; then stop_test; fi
  if [[ -n "$server_pid" ]]; then
    # The launcher stops the broker JVM (SIGTERM, then SIGKILL to its
    # process group) and exits.
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

env_file="$TEST_TMPDIR/${SERVER_NAME}.env"
rm -f "$env_file"
"$(resolve "$SERVER_SHORT")" &
server_pid=$!
# The launcher enforces its own ready_timeout; this outer bound only guards
# against a hung launcher.
deadline=$(( $(date +%s) + 900 ))
until [[ -f "$env_file" ]]; do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    wait "$server_pid" && rc=0 || rc=$?
    server_pid=""
    echo "kafka_test: the Kafka broker exited (status $rc) before it was ready; see [kafka] lines above" >&2
    exit 1
  fi
  (( $(date +%s) < deadline )) || { echo "kafka_test: timed out waiting for $env_file" >&2; exit 1; }
  sleep 0.1
done

set -a
# shellcheck disable=SC1090
source "$env_file"
set +a

# Not exec: the trap must stop the broker when the test ends. The test runs
# in the background (stdin kept) so the broker can be watched: if it exits
# while the test runs, the test is stopped and fails with the broker's
# status and log, instead of failing later on connection errors.
inner="$(resolve "$INNER_SHORT")"
"$inner" "$@" 0<&0 &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    wait "$server_pid" && rc=0 || rc=$?
    server_pid=""
    stop_test
    echo "kafka_test: the Kafka broker exited (status $rc) while the test was running; the test was stopped (see [kafka] lines above)." >&2
    exit 1
  fi
  sleep 0.1
done
wait "$test_pid" && rc=0 || rc=$?
test_pid=""
exit "$rc"
