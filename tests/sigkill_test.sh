#!/usr/bin/env bash
# The broker JVM never outlives its launcher: SIGTERM stops it cleanly
# (exit 0, env file removed); SIGKILL of the launcher (no handler runs)
# still takes the JVM down (Linux PR_SET_PDEATHSIG).
#
# Argv: <kafka_server>
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }
SERVER="$TEST_SRCDIR/_main/$1"
env_file="$TEST_TMPDIR/sigkill_server.env"

wait_env() {
  local pid="$1"
  for _ in $(seq 1 1800); do
    [[ -f "$env_file" ]] && return 0
    kill -0 "$pid" 2>/dev/null || fail "kafka_server exited before it was ready"
    sleep 0.1
  done
  fail "kafka_server not ready"
}
gone() {  # gone PID SECONDS
  for _ in $(seq 1 $(( $2 * 10 ))); do kill -0 "$1" 2>/dev/null || return 0; sleep 0.1; done
  return 1
}

# 1. SIGTERM.
"$SERVER" & launcher=$!
wait_env "$launcher"
jvm="$(sed -n 's/^KAFKA_PID=//p' "$env_file")"
kill -0 "$jvm" || fail "no JVM $jvm"
t=$(date +%s)
kill -TERM "$launcher"
wait "$launcher" && rc=0 || rc=$?
[[ "$rc" == 0 ]] || fail "SIGTERM: launcher exited $rc"
gone "$jvm" 5 || fail "SIGTERM: JVM $jvm still running"
[[ ! -e "$env_file" ]] || fail "SIGTERM: env file left"
echo "ok: SIGTERM stopped launcher and JVM $jvm in $(( $(date +%s) - t ))s"

if [[ "$(uname -s)" != Linux ]]; then echo "PASS (SIGKILL case is Linux-only)"; exit 0; fi

# 2. SIGKILL of the launcher.
"$SERVER" & launcher=$!
wait_env "$launcher"
jvm="$(sed -n 's/^KAFKA_PID=//p' "$env_file")"
kill -0 "$jvm" || fail "no JVM $jvm"
kill -KILL "$launcher"
wait "$launcher" 2>/dev/null || true
gone "$jvm" 10 || { kill -KILL "$jvm"; fail "SIGKILL: JVM $jvm outlived its launcher"; }
echo "ok: SIGKILL of the launcher took JVM $jvm down"
echo PASS
