#!/usr/bin/env bash
# kafka_test's trampoline stops the broker when the test ends: run it to
# completion (in its own session, so nothing else kills its children), then
# check the broker JVM is gone (by the pid it recorded), both ports are
# closed and the env file is removed.
#
# Argv: <trampoline> <record_broker server> <record_broker inner>
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }

export RECORD_FILE="$TEST_TMPDIR/record"
inner_tmp="$TEST_TMPDIR/inner"; mkdir -p "$inner_tmp"
start=$(date +%s)
TEST_TMPDIR="$inner_tmp" setsid "$TEST_SRCDIR/_main/$1" "$2" "$3" record_broker_kafka \
  || fail "trampoline exited $?"
read -r pid port cport < "$RECORD_FILE"
[[ -n "$pid" && -n "$port" ]] || fail "nothing recorded"
echo "ok: the test ran against broker pid $pid on $port (trampoline took $(( $(date +%s) - start ))s)"
kill -0 "$pid" 2>/dev/null && fail "broker JVM $pid still running after the test ended"
for p in "$port" "$cport"; do
  (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null && fail "something still listening on $p"
done
[[ -e "$inner_tmp/record_broker_kafka.env" ]] && fail "env file left behind"
grep -q "shutting down" "$inner_tmp/record_broker_kafka/logs/server.log" \
  || fail "no clean shutdown in server.log"
echo "PASS: broker $pid stopped, ports $port/$cport closed, env file removed"
