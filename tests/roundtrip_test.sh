#!/usr/bin/env bash
# kafka_test with the defaults: the env file's variables, produce/consume
# round-trips through the testlib helpers (plain, keyed, several records,
# a topic auto-created by the first produce, a consumer group), and the
# test's own args.
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }
source "$KAFKA_TESTLIB"

[[ "$#" -eq 2 && "$1" == one && "$2" == "two words" ]] || fail "args: $*"
for v in KAFKA_BOOTSTRAP_SERVERS KAFKA_HOST KAFKA_PORT KAFKA_CONTROLLER_PORT KAFKA_CLUSTER_ID \
         KAFKA_VERSION KAFKA_PID KAFKA_STATE_DIR KAFKA_LOG KAFKA_HOME KAFKA_JAVA_HOME KAFKA_TESTLIB; do
  [[ -n "${!v:-}" ]] || fail "$v not set"
done
[[ "$KAFKA_BOOTSTRAP_SERVERS" == "127.0.0.1:$KAFKA_PORT" ]] || fail "bootstrap $KAFKA_BOOTSTRAP_SERVERS"
[[ "$KAFKA_HOST" == 127.0.0.1 ]] || fail "host $KAFKA_HOST"
[[ "$KAFKA_PORT" != "$KAFKA_CONTROLLER_PORT" ]] || fail "one port for both listeners"
[[ "${#KAFKA_CLUSTER_ID}" -eq 22 ]] || fail "cluster id '$KAFKA_CLUSTER_ID'"
[[ -z "$KAFKA_TOPICS" ]] || fail "KAFKA_TOPICS='$KAFKA_TOPICS' with no topics"
[[ "$KAFKA_STATE_DIR" == "$TEST_TMPDIR/roundtrip_test_kafka" ]] || fail "state dir $KAFKA_STATE_DIR"
[[ -f "$KAFKA_LOG" && "$KAFKA_LOG" == "$KAFKA_STATE_DIR"/* ]] || fail "log $KAFKA_LOG"
kill -0 "$KAFKA_PID" || fail "broker pid $KAFKA_PID not running"
[[ -x "$KAFKA_JAVA_HOME/bin/java" ]] || fail "no java in $KAFKA_JAVA_HOME"
"$KAFKA_JAVA_HOME/bin/java" -XX:-UsePerfData -version 2>&1 | grep -q 'version "21' || fail "the JDK isn't 21"
echo "ok: env ($KAFKA_BOOTSTRAP_SERVERS, cluster $KAFKA_CLUSTER_ID, Kafka $KAFKA_VERSION)"

# Listening on loopback only, both ports.
for p in "$KAFKA_PORT" "$KAFKA_CONTROLLER_PORT"; do
  (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null || fail "nothing on 127.0.0.1:$p"
done
grep -q "^listeners=PLAINTEXT://127.0.0.1:$KAFKA_PORT,CONTROLLER://127.0.0.1:$KAFKA_CONTROLLER_PORT$" \
  "$KAFKA_STATE_DIR/config/server.properties" || fail "listeners"
grep -q "^advertised.listeners=PLAINTEXT://127.0.0.1:$KAFKA_PORT," \
  "$KAFKA_STATE_DIR/config/server.properties" || fail "advertised listener"
echo "ok: listeners on 127.0.0.1:$KAFKA_PORT / $KAFKA_CONTROLLER_PORT"

# 1. One record; the topic doesn't exist yet: auto-created by the produce.
payload="hello-$RANDOM-$$"
printf '%s\n' "$payload" | kafka_produce greetings
got="$(kafka_consume greetings 1)"
[[ "$got" == "$payload" ]] || fail "consumed '$got', produced '$payload'"
[[ "$(kafka_partitions greetings)" == 1 ]] || fail "auto-created topic partitions"
echo "ok: produce/consume round-trip (auto-created topic)"

# 2. Several records, in order (one partition).
seq 1 50 | sed 's/^/msg-/' | kafka_produce numbers
got="$(kafka_consume numbers 50)"
[[ "$got" == "$(seq 1 50 | sed 's/^/msg-/')" ]] || fail "50 records: $got"
echo "ok: 50 records in order"

# 3. Keyed records.
printf 'k1\tv1\nk2\tv2\n' | kafka_produce keyed --keyed
got="$(kafka_consume keyed 2 --keyed)"
[[ "$got" == "$(printf 'k1\tv1\nk2\tv2')" ]] || fail "keyed: $got"
echo "ok: keyed records"

# 4. A consumer group commits offsets (the __consumer_offsets topic works).
kafka_consume numbers 50 --group g1 >/dev/null
printf 'msg-51\n' | kafka_produce numbers
got="$(kafka_consume numbers 1 --group g1)"
[[ "$got" == msg-51 ]] || fail "the group resumed at '$got', not msg-51"
echo "ok: consumer group resumes from its committed offset"

# 5. Asking for more records than exist fails, naming the count.
if KAFKA_CONSUME_TIMEOUT_MS=3000 kafka_consume greetings 2 >/dev/null 2>"$TEST_TMPDIR/short.err"; then
  fail "kafka_consume of 2 records from a 1-record topic passed"
fi
grep -q "got 1 of 2 record(s) from greetings" "$TEST_TMPDIR/short.err" || { cat "$TEST_TMPDIR/short.err"; fail "short consume message"; }
echo "ok: a short consume fails"
echo PASS
