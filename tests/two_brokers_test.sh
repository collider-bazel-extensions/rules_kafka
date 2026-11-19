#!/usr/bin/env bash
# The test half of a rules_itest service_test with two kafka_server
# services (kafka_a, kafka_b) and their health checks: both brokers run at
# once in one test, on their own ports with their own cluster ids and
# data dirs, and a record produced to one is not on the other.
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }

for s in kafka_a kafka_b; do
  [[ -f "$TEST_TMPDIR/$s.env" ]] || fail "no $TEST_TMPDIR/$s.env"
done
set -a; source "$TEST_TMPDIR/kafka_b.env"; set +a
B="$KAFKA_BOOTSTRAP_SERVERS"; B_ID="$KAFKA_CLUSTER_ID"; B_DIR="$KAFKA_STATE_DIR"; B_PID="$KAFKA_PID"
set -a; source "$TEST_TMPDIR/kafka_a.env"; set +a
A="$KAFKA_BOOTSTRAP_SERVERS"; A_ID="$KAFKA_CLUSTER_ID"; A_DIR="$KAFKA_STATE_DIR"; A_PID="$KAFKA_PID"
source "$KAFKA_TESTLIB"

[[ "$A" != "$B" ]] || fail "both brokers on $A"
[[ "$A_ID" != "$B_ID" ]] || fail "both brokers have cluster id $A_ID"
[[ "$A_DIR" == "$TEST_TMPDIR/kafka_a" && "$B_DIR" == "$TEST_TMPDIR/kafka_b" ]] || fail "state dirs $A_DIR $B_DIR"
[[ "$A_PID" != "$B_PID" ]] && kill -0 "$A_PID" && kill -0 "$B_PID" || fail "broker pids $A_PID $B_PID"
echo "ok: kafka_a on $A ($A_ID), kafka_b on $B ($B_ID)"

# Each broker's pre-created topic has its own partition count.
[[ "$(KAFKA_BOOTSTRAP_SERVERS=$A kafka_partitions shared)" == 2 ]] || fail "kafka_a shared partitions"
[[ "$(KAFKA_BOOTSTRAP_SERVERS=$B kafka_partitions shared)" == 1 ]] || fail "kafka_b shared partitions"

# A record on kafka_a only.
printf 'only-on-a\n' | KAFKA_BOOTSTRAP_SERVERS=$A kafka_produce isolated
[[ "$(KAFKA_BOOTSTRAP_SERVERS=$A kafka_consume isolated 1)" == only-on-a ]] || fail "kafka_a round-trip"
if KAFKA_BOOTSTRAP_SERVERS=$B kafka_topics --list | grep -qx isolated; then
  fail "kafka_b has kafka_a's topic"
fi
printf 'only-on-b\n' | KAFKA_BOOTSTRAP_SERVERS=$B kafka_produce isolated
[[ "$(KAFKA_BOOTSTRAP_SERVERS=$B kafka_consume isolated 1)" == only-on-b ]] || fail "kafka_b round-trip"
echo "ok: the brokers are independent"
echo PASS
