#!/usr/bin/env bash
# kafka_test(topics = ["events:3", "audit", "orders:2"], auto_create_topics =
# False): the topics exist with their partition counts before the test
# starts, KAFKA_TOPICS lists them, records round-trip per partition, and
# producing to a missing topic fails (auto-create off).
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }
source "$KAFKA_TESTLIB"

[[ "$KAFKA_TOPICS" == "events,audit,orders" ]] || fail "KAFKA_TOPICS='$KAFKA_TOPICS'"
listed="$(kafka_topics --list | grep -v '^__' | sort | tr '\n' ' ')"
[[ "$listed" == "audit events orders " ]] || fail "topics listed: '$listed'"
for tp in events:3 audit:1 orders:2; do
  got="$(kafka_partitions "${tp%%:*}")"
  [[ "$got" == "${tp##*:}" ]] || fail "${tp%%:*} has $got partitions, want ${tp##*:}"
done
echo "ok: events (3), audit (1), orders (2) exist"
grep -q '^auto.create.topics.enable=false$' "$KAFKA_STATE_DIR/config/server.properties" \
  || fail "auto.create.topics.enable"

# Records spread over events' partitions, read back per partition.
seq 1 30 | sed 's/^/e-/' | kafka_produce events \
  --command-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner
seen=""
for p in 0 1 2; do
  n="$(KAFKA_CONSUME_TIMEOUT_MS=10000 kafka_consume events 1 --partition "$p" || true)"
  seen="$seen${n:+$n }"
done
[[ "$(wc -w <<<"$seen")" -eq 3 ]] || fail "records across partitions: '$seen'"
echo "ok: records in all three partitions of events ($seen)"

# Auto-create is off: producing to a missing topic fails.
if printf 'x\n' | KAFKA_CLI_HEAP_OPTS=-Xmx128m kafka_produce missing \
     --command-property max.block.ms=3000 >"$TEST_TMPDIR/missing.out" 2>&1; then
  if kafka_topics --list | grep -qx missing; then fail "the topic 'missing' was auto-created"; fi
fi
kafka_topics --list | grep -qx missing && fail "the topic 'missing' exists"
grep -qiE "not present in metadata|UNKNOWN_TOPIC|TimeoutException" "$TEST_TMPDIR/missing.out" \
  || { cat "$TEST_TMPDIR/missing.out"; fail "no metadata error producing to a missing topic"; }
echo "ok: no auto-create"
echo PASS
