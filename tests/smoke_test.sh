#!/usr/bin/env bash
# Round-trip a unique payload through Kafka to prove the controller +
# broker + producer/consumer protocol path all work end-to-end.
# Strategy:
#
#   1. `kubectl exec` into the kafka-controller-0 pod (combined
#      controller+broker, ships with the kafka-*.sh CLI tools at
#      /opt/bitnami/kafka/bin/).
#   2. Create a topic with kafka-topics.sh.
#   3. Produce a unique payload via kafka-console-producer.sh, sending
#      one line on stdin.
#   4. Consume from offset 0 via kafka-console-consumer.sh with
#      `--max-messages 1` so it exits cleanly after reading the one
#      record.
#   5. Assert the consumed line equals the produced line.
#
# Proves: KRaft metadata controller + broker log append + topic
# auto-resolution + offset-zero seek + protocol read-path. Not
# "kafka pod is up."
set -euo pipefail

CLUSTER_NAME="cluster"
env_file="$TEST_TMPDIR/${CLUSTER_NAME}.env"
[[ -f "$env_file" ]] || { echo "missing kind env file" >&2; exit 1; }
# shellcheck disable=SC1090
source "$env_file"

KCTL=("$KUBECTL" --kubeconfig="$KUBECONFIG")

NS="kafka"
POD="kafka-controller-0"
TOPIC="rules-kafka-smoke-$RANDOM"
PAYLOAD="rules_kafka-smoke-$RANDOM-$(date +%s)"
BOOTSTRAP="localhost:9092"

# Path to the bitnami/kafka image's CLI tools.
KAFKA_BIN="/opt/bitnami/kafka/bin"

echo "smoke_test: creating topic '$TOPIC'"
"${KCTL[@]}" -n "$NS" exec "$POD" -- \
    "$KAFKA_BIN/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --create --topic "$TOPIC" \
    --partitions 1 --replication-factor 1

echo "smoke_test: producing payload to '$TOPIC'"
# `kafka-console-producer.sh` reads stdin one line at a time, sends each
# record asynchronously, and on stdin EOF calls producer.close() which
# blocks for in-flight acks. With `echo "$PAYLOAD" | ...`, EOF arrives
# instantly — observed in CI: producer exits (no error) but the record
# isn't durably committed before kubectl exec tears down. Two things
# guard against this:
#   - `acks=all` makes the broker wait for the in-sync replica set to
#     write before acknowledging (RF=1 here so it's just the one
#     broker; still, the producer waits for that ack before exit).
#   - `(echo ...; sleep 3)` keeps stdin open for 3s past the message,
#     giving producer.close() room to flush + ack before stdin EOF
#     triggers JVM shutdown.
( echo "$PAYLOAD"; sleep 3 ) | "${KCTL[@]}" -n "$NS" exec -i "$POD" -- \
    "$KAFKA_BIN/kafka-console-producer.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --producer-property acks=all

# Consumer with retry. A single 30s consumer call is racy if the broker
# took >30s to advance the high-water-mark visible to a new consumer
# group. Retry up to 3x; total ceiling 90s.
echo "smoke_test: consuming from offset 0 (max 1 message)"
GOT=""
for attempt in 1 2 3; do
  GOT=$("${KCTL[@]}" -n "$NS" exec "$POD" -- \
      "$KAFKA_BIN/kafka-console-consumer.sh" \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$TOPIC" \
      --from-beginning \
      --max-messages 1 \
      --timeout-ms 30000 2>/dev/null | tr -d '\r' | head -1)
  if [[ -n "$GOT" ]]; then break; fi
  echo "smoke_test: attempt $attempt got nothing; retrying"
done

if [[ "$GOT" != "$PAYLOAD" ]]; then
  echo "smoke_test: FAIL — payload didn't round-trip" >&2
  echo "  expected: '$PAYLOAD'" >&2
  echo "  got:      '$GOT'" >&2
  echo "---- topic describe ----" >&2
  "${KCTL[@]}" -n "$NS" exec "$POD" -- \
      "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
      --describe --topic "$TOPIC" >&2 || true
  echo "---- broker logs (tail) ----" >&2
  "${KCTL[@]}" -n "$NS" logs "$POD" --tail=80 >&2 || true
  exit 1
fi

echo "smoke_test: OK — payload round-tripped through Kafka (produce -> log -> consume)"
