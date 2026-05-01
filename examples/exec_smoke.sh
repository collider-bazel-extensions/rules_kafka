#!/usr/bin/env bash
# Minimal produce/consume round-trip via kubectl exec into the broker.
# Same pattern the rule set's tests/smoke_test.sh uses — extracted here
# for reference. Run after `kafka_install` is healthy.
#
# Assumes:
#   - kubectl on PATH, KUBECONFIG pointing at the cluster.
#   - Kafka installed in namespace `kafka`, release name `kafka`,
#     PLAINTEXT listener on 9092.
set -euo pipefail

NS="${NS:-kafka}"
POD="${POD:-kafka-controller-0}"
TOPIC="example-$RANDOM"
PAYLOAD="hello-$RANDOM-$(date +%s)"
KAFKA_BIN="/opt/bitnami/kafka/bin"

echo "[exec_smoke] creating topic $TOPIC"
kubectl -n "$NS" exec "$POD" -- \
    "$KAFKA_BIN/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo "[exec_smoke] producing '$PAYLOAD'"
echo "$PAYLOAD" | kubectl -n "$NS" exec -i "$POD" -- \
    "$KAFKA_BIN/kafka-console-producer.sh" \
    --bootstrap-server localhost:9092 --topic "$TOPIC"

echo "[exec_smoke] consuming"
GOT=$(kubectl -n "$NS" exec "$POD" -- \
    "$KAFKA_BIN/kafka-console-consumer.sh" \
    --bootstrap-server localhost:9092 --topic "$TOPIC" \
    --from-beginning --max-messages 1 --timeout-ms 30000 2>/dev/null \
    | tr -d '\r')

if [[ "$GOT" == "$PAYLOAD" ]]; then
  echo "[exec_smoke] OK — round-tripped '$PAYLOAD'"
else
  echo "[exec_smoke] FAIL — produced '$PAYLOAD', got '$GOT'"
  exit 1
fi
