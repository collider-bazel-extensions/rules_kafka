#!/usr/bin/env bash
# kafka_test body for cleanup_test: records the broker's pid, ports and
# env file, then passes.
set -euo pipefail
printf '%s %s %s\n' "$KAFKA_PID" "$KAFKA_PORT" "$KAFKA_CONTROLLER_PORT" > "$RECORD_FILE"
