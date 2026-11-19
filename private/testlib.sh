#!/usr/bin/env bash
# Shell helpers for a kafka_server, over Kafka's own CLI scripts:
#
#   source "$KAFKA_TESTLIB"     # after sourcing the server's env file
#                               # (kafka_test does both)
#
#   kafka_cli SCRIPT ARGS...    run $KAFKA_HOME/bin/SCRIPT(.sh) on the pinned
#                               JDK: logs and java.io.tmpdir under
#                               $TEST_TMPDIR, a small heap, no JMX
#   kafka_topics ARGS...        kafka-topics.sh --bootstrap-server <broker> ARGS
#   kafka_create_topic TOPIC [PARTITIONS]
#                               create TOPIC (default 1 partition); ok if it exists
#   kafka_partitions TOPIC      print TOPIC's partition count
#   kafka_produce TOPIC [ARGS...]
#                               each stdin line becomes one record (acks=all,
#                               waits until written). With --keyed, lines are
#                               KEY<TAB>VALUE. Extra ARGS go to
#                               kafka-console-producer.sh.
#   kafka_consume TOPIC COUNT [ARGS...]
#                               print the first COUNT records' values from the
#                               beginning of TOPIC, one per line; fails if
#                               fewer arrive within $KAFKA_CONSUME_TIMEOUT_MS
#                               (default 60000). With --keyed, KEY<TAB>VALUE.
#                               With --partition N: that partition, from
#                               --offset (default earliest).
#                               Extra ARGS go to kafka-console-consumer.sh
#                               (e.g. --group G, --partition N).
#
# They talk to $KAFKA_BOOTSTRAP_SERVERS; set it per call to use another
# broker (KAFKA_BOOTSTRAP_SERVERS=$OTHER kafka_produce t <<<x). Every helper
# starts a JVM (about a second each).

kafka_cli() {
  local script="$1"; shift
  [[ "$script" == *.sh ]] || script="$script.sh"
  local tmp="${TEST_TMPDIR:-${KAFKA_STATE_DIR:?kafka_cli: source the kafka_server env file first}}/kafka-cli"
  mkdir -p "$tmp/logs" "$tmp/tmp"
  (
    # The same environment the launcher gives Kafka's scripts: nothing of
    # the caller's leaks in, nothing is written outside $TEST_TMPDIR.
    unset CLASSPATH JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS JMX_PORT KAFKA_DEBUG \
          KAFKA_OPTS KAFKA_LOG4J_OPTS KAFKA_GC_LOG_OPTS EXTRA_ARGS
    export JAVA_HOME="$KAFKA_JAVA_HOME"
    export LOG_DIR="$tmp/logs"
    export KAFKA_HEAP_OPTS="${KAFKA_CLI_HEAP_OPTS:--Xmx256m}"
    export KAFKA_JVM_PERFORMANCE_OPTS="-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -Xshare:auto -XX:-UsePerfData -Djava.awt.headless=true -Djava.io.tmpdir=$tmp/tmp"
    export KAFKA_JMX_OPTS="-Djava.awt.headless=true"  # non-empty: no JMX agent
    exec "$KAFKA_HOME/bin/$script" "$@"
  )
}

kafka_topics() {
  kafka_cli kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"
}

kafka_create_topic() {
  local topic="${1:?kafka_create_topic TOPIC [PARTITIONS]}"
  kafka_topics --create --if-not-exists --topic "$topic" \
    --partitions "${2:-1}" --replication-factor 1 >/dev/null
}

kafka_partitions() {
  local topic="${1:?kafka_partitions TOPIC}" out
  out="$(kafka_topics --describe --topic "$topic")" || return 1
  printf '%s\n' "$out" | sed -n 's/.*PartitionCount: *\([0-9][0-9]*\).*/\1/p' | head -n 1
}

kafka_produce() {
  local topic="${1:?kafka_produce TOPIC [--keyed] [ARGS...]}"; shift
  local extra=()
  if [[ "${1:-}" == --keyed ]]; then
    shift
    extra=(--reader-property parse.key=true --reader-property "key.separator=$(printf '\t')")
  fi
  kafka_cli kafka-console-producer --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$topic" --command-property acks=all --command-property linger.ms=0 \
    ${extra[@]+"${extra[@]}"} "$@"
}

kafka_consume() {
  local topic="${1:?kafka_consume TOPIC COUNT [--keyed] [ARGS...]}"
  local count="${2:?kafka_consume TOPIC COUNT [--keyed] [ARGS...]}"
  shift 2
  local extra=()
  if [[ "${1:-}" == --keyed ]]; then
    shift
    extra=(--formatter-property print.key=true --formatter-property "key.separator=$(printf '\t')")
  fi
  # --from-beginning, unless the caller picks a partition (then --offset,
  # default earliest; the consumer refuses both together).
  local a start=(--from-beginning)
  for a in "$@"; do
    case "$a" in
      --partition) start=(--offset earliest) ;;
    esac
  done
  for a in "$@"; do
    case "$a" in
      --offset) start=() ;;
    esac
  done
  local out n err="${TEST_TMPDIR:-$KAFKA_STATE_DIR}/kafka-cli/consume.err"
  mkdir -p "${err%/*}"
  out="$(kafka_cli kafka-console-consumer --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$topic" ${start[@]+"${start[@]}"} --max-messages "$count" \
    --timeout-ms "${KAFKA_CONSUME_TIMEOUT_MS:-60000}" \
    ${extra[@]+"${extra[@]}"} "$@" 2>"$err")" || true
  n=0
  [[ -z "$out" ]] || n="$(printf '%s\n' "$out" | wc -l | tr -d ' ')"
  if (( n < count )); then
    echo "kafka_consume: got $n of $count record(s) from $topic:" >&2
    sed 's/^/  /' "$err" >&2
    [[ -z "$out" ]] || printf '%s\n' "$out"
    return 1
  fi
  printf '%s\n' "$out"
}
