# rules_kafka

[Apache Kafka](https://kafka.apache.org/) for Bazel tests, two ways:

- **Per-test broker** (no cluster, no container runtime): `kafka_server`
  starts a single-node KRaft broker on free 127.0.0.1 ports for one test,
  with its data and logs in `$TEST_TMPDIR`. `kafka_test` is an `sh_test`
  with an auto-launched broker and shell helpers; `kafka_server` +
  `kafka_server_health_check` drop into a
  [rules_itest](https://github.com/dzbarsky/rules_itest) composition.
- **Cluster install** (kind / any cluster): `kafka_install` is a macro
  emitting a [`rules_kubectl`](https://github.com/collider-bazel-extensions/rules_kubectl)
  `kubectl_apply` target with Bitnami's pinned Kafka manifest and the
  right wait shape.

```python
load("@rules_kafka//:defs.bzl",
     "kafka_test", "kafka_server", "kafka_server_health_check",  # per-test broker
     "kafka_install", "kafka_health_check")                       # cluster install

kafka_test(name = "ingest_test", srcs = ["ingest_test.sh"], topics = ["events:3"])

kafka_install(name = "kafka_install_bin")          # default ns: kafka
kafka_health_check(name = "kafka_health_bin")
```

**Pinned versions:** per-test broker: Apache Kafka `4.4.0`
(`kafka_2.13-4.4.0.tgz`, sha512-verified) on rules_java's remote JDK 21
(Zulu 21.0.9). No host Java. Cluster install: Bitnami's `kafka` helm
chart `32.4.3` (Apache Kafka `4.0.0`). Smoke-fixture render — single combined controller+broker
pod, KRaft mode, PLAINTEXT listeners (no SASL/auth), emptyDir storage.
The values file is exported as `@rules_kafka//config:kafka-values.yaml`
for inspection / extension. Production deployments override every
listener/replica/auth/storage field.

**Supported platforms:** per-test broker: Linux and macOS on x86\_64 and
aarch64 (rules_java has a remote JDK for each), validated on Linux
x86\_64 in CI; needs `python3` on the host for the launcher. Cluster
install: any platform where rules_kubectl runs, validated on Linux
x86\_64 in CI.

---

## Contents

- [Installation](#installation) (Bzlmod-only)
- [Per-test broker](#per-test-broker) (`kafka_test`, `kafka_server`, `kafka_server_health_check`)
- [Quickstart](#quickstart) (cluster install)
- [Producing + consuming](#producing--consuming)
- [Macros](#macros)
- [Hermeticity exceptions](#hermeticity-exceptions)
- [Contributing](#contributing)

---

## Installation

```python
bazel_dep(name = "rules_kafka", version = "0.2.0")
archive_override(
    module_name = "rules_kafka",
    urls = ["https://github.com/collider-bazel-extensions/rules_kafka/archive/refs/tags/v0.2.0.tar.gz"],
    strip_prefix = "rules_kafka-0.2.0",
    integrity = "INTEGRITY_AFTER_TAG",
)

# rules_kafka depends on rules_kubectl, which isn't in the BCR either.
bazel_dep(name = "rules_kubectl", version = "0.1.0")
git_override(
    module_name = "rules_kubectl",
    remote = "https://github.com/collider-bazel-extensions/rules_kubectl",
    commit = "bb75b97850b08a18abcdca7af86514561a122b03",  # v0.1.0
)
```

That's all the wiring: rules_kafka's own `MODULE.bazel` pins Kafka 4.4.0
and names rules_java's remote JDK 21. Both are downloaded lazily, only
when a `kafka_server` / `kafka_test` target is built: a consumer that only
uses `kafka_install` fetches neither (CI's consumer job checks this). To
run another Kafka 4.x release, set it in your root module (the root's tag
wins; other modules' are ignored):

```python
kafka = use_extension("@rules_kafka//:extensions.bzl", "kafka")
kafka.version(version = "4.4.0")            # a pinned version, or:
# kafka.version(version = "4.3.2", sha512 = "<kafka_2.13-4.3.2.tgz.sha512>")
```

A version not pinned in `private/versions.bzl` needs `sha512` (the hex
digest from the release's `.sha512` file; `urls` default to
downloads.apache.org then archive.apache.org, `strip_prefix` to
`kafka_2.13-<version>`). Without it the build fails naming the pinned
versions.

The release archive (the tag tarball) leaves out `tests/` and `tools/`, whose packages load dev-only modules, so `bazel query @rules_kafka//...` works for consumers. A `git_override` clone still contains them; name packages instead (for example `@rules_kafka//:all`).

Bzlmod-only. Transitively pulls in
[`rules_kubectl`](https://github.com/collider-bazel-extensions/rules_kubectl).

---

## Per-test broker

Each `kafka_server` is one Kafka JVM in combined KRaft mode (broker +
controller, `node.id=1`), started for one test:

- listeners on two free ports, **127.0.0.1 only** (`PLAINTEXT` client
  listener, `CONTROLLER` listener); the advertised listener equals the
  bound one;
- storage formatted with a **random cluster id**
  (`kafka-storage.sh format --standalone`);
- single-node settings: `default.replication.factor`,
  `min.insync.replicas`, `offsets.topic.replication.factor`,
  `transaction.state.log.replication.factor` / `.min.isr` and the share
  coordinator's all 1; `offsets.topic.num.partitions`,
  `transaction.state.log.num.partitions` and
  `share.coordinator.state.topic.num.partitions` 1 (production's 50 slow
  a test's first consumer group down); `group.initial.rebalance.delay.ms=0`;
  `auto.create.topics.enable=true` unless `auto_create_topics = False`;
- data in `$TEST_TMPDIR/<name>/data`, `server.log` in
  `$TEST_TMPDIR/<name>/logs`, `java.io.tmpdir` in `$TEST_TMPDIR/<name>/tmp`;
  no JMX agent, no GC log, no `/tmp/hsperfdata_*` file. Nothing is written
  outside `$TEST_TMPDIR`, and nothing talks beyond loopback (the tests run
  with `block-network`);
- the JVM: `-Xmx512m` (`heap`), SerialGC and C1 only (start-up over
  throughput).

Ready means: the broker answers metadata (DescribeCluster lists the broker
and the controller, ListTopics works) and every `topics` entry exists with
its partition count and a leader per partition. Then it writes
`$TEST_TMPDIR/<name>.env`. On start-up failure it exits 1 and prints the
tail of `server.log` (or the JVM's console output). On SIGTERM/SIGINT it
stops the broker (SIGTERM to the JVM's PID, Kafka's clean shutdown;
SIGKILL to its process group after 30 s), removes the env file and exits
0. On Linux the JVM also dies if the launcher is SIGKILLed
(`PR_SET_PDEATHSIG`).

Start-up is dominated by JVM start: two JVMs run before the env file
appears (`kafka-storage.sh format`, then the broker plus a small readiness
JVM). Measured on a shared 16-core machine: 8-15 s for one broker at load
average ~22, 24-31 s with 8 brokers starting at once at load ~87. CI's
"Broker start-up times" step prints the times on GitHub's runners. The launcher logs it:
`[kafka] Kafka 4.4.0 ready on 127.0.0.1:57705 (controller 59827) in 8.0s
(format 2.8s, broker + readiness 5.2s)`.

### Environment

The env file (sourced by `kafka_test`; `set -a; source` it yourself
elsewhere) has these names. They are stable:

| Variable | Value |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:<port>` — what clients connect to |
| `KAFKA_HOST` | `127.0.0.1` |
| `KAFKA_PORT` | the client (`PLAINTEXT`) listener port |
| `KAFKA_CONTROLLER_PORT` | the KRaft controller listener port |
| `KAFKA_CLUSTER_ID` | the random cluster id storage was formatted with |
| `KAFKA_TOPICS` | the pre-created topics, comma-separated (empty if none) |
| `KAFKA_VERSION` | e.g. `4.4.0` |
| `KAFKA_PID` | the broker JVM's PID |
| `KAFKA_STATE_DIR` | `$TEST_TMPDIR/<name>` (`config/`, `data/`, `logs/`, `tmp/`) |
| `KAFKA_LOG` | `$TEST_TMPDIR/<name>/logs/server.log` |
| `KAFKA_HOME` | the Kafka distribution (`bin/`, `libs/`, `config/`) |
| `KAFKA_JAVA_HOME` | the JDK the broker runs on |
| `KAFKA_TESTLIB` | the shell helpers below |

librdkafka / confluent-kafka-go (e.g. OpenMeter's sink worker) and the
Java client take `KAFKA_BOOTSTRAP_SERVERS` as `bootstrap.servers` as is.

### `kafka_test`

```python
load("@rules_kafka//:defs.bzl", "kafka_test")

kafka_test(
    name = "ingest_test",
    srcs = ["ingest_test.sh"],
    topics = ["events:3", "audit"],    # optional; "name" or "name:partitions"
    auto_create_topics = True,         # default
    heap = "512m",                     # default; the broker's -Xmx
    jvm_flags = [],                    # extra broker JVM options
    properties = {"log.retention.ms": "60000"},  # extra broker properties
    ready_timeout = 120,               # seconds; default
    tags = ["block-network"],          # recommended: loopback only
)
```

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$KAFKA_TESTLIB"
printf 'hello\n' | kafka_produce events
[[ "$(kafka_consume events 1)" == hello ]]
```

`data`, `deps`, `args`, `env`, `size` (default `"medium"`), `timeout` and
other `sh_test` attributes are forwarded. The trampoline starts the broker,
sources the env file, runs the test (not `exec`: it stops the broker when
the test ends, and stops the test if the broker dies first), and
propagates the test's exit status. Each target has its own broker, so
tests are safe under `--jobs=N`.

`properties` can't set what `kafka_server` owns: `listeners`,
`advertised.listeners`, `process.roles`, `node.id`, `controller.*`,
`listener.security.protocol.map`, `inter.broker.listener.name`,
`log.dir(s)`, `metadata.log.dir`, `auto.create.topics.enable` (use
`auto_create_topics`). Bad topics, heap sizes, timeouts and JVM flags
fail at analysis time.

### Shell helpers (`source "$KAFKA_TESTLIB"`)

Over Kafka's own CLI scripts, on the pinned JDK, with their logs and temp
files under `$TEST_TMPDIR`. Each starts a JVM (about a second).

| Helper | Does |
|---|---|
| `kafka_produce TOPIC [--keyed] [ARGS...]` | each stdin line becomes a record (`acks=all`); `--keyed`: `KEY<TAB>VALUE` lines |
| `kafka_consume TOPIC COUNT [--keyed] [ARGS...]` | prints the first COUNT values from the beginning; fails if fewer arrive in `$KAFKA_CONSUME_TIMEOUT_MS` (60000). `--partition N` reads one partition; `--group G` commits offsets |
| `kafka_topics ARGS...` | `kafka-topics.sh --bootstrap-server <broker> ARGS` |
| `kafka_create_topic TOPIC [PARTITIONS]` | creates it (`--if-not-exists`) |
| `kafka_partitions TOPIC` | prints its partition count |
| `kafka_cli SCRIPT ARGS...` | any `$KAFKA_HOME/bin/SCRIPT.sh` (e.g. `kafka-consumer-groups`) |

They use `$KAFKA_BOOTSTRAP_SERVERS`; set it per call for another broker:
`KAFKA_BOOTSTRAP_SERVERS=$OTHER kafka_produce t <<<x`.

### `kafka_server` + `kafka_server_health_check` (rules_itest)

```python
load("@rules_itest//:itest.bzl", "itest_service", "service_test")
load("@rules_kafka//:defs.bzl", "kafka_server", "kafka_server_health_check")

kafka_server(name = "kafka", topics = ["events:3"], tags = ["manual"])
kafka_server_health_check(name = "kafka_health", server = ":kafka", tags = ["manual"])

itest_service(
    name = "kafka_svc",
    exe = ":kafka",
    health_check = ":kafka_health",
    # Kafka's clean shutdown takes a few seconds; rules_itest's 1s default
    # would SIGKILL the launcher (the broker JVM still dies with it on Linux).
    shutdown_timeout = "60s",
)

service_test(
    name = "ingest_test",
    services = [":kafka_svc"],     # alongside pg_server, valkey_server, ...
    test = ":ingest_test_bin",
    tags = ["block-network"],
)
```

`kafka_server` takes the same attributes as `kafka_test` (`topics`,
`auto_create_topics`, `heap`, `jvm_flags`, `properties`,
`ready_timeout` as a string). It writes `$TEST_TMPDIR/kafka.env` (named
after the target); `kafka_server_health_check` exits 0 once that file
exists. Services or tests in any language read it, or a wrapper exports
`KAFKA_BOOTSTRAP_SERVERS`. `tests/BUILD.bazel`'s `two_brokers_test` runs two
brokers side by side this way. `bazel run` of a `kafka_server` works too
(without `TEST_TMPDIR` it uses a fresh `mktemp -d` directory).

Providers: `KafkaServerInfo` (`version`, `topics`, `auto_create_topics`),
`KafkaDistributionInfo` (the `@kafka_dist` release).

---

## Quickstart (cluster install)

```python
load("@rules_itest//:itest.bzl", "itest_service", "service_test")
load("@rules_kafka//:defs.bzl", "kafka_install", "kafka_health_check")
load("@rules_kind//:defs.bzl", "kind_cluster", "kind_health_check")
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

# 1. Cluster.
kind_cluster(name = "cluster", k8s_version = "1.35")
kind_health_check(name = "cluster_health", cluster = ":cluster")
itest_service(name = "kind_svc", exe = ":cluster", health_check = ":cluster_health")

# 2. Kafka.
kafka_install(name = "kafka_install_bin")
kafka_health_check(name = "kafka_health_bin")
sh_binary(name = "kafka_install_wrapper", srcs = ["install_wrapper.sh"], data = [":kafka_install_bin"])
sh_binary(name = "kafka_health_wrapper",  srcs = ["health_wrapper.sh"],  data = [":kafka_health_bin"])

itest_service(
    name = "kafka_svc",
    exe = ":kafka_install_wrapper",
    deps = [":kind_svc"],
    health_check = ":kafka_health_wrapper",
)

# 3. Your produce/consume workload — see `examples/` for shapes.
```

`install_wrapper.sh` / `health_wrapper.sh` are ~20-line shims — see
`tests/install_wrapper.sh` for the canonical shape (sources kind's env
file under `set -a`, exec's the rule binary).

---

## Producing + consuming

Once Kafka is up, the in-cluster client service
`kafka.<namespace>.svc.cluster.local:9092` accepts plain Kafka wire
protocol (PLAINTEXT, no auth in v0.1 smoke-fixture render). The pinned
release name `kafka` makes that DNS deterministic.

The simplest pattern (which `tests/smoke_test.sh` uses): `kubectl exec`
into the broker pod and use the Kafka CLI tools the bitnami image
ships at `/opt/bitnami/kafka/bin/`:

```bash
# Create a topic.
kubectl -n kafka exec kafka-controller-0 -- \
    /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic my-topic --partitions 1 --replication-factor 1

# Produce.
echo "hello" | kubectl -n kafka exec -i kafka-controller-0 -- \
    /opt/bitnami/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic my-topic

# Consume.
kubectl -n kafka exec kafka-controller-0 -- \
    /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic my-topic \
    --from-beginning --max-messages 1 --timeout-ms 30000
```

For a real client workload from another pod, point your producer/consumer
at `kafka.kafka.svc.cluster.local:9092`. See [`examples/`](examples/).

---

## Macros

### `kafka_install`

```python
kafka_install(
    name = "kafka_install_bin",
    namespace = "kafka",        # default
    wait_timeout = "300s",      # default
)
```

Expands to a `kubectl_apply(...)` target that:

- Applies `@rules_kafka//private/manifests:kafka.yaml`.
- `create_namespace = True` (default `kafka`).
- `server_side = True`.
- `wait_for_rollouts = ["sts/kafka-controller"]` — the single combined
  controller+broker StatefulSet.
- `wait_timeout = "300s"` — bitnami/kafka image is ~500MB and KRaft
  cluster formation takes a few seconds for the lone node to elect
  itself leader.

Drops into `itest_service.exe`.

### `kafka_health_check`

```python
kafka_health_check(name = "kafka_health_bin", namespace = "kafka")
```

Drops into `itest_service.health_check`. Same wait shape with
`--timeout=0s`.

---

## Hermeticity exceptions

| Component | Status | Notes |
|---|---|---|
| Kafka (per-test broker) | Fully hermetic. `kafka_2.13-4.4.0.tgz` pinned by sha512 in `private/versions.bzl` (from the release's published `.sha512`). | Regenerate with `tools/update_versions.sh [<version>]`; CI runs it with `--check`. |
| JDK (per-test broker) | rules_java's remote JDK 21 (Zulu 21.0.9), named directly. | No host Java is used. |
| `python3` (per-test broker) | Host. | The launcher (as rules_valkey's). |
| Kafka manifest | Fully hermetic. URL + sha256 pinned in `tools/versions.bzl`; pre-rendered + committed. | Re-render via `bash tools/render_kafka.sh <ver>`. |
| `kubectl` | Inherited from `rules_kubectl`. | |
| Target cluster | Out of scope. | |
| Kafka container image | Pulled at runtime by the cluster's nodes. `docker.io/bitnami/kafka:4.0.0-debian-12-r10` from the chart. | Future: pre-load via `kind_cluster.images`. |

---

## Contributing

PRs welcome. Conventions match the sibling rule sets:

- New rules need an analysis test in `tests/analysis_tests.bzl`.
- `bazel test //tests/...` runs the analysis and per-test broker tests (no
  container runtime). The kind smoke is `bazel test //tests:smoke_test`
  (tagged `manual`).
- Bumping Kafka: `tools/update_versions.sh [<version>]` (default: the
  latest 4.x at downloads.apache.org), commit `private/versions.bzl`.
- Bumping the pinned chart version: edit `tools/versions.bzl`, add a
  `helm_template + sh_binary` block in `tools/BUILD.bazel`, run
  `bash tools/render_kafka.sh <new-version>`, commit.
- `MODULE.bazel.lock` is intentionally not committed.

### Help wanted

- macOS validation
- SASL/SCRAM smoke (re-enable SASL listeners; smoke a credentialled connect)
- TLS smoke (mutual-TLS listeners with chart-self-signed certs)
- Multi-broker HA smoke (replicaCount: 3, replication-factor: 3, fault inject one)
- Strimzi-based variant (operator + `Kafka`/`KafkaTopic` CRDs) — different family, same goal
- Kafka Connect / Schema Registry / KRaft-mode tiered-storage compositions

---

## Requesting a new rules package

Want Bazel rules for another tool or service?
[Request a rules package](https://github.com/collider-bazel-extensions/.github/issues/new?template=request-a-rules-package.yml).
The [organization page](https://github.com/collider-bazel-extensions) lists every package.

## License

[Apache License 2.0](LICENSE) (SPDX: `Apache-2.0`).

Manifests in `private/manifests/` that are rendered from upstream charts or releases remain under their upstream projects' licences.
