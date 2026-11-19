# rules_kafka — design decisions

[Apache Kafka](https://kafka.apache.org/) for Bazel tests. Two families:

- **Cluster install** (v0.1): same shape as `rules_loki` / `rules_grafana` /
  `rules_harbor` / `rules_external_secrets` — pure glue over
  `rules_kubectl` + `rules_helm`. Decisions 1-15 below.
- **Per-test broker** (v0.2): the per-test-service shape of rules_pg's
  `pg_server` and rules_valkey's `valkey_server`. Decisions 16-35 below.

## Decided

| # | Decision | Choice | Source |
|---|---|---|---|
| 1 | Bzlmod / WORKSPACE | **Bzlmod-only at v0.1.** | Sibling-family |
| 2 | Module / repo name | `rules_kafka`. | Convention |
| 3 | Architecture | **Layered.** Two macros wrapping `kubectl_apply` / `kubectl_apply_health_check`. | rules_loki / rules_grafana / rules_harbor / rules_external_secrets |
| 4 | Manifest provisioning | Bitnami's `kafka` helm chart **pre-rendered** into `private/manifests/kafka.yaml` via `rules_helm`. Committed. | rules_loki pattern |
| 5 | Distribution mode | **Bitnami chart, KRaft mode.** Strimzi is the operator-based alternative — different family (operator install + CR-driven smoke), comparable to rules_cloudnativepg's shape. Bitnami matches rules_loki's pure-glue shape exactly. | See "Bitnami vs. Strimzi" below |
| 6 | Single-version | One pinned chart (`32.4.3`, Apache Kafka `4.0.0`). Multi-version is the rules_cloudnativepg pattern; add it if a real consumer wants two versions side-by-side. | Pragmatic |
| 7 | Render mode | **Smoke fixture.** Single combined controller+broker pod (`controller.replicaCount: 1`, `controllerOnly: false`), KRaft mode, **PLAINTEXT** listeners (no SASL/auth), emptyDir storage, no metrics, no NetworkPolicy. README warns the values are not a production starting point. | Smoke pragmatic |
| 8 | Public surface | `kafka_install`, `kafka_health_check`. **No `kafka_topic` rule** — topics are runtime concerns (consumers create them via `kafka-topics.sh` or via `auto.create.topics.enable`); a Bazel rule would be a YAML translator over the Kafka admin API. v0.1 documents the YAML pattern + ships `examples/`. | Smaller surface |
| 9 | Namespace | `kafka` (default, idempotent create). | Convention |
| 10 | Wait shape | One StatefulSet — `kafka-controller`. With `controllerOnly: false`, this single StatefulSet is both Raft controller AND broker. Combined mode is the chart's default + simplest topology. No CRDs (Bitnami's chart doesn't ship any; Strimzi does). | Bitnami-specific |
| 11 | Smoke client | **`kubectl exec` into the broker pod.** The bitnami/kafka image ships `kafka-console-producer.sh` / `kafka-console-consumer.sh` / `kafka-topics.sh` at `/opt/bitnami/kafka/bin/`. Spinning up a dedicated client pod adds another image pull + wait without changing what we're testing. | Pragmatic |
| 12 | Smoke assertion | Create a topic, produce a unique payload via `kafka-console-producer.sh`, consume from offset 0 via `kafka-console-consumer.sh --max-messages 1`, assert the consumed line equals the produced one. Proves KRaft metadata controller + broker log append + topic auto-resolution + offset-zero seek + fetch protocol path — **not** "kafka pod is up." | Per the family's "exercise the functionality" rule |
| 13 | Cross-rule deps | `rules_kubectl` public dep. `rules_helm` + `rules_kind` + `rules_itest` + `rules_shell` dev-only. No cert-manager required (PLAINTEXT smoke; SASL/TLS smokes are deferred). | rules_loki precedent |
| 14 | Naming | snake_case rules/macros, `MixedCaseInfo` providers (none in v0.1), `UPPER_SNAKE` constants. | All siblings |
| 15 | Update workflow | `bash tools/render_kafka.sh <chart-version>` → thin shim. | rules_loki precedent |

## Per-test broker (v0.2.0)

Built for rules_openmeter (collider-bazel-extensions/.github#7): OpenMeter's
ingest API produces to Kafka and its sink worker consumes with
confluent-kafka-go (librdkafka), so a test needs a real broker next to
`pg_server`, `valkey_server` and ClickHouse.

| # | Decision | Choice | Why |
|---|---|---|---|
| 16 | Shape | `kafka_server` (long-running executable, writes `$TEST_TMPDIR/<name>.env` once ready), `kafka_server_health_check` (env file exists), `kafka_test` (sh_test macro + trampoline). Non-breaking: `kafka_install` / `kafka_health_check` unchanged. | rules_valkey's `valkey_server` / `valkey_health_check` / `valkey_test`. `kafka_health_check` was taken, hence `kafka_server_health_check`. |
| 17 | Software | The Apache binary release `kafka_2.13-<ver>.tgz`, latest 4.x (4.4.0). `urls` = downloads.apache.org, then archive.apache.org (old releases leave downloads). | Upstream bits, not a container image or a distro build. 4.x: KRaft-only, no ZooKeeper. |
| 18 | Pin | sha512 hex from the published `.sha512` (Apache publishes no sha256), converted in Starlark to an SRI `integrity` for `download_and_extract`. `tools/update_versions.sh [--check]` regenerates `private/versions.bzl` and verifies the archive hashes to it. | Verify against what Apache publishes, with no host `sha512sum` in the fetch. A unit test pins the conversion. |
| 19 | Version selection | `kafka.version(version, sha512, urls, scala_version, strip_prefix)` in `//:extensions.bzl`; the root's tag wins, else rules_kafka's own, else `DEFAULT_VERSION`. One repo, `@kafka_dist`. Unpinned versions need `sha512`. | rules_zap / rules_argocd. One Kafka per build keeps runfiles and the ready helper simple. |
| 20 | JVM | rules_java's remote JDK 21 (`@remotejdk21_<platform>//:jdk`), named directly by a `select` alias in `//private:jdk`, not resolved as a Java toolchain. | rules_zap's approach: no host Java, no `--java_runtime_version`. Kafka 4.x brokers need Java 17+; 21 is supported and is the newest LTS rules_java pins at 9.6.1. |
| 21 | Laziness | The extension and the JDK repos are only fetched when a `kafka_server` is built. `defs.bzl` loads `@rules_java` (a source repo) and `@rules_shell`, not the JDK. CI's consumer job builds a `kafka_install`-only consumer and fails if `kafka_dist` or a `remotejdk*` repo was fetched. | A kind-only consumer shouldn't download 135 MB of Kafka and a JDK. |
| 22 | Launcher | `private/launcher.py` (host `python3`, as rules_valkey). The wrapper resolves runfiles into variables, then execs it. | Free ports, signals, process groups and timeouts are clearer in Python than bash. |
| 23 | Topology | Combined `process.roles=broker,controller`, `node.id=1`, `controller.quorum.bootstrap.servers` + `format --standalone` (a one-voter dynamic quorum, KIP-853). | Kafka 4.x's own `config/server.properties` shape. |
| 24 | Listeners | `PLAINTEXT://127.0.0.1:<free>` and `CONTROLLER://127.0.0.1:<free>`; `advertised.listeners` = the bound ones. No other listeners, no SASL/TLS. | A client must reach exactly what it bootstraps from. Loopback only. |
| 25 | Cluster id | 16 random bytes, URL-safe base64, no padding, not starting with `-` — what `kafka-storage.sh random-uuid` prints — generated in the launcher. | Saves a JVM start (~1-3 s) per broker. |
| 26 | Single-node settings | Replication 1 everywhere (`default.replication.factor`, `min.insync.replicas`, offsets, transaction state + min ISR, share coordinator), `group.initial.rebalance.delay.ms=0`, and 1 partition (not 50) for `__consumer_offsets`, `__transaction_state`, `__share_group_state`. Fewer network/IO/background threads. | Testcontainers' Kafka does the same for offsets; a test's first consumer group is ready sooner. |
| 27 | Topics | `auto_create_topics` (default on). `topics = ["name", "name:N"]` are created before the env file appears; analysis rejects bad names (Kafka's `[a-zA-Z0-9._-]{1,249}`, not `.`/`..`), partitions < 1, duplicates. | OpenMeter's sink worker subscribes to topics that may not have been produced to yet. |
| 28 | Readiness | `KafkaReady.java` (compiled at build time with the pinned JDK's `javac` against `kafka-clients`, like rules_zap's driver): DescribeCluster lists a broker and a controller and ListTopics works; one CreateTopics request; then DescribeTopics until each topic has its partition count and a leader per partition. One JVM, not one `kafka-topics.sh` per topic. | Metadata answering is what clients need; per-topic CLI calls would cost a second each. A topic that already exists with another partition count fails. |
| 29 | Broker config overrides | `properties` (string dict) appended to `server.properties`; the listener/KRaft/path keys and `auto.create.topics.enable` are rejected at analysis. `heap` → `-Xmx` (default 512m), `jvm_flags` → `KAFKA_OPTS`. | Users can tune retention etc. without breaking the loopback/single-node guarantees. |
| 30 | JVM flags | `-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -Xshare:auto -XX:-UsePerfData -Djava.io.tmpdir=<state>/tmp`. `KAFKA_JMX_OPTS` set to a harmless flag (kafka-run-class.sh enables the JMX agent when it's empty; the `jmxremote` property's presence alone starts it). `EXTRA_ARGS="-name kafkaServer"` (no `-loggc`). Caller `KAFKA_*`, `CLASSPATH`, `JAVA_TOOL_OPTIONS`, `JMX_PORT`, `LOG_DIR` are dropped. | Start-up over throughput; no `/tmp/hsperfdata_<user>`; no JMX; a test's environment can't change the broker. `no_writes_test` fails if `-XX:-UsePerfData` is removed (checked by mutation). |
| 31 | Paths | `$TEST_TMPDIR/<name>/{config,data,logs,tmp}`; `LOG_DIR` set for every Kafka script (they `mkdir -p $LOG_DIR`, default `<kafka>/logs`, i.e. in runfiles). | Nothing outside `$TEST_TMPDIR`. `no_writes_test` checks open-for-write fds, hsperfdata, /tmp, the distribution and the JDK. |
| 32 | Process control | Every JVM (format, broker, ready helper) starts in its own session with `PR_SET_PDEATHSIG=SIGKILL` on Linux (Kafka's scripts `exec java`, which keeps it). Stop: SIGTERM by PID, wait up to 30 s, then SIGKILL to the process group. Never `pkill -f`. | rules_itest SIGKILLs a service's process group after `shutdown_timeout` (default 1 s) and Bazel's process-wrapper kills groups too; the JVM's own session keeps the shutdown ordered, PDEATHSIG covers a SIGKILLed launcher. |
| 33 | Failures | Format failure, broker exit, timeout → exit 1 with the tail (40 lines) of `server.log`, else the JVM's `kafka.out`. "Address already in use" → retry with new ports (3 attempts). | Name the cause; port races between choosing and binding are real under `--jobs`. |
| 34 | Shell helpers | `testlib.sh` over Kafka's CLI scripts (`kafka-console-producer/consumer`, `kafka-topics`) with the same hermetic env, using Kafka 4.x's `--command-property` / `--reader-property` / `--formatter-property`. Its path is in the env file (`KAFKA_TESTLIB`), so itest tests can use it too. | The task's "Kafka's own CLI scripts"; nothing to build. |
| 35 | Tests offline | Broker tests are tagged `block-network`; `.bazelrc` sets `--sandbox_default_allow_network=false` for tests; `network_blocked_test` proves the sandbox is offline; CI relaxes Ubuntu 24.04's AppArmor userns restriction so linux-sandbox (not a networked fallback) runs. | Proves "nothing beyond loopback", as rules_zap does. |

### Start-up time

Two JVMs before the env file: `kafka-storage.sh format` (2-5 s here), then
the broker plus the ready helper (5-10 s here). On this 16-core machine
under load average ~22, total 8-15 s. The launcher logs each phase.
Options not taken: formatting in the broker JVM (Kafka has no such mode),
AppCDS archives for Kafka's classpath (machine-specific, built at test
time), GraalVM native image (not in the Apache release).

## Bitnami vs. Strimzi

Two reasonable paths to install Kafka in Kubernetes; both work, they
just match different family shapes:

- **Bitnami chart** — plain Helm chart that emits a StatefulSet + Services
  directly. Matches the rules_loki / rules_grafana / rules_harbor pattern:
  one `kubectl_apply` + done. No CRDs, no operator.
- **Strimzi** — Kafka operator. Install the operator, then apply `Kafka`
  + `KafkaTopic` CRs, the operator reconciles them into pods. Matches the
  rules_cloudnativepg pattern: install + CR-driven smoke + CRD wait.

v0.1 picks Bitnami because:

1. It's structurally simpler — one StatefulSet, no operator pod, no CRDs
   to wait on, no admission webhook to configure.
2. The chart's defaults map cleanly to "smoke fixture" with a few
   overrides (single replica, PLAINTEXT, no persistence).
3. Strimzi's value-add (CRD-based topic/user management, multi-cluster
   federation, MirrorMaker, KafkaConnect) is orthogonal to "Kafka exists
   and accepts produce/consume."

A future `rules_kafka_strimzi` (or the same rule set with a `mode = ...`
toggle) would be a reasonable companion if a consumer specifically wants
the operator path. v0.1 keeps scope tight.

## KRaft vs. ZooKeeper

Apache Kafka 4.0 (the appVersion in chart 32.4.3) **removed ZooKeeper**.
KRaft (Kafka Raft) is the only supported metadata mode; chart's
`kraftVersion: 1` (dynamic quorum) is the default. No ZK pods, no ZK
StatefulSet, no ZK service to wait on. Single combined controller+broker
node forms a one-vote quorum — degenerate but functional for a smoke.

## Bitnami chart specifics — gotchas worth knowing

- **Default listeners are SASL_PLAINTEXT, not PLAINTEXT.** Chart values
  set `listeners.{client,controller,interbroker,external}.protocol:
  SASL_PLAINTEXT`, which means `sasl.interbroker.{user,password}` get
  random-generated and JAAS configs are emitted. That works in production
  but adds credential plumbing to a smoke. v0.1 explicitly overrides all
  four listener protocols to `PLAINTEXT`.
- **Combined mode vs. controller-only.** With `controller.controllerOnly:
  false` (the default), the `controller` StatefulSet runs combined
  controller+broker pods, and the `broker` StatefulSet has
  `replicaCount: 0` (rendered but empty). With `controllerOnly: true`,
  controllers are Raft-only and brokers run separately on the `broker`
  StatefulSet. v0.1 stays in combined mode — fewer pods, fewer Services,
  one wait target.
- **`controller.replicaCount: 3` is the chart default.** Three is the
  minimum for a real Raft quorum that survives a single-node failure;
  v0.1 sets it to 1 (degenerate, fine for tests).
- **Image is Bitnami-licensed.** `docker.io/bitnami/kafka:4.0.0-debian-12-r10`.
  Bitnami's August-2025 catalog reorganization moved many images to a
  `bitnamilegacy` namespace + a paid "Premium" tier; tags currently
  referenced by the chart still pull anonymously as of this writing,
  but it's worth tracking. A future bump may need to re-render to a
  legacy-namespace image or pivot to `apache/kafka`.
- **`auto.create.topics.enable`** — the chart leaves this at Kafka's
  default behavior; v0.1 explicitly enables it via `extraConfig` so the
  smoke's first produce can target a topic that doesn't exist yet
  (we still create the topic explicitly, but consumers shouldn't have
  to do that).

## v0.1.0 status

| Area | State |
|---|---|
| MODULE.bazel (Bzlmod-only) | done |
| `kafka_install` + `kafka_health_check` macros | done |
| Pinned Bitnami `kafka` chart 32.4.3 (rendered + committed) | done |
| `config/kafka-values.yaml` (exported) | done |
| Maintainer render flow (`bash tools/render_kafka.sh <ver>`) | done |
| Analysis test | done |
| Smoke (kind + Kafka + topic create + produce/consume round-trip) | done |
| `examples/` (producer/consumer pods, topic creation) | done |

## Release archive contents (v0.1.1)

`tests/` and `tools/` are `export-ignore` (`.gitattributes`): their packages load dev-only modules (rules_itest, rules_kind, rules_shell, rules_helm), which made `@rules_kafka//...` unloadable for consumers. `REPO.bazel` `ignore_directories` would also hide them from this repo's own `bazel test`, so it isn't used. A `git_override` clone still contains them. CI's `consumer` job (`.github/consumer/check.sh`) checks the commit's `git archive`: the directories are absent and `@rules_kafka//...` loads for a consumer.

## Deferred (not v0.2.0)

- Per-test broker: SASL/TLS listeners, multi-node clusters, a
  Schema Registry companion, Windows.


- **SASL/SCRAM smoke** — re-enable SASL listeners; smoke a credentialled
  connect (auto-generated password from chart secret). Different code
  path; v0.2 candidate.
- **TLS smoke** — mTLS listeners with chart-self-signed certs.
- **Multi-broker HA smoke** — `controller.replicaCount: 3` with
  `replication.factor: 3` on the topic; fault-inject one broker; assert
  produce/consume still works under partial failure.
- **Strimzi-based variant** — operator + `Kafka` / `KafkaTopic` CRDs.
  Different family, same goal.
- **Kafka Connect / Schema Registry / Cruise Control / MirrorMaker** —
  ecosystem components, each its own rule set candidate.
- **Tiered storage** — KRaft 4.0's tiered-storage feature (offload old
  segments to S3). Adds bucket fixture.
- **Multi-version chart support** — single pin in v0.1.
