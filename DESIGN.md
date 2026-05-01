# rules_kafka — design decisions

Hermetic [Apache Kafka](https://kafka.apache.org/) install for Bazel
test compositions. Same shape as `rules_loki` / `rules_grafana` /
`rules_harbor` / `rules_external_secrets` — pure glue over `rules_kubectl`
+ `rules_helm`.

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

## Deferred (not v0.1.0)

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
