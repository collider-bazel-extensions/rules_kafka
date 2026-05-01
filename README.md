# rules_kafka

Hermetic [Apache Kafka](https://kafka.apache.org/) install for Bazel
test compositions. Pure glue layer over
[`rules_kubectl`](https://github.com/collider-bazel-extensions/rules_kubectl) —
`kafka_install` is a macro emitting a `kubectl_apply` target pre-configured
with Kafka's pinned manifest and the right wait shape.

```python
load("@rules_kafka//:defs.bzl", "kafka_install", "kafka_health_check")

kafka_install(name = "kafka_install_bin")          # default ns: kafka
kafka_health_check(name = "kafka_health_bin")
```

That's the whole API. Kafka is a distributed log/streaming broker;
this rule set installs a single-node KRaft cluster (no ZooKeeper) ready
for produce/consume traffic via the standard Kafka wire protocol.

**Pinned versions:** Bitnami's `kafka` helm chart `32.4.3` (Apache
Kafka `4.0.0`). Smoke-fixture render — single combined controller+broker
pod, KRaft mode, PLAINTEXT listeners (no SASL/auth), emptyDir storage.
The values file is exported as `@rules_kafka//config:kafka-values.yaml`
for inspection / extension. Production deployments override every
listener/replica/auth/storage field.

**Supported platforms (v0.1):** any platform where rules_kubectl runs.
Validated on Linux x86\_64 in CI.

---

## Contents

- [Installation](#installation) (Bzlmod-only)
- [Quickstart](#quickstart)
- [Producing + consuming](#producing--consuming)
- [Macros](#macros)
- [Hermeticity exceptions](#hermeticity-exceptions)
- [Contributing](#contributing)

---

## Installation

```python
bazel_dep(name = "rules_kafka", version = "0.1.0")
```

Bzlmod-only. Transitively pulls in
[`rules_kubectl`](https://github.com/collider-bazel-extensions/rules_kubectl).

---

## Quickstart

```python
load("@rules_itest//:itest.bzl", "itest_service", "service_test")
load("@rules_kafka//:defs.bzl", "kafka_install", "kafka_health_check")
load("@rules_kind//:defs.bzl", "kind_cluster", "kind_health_check")
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

# 1. Cluster.
kind_cluster(name = "cluster", k8s_version = "1.32")
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
| Kafka manifest | Fully hermetic. URL + sha256 pinned in `tools/versions.bzl`; pre-rendered + committed. | Re-render via `bash tools/render_kafka.sh <ver>`. |
| `kubectl` | Inherited from `rules_kubectl`. | |
| Target cluster | Out of scope. | |
| Kafka container image | Pulled at runtime by the cluster's nodes. `docker.io/bitnami/kafka:4.0.0-debian-12-r10` from the chart. | Future: pre-load via `kind_cluster.images`. |

---

## Contributing

PRs welcome. Conventions match the sibling rule sets:

- New rules need an analysis test in `tests/analysis_tests.bzl`.
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
