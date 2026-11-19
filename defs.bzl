"""Public API for rules_kafka.

Per-test broker (test-time, no cluster):
  kafka_server              — a single-node KRaft broker on free 127.0.0.1
                              ports; writes $TEST_TMPDIR/<name>.env once ready.
  kafka_server_health_check — pairs with kafka_server for itest_service.
  kafka_test                — sh_test macro: auto-launched broker + shell
                              helpers over Kafka's CLI scripts.

Cluster install (kind / rules_kubectl):
  kafka_install / kafka_health_check — Bitnami's Kafka chart, pre-rendered.
"""

load("@rules_kubectl//:defs.bzl", "kubectl_apply", "kubectl_apply_health_check")
load("//private:distribution.bzl", _KafkaDistributionInfo = "KafkaDistributionInfo")
load(
    "//private:server.bzl",
    _KafkaServerInfo = "KafkaServerInfo",
    _kafka_server = "kafka_server",
    _kafka_server_health_check = "kafka_server_health_check",
)
load("//private:test.bzl", _kafka_test = "kafka_test")

kafka_server = _kafka_server
kafka_server_health_check = _kafka_server_health_check
kafka_test = _kafka_test
KafkaServerInfo = _KafkaServerInfo
KafkaDistributionInfo = _KafkaDistributionInfo

# Bitnami Kafka chart in KRaft + combined-mode (`controllerOnly: false`)
# renders one StatefulSet per release: `<release>-controller`. Pods in
# that set are both Raft controllers AND brokers, listening on 9092
# (client) / 9093 (controller) / 9094 (interbroker). Release name is
# pinned to `kafka` at maintainer-render time.
_KAFKA_DEPLOYS = []
_KAFKA_ROLLOUTS = [
    "sts/kafka-controller",
]
_KAFKA_CRD = ""  # Kafka doesn't ship CRDs (Strimzi does; this rule set wraps Bitnami's chart).

def kafka_install(
        name,
        namespace = "kafka",
        wait_timeout = "300s",
        **kwargs):
    """Apply the pinned Kafka manifest into `namespace` and block until
    the controller StatefulSet's pods are Ready before idling.

    Drops into `itest_service.exe`. Wait timeout 300s — the bitnami/kafka
    image is ~500MB and KRaft cluster formation adds another few seconds
    for the single node to elect itself leader.
    """
    extra_deploys = kwargs.pop("wait_for_deployments", [])
    extra_rollouts = kwargs.pop("wait_for_rollouts", [])
    kubectl_apply(
        name = name,
        manifests = ["@rules_kafka//private/manifests:kafka.yaml"],
        namespace = namespace,
        create_namespace = True,
        server_side = True,
        wait_for_deployments = list(_KAFKA_DEPLOYS) + list(extra_deploys),
        wait_for_rollouts = list(_KAFKA_ROLLOUTS) + list(extra_rollouts),
        wait_timeout = wait_timeout,
        **kwargs
    )

def kafka_health_check(
        name,
        namespace = "kafka",
        **kwargs):
    """Readiness probe paired with `kafka_install`."""
    extra_deploys = kwargs.pop("wait_for_deployments", [])
    extra_rollouts = kwargs.pop("wait_for_rollouts", [])
    kubectl_apply_health_check(
        name = name,
        namespace = namespace,
        wait_for_deployments = list(_KAFKA_DEPLOYS) + list(extra_deploys),
        wait_for_rollouts = list(_KAFKA_ROLLOUTS) + list(extra_rollouts),
        **kwargs
    )
