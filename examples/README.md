# Examples

Reference produce/consume shapes for Kafka. Apply them after `kafka_install`
has the broker running.

| File | Use case | Exercised by smoke? |
|---|---|---|
| [`producer_consumer_pods.yaml`](producer_consumer_pods.yaml) | Stand up dedicated producer + consumer pods that talk to the broker via the `kafka.kafka.svc.cluster.local:9092` Service. The closest analogue to "real workloads in another namespace produce/consume." | No (smoke uses `kubectl exec` into the broker pod for the same goal — fewer image pulls) |
| [`topic_create_job.yaml`](topic_create_job.yaml) | One-shot Job that runs `kafka-topics.sh --create`. Useful for declaratively pre-creating topics in a composition without baking it into the smoke. | No |
| [`exec_smoke.sh`](exec_smoke.sh) | The minimal `kubectl exec` produce/consume round-trip — same shape as `tests/smoke_test.sh` but extracted for reading. | Yes (the rule set's smoke uses this exact pattern) |

## Connecting from your own code

Inside the cluster, point your Kafka client at:

```
bootstrap.servers = kafka.kafka.svc.cluster.local:9092
security.protocol = PLAINTEXT
```

The release name is pinned to `kafka` (so the Service is `kafka` in
namespace `kafka`). If you change the namespace via `kafka_install(name = ..., namespace = "...")`,
the Service name stays `kafka` but the namespace in the DNS changes.

## Things this directory deliberately does NOT show

- **SASL/SCRAM authentication.** The v0.1 smoke render uses PLAINTEXT
  listeners. Real-world deployments will want SASL_SSL or mTLS; the
  chart supports both via `listeners.*.protocol`, but credential
  plumbing (JAAS, secret refs) is out of scope for v0.1 examples.
- **TLS.** Same reasoning — chart supports it, examples haven't been
  exercised yet.
- **Multi-broker / replication-factor=3 topics.** The render is
  single-broker; topics here use `--replication-factor 1`. Bumping to 3
  on a single-broker cluster makes the create call hang.
- **Kafka Connect / Schema Registry / Cruise Control.** Different
  components; different rule sets when they exist.
