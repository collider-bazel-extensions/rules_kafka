package rules_kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * kafka_server's readiness step, one JVM for all of it:
 *
 * <pre>
 *   KafkaReady --bootstrap HOST:PORT --timeout-ms MS [--topic NAME[:PARTITIONS]]...
 * </pre>
 *
 * Waits until the broker answers metadata (DescribeCluster lists a broker and
 * a controller) and ListTopics succeeds, creates each --topic (with the
 * broker's default partition count when none is given), then waits until
 * every --topic is in the broker's metadata with the expected partition count
 * and a leader for each partition. Exit 0 when ready; 1 with a message
 * otherwise (timeout, or a topic that exists with another partition count).
 */
public final class KafkaReady {
  private static void log(String msg) {
    System.err.println("[kafka] " + msg);
    System.err.flush();
  }

  public static void main(String[] argv) throws Exception {
    String bootstrap = null;
    long timeoutMs = 60_000;
    Map<String, Integer> topics = new LinkedHashMap<>(); // 0 = broker default
    for (int i = 0; i < argv.length; i++) {
      switch (argv[i]) {
        case "--bootstrap":
          bootstrap = argv[++i];
          break;
        case "--timeout-ms":
          timeoutMs = Long.parseLong(argv[++i]);
          break;
        case "--topic":
          String t = argv[++i];
          int colon = t.lastIndexOf(':');
          if (colon < 0) {
            topics.put(t, 0);
          } else {
            topics.put(t.substring(0, colon), Integer.parseInt(t.substring(colon + 1)));
          }
          break;
        default:
          log("KafkaReady: unknown argument " + argv[i]);
          System.exit(2);
      }
    }
    if (bootstrap == null) {
      log("KafkaReady: --bootstrap is required");
      System.exit(2);
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    System.exit(run(bootstrap, deadline, topics));
  }

  private static long left(long deadline) {
    return Math.max(0, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
  }

  private static int run(String bootstrap, long deadline, Map<String, Integer> topics)
      throws InterruptedException {
    Properties p = new Properties();
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(AdminClientConfig.CLIENT_ID_CONFIG, "rules_kafka-ready");
    p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
    p.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "50");
    p.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, "50");
    p.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "250");
    // No client telemetry push (KIP-714); nothing leaves loopback anyway.
    p.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "false");
    try (Admin admin = Admin.create(p)) {
      String last = "no answer yet";
      // 1. The broker answers metadata.
      while (true) {
        try {
          DescribeClusterResult c = admin.describeCluster();
          java.util.Collection<Node> nodes = c.nodes().get(Math.max(1, left(deadline)), TimeUnit.MILLISECONDS);
          Node controller = c.controller().get(Math.max(1, left(deadline)), TimeUnit.MILLISECONDS);
          admin.listTopics().names().get(Math.max(1, left(deadline)), TimeUnit.MILLISECONDS);
          if (!nodes.isEmpty() && controller != null && !controller.isEmpty()) {
            break;
          }
          last = "no broker or controller in the cluster metadata yet";
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
          last = String.valueOf(e.getCause() != null ? e.getCause() : e);
        }
        if (left(deadline) == 0) {
          log("the broker did not answer metadata in time: " + last);
          return 1;
        }
        Thread.sleep(50);
      }
      if (topics.isEmpty()) {
        return 0;
      }

      // 2. Create the topics (one request).
      List<NewTopic> create = new ArrayList<>();
      for (Map.Entry<String, Integer> e : topics.entrySet()) {
        Optional<Integer> parts = e.getValue() > 0 ? Optional.of(e.getValue()) : Optional.empty();
        create.add(new NewTopic(e.getKey(), parts, Optional.empty()));
      }
      CreateTopicsResult r = admin.createTopics(create);
      Map<String, Integer> created = new HashMap<>();
      for (Map.Entry<String, Integer> e : topics.entrySet()) {
        try {
          r.values().get(e.getKey()).get(Math.max(1, left(deadline)), TimeUnit.MILLISECONDS);
          created.put(e.getKey(), r.numPartitions(e.getKey()).get());
        } catch (ExecutionException ex) {
          if (!(ex.getCause() instanceof TopicExistsException)) {
            log("creating topic " + e.getKey() + " failed: " + ex.getCause());
            return 1;
          }
        } catch (java.util.concurrent.TimeoutException ex) {
          log("creating topic " + e.getKey() + " timed out");
          return 1;
        }
      }

      // 3. Every topic is in the metadata, with its partition count and leaders.
      while (true) {
        try {
          Map<String, TopicDescription> d = admin.describeTopics(topics.keySet())
              .allTopicNames().get(Math.max(1, left(deadline)), TimeUnit.MILLISECONDS);
          String notYet = null;
          for (Map.Entry<String, Integer> e : topics.entrySet()) {
            TopicDescription td = d.get(e.getKey());
            int want = e.getValue() > 0 ? e.getValue() : created.getOrDefault(e.getKey(), 0);
            if (td == null) {
              notYet = e.getKey() + " not in the metadata";
              break;
            }
            if (want > 0 && td.partitions().size() != want) {
              if (!created.containsKey(e.getKey())) {
                log("topic " + e.getKey() + " already exists with " + td.partitions().size()
                    + " partitions, not " + want);
                return 1;
              }
              notYet = e.getKey() + " has " + td.partitions().size() + " of " + want + " partitions";
              break;
            }
            for (TopicPartitionInfo tp : td.partitions()) {
              if (tp.leader() == null || tp.leader().isEmpty()) {
                notYet = e.getKey() + "-" + tp.partition() + " has no leader";
                break;
              }
            }
            if (notYet != null) {
              break;
            }
          }
          if (notYet == null) {
            return 0;
          }
          last = notYet;
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
          last = String.valueOf(e.getCause() != null ? e.getCause() : e);
        }
        if (left(deadline) == 0) {
          log("the topics were not ready in time: " + last);
          return 1;
        }
        Thread.sleep(50);
      }
    }
  }
}
