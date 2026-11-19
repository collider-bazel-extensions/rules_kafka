"""kafka_test — sh_test macro with an auto-launched Kafka broker.

Each target gets its own kafka_server (own ports, own data dir under its
$TEST_TMPDIR), so tests are safe under `--jobs=N`.

The trampoline (kafka_test_trampoline.sh) starts the broker, waits for its
env file, sources it (KAFKA_BOOTSTRAP_SERVERS, KAFKA_HOST, KAFKA_PORT, ...,
KAFKA_TESTLIB) and runs the test with the target's args. It runs the test
rather than exec'ing it, so it can stop the broker when the test ends, and
it stops the test if the broker dies first.
"""

load("@rules_shell//shell:sh_binary.bzl", "sh_binary")
load("@rules_shell//shell:sh_test.bzl", "sh_test")
load(":server.bzl", "kafka_server")

def kafka_test(
        name,
        srcs,
        topics = None,
        auto_create_topics = True,
        heap = "512m",
        jvm_flags = None,
        properties = None,
        ready_timeout = 120,
        deps = None,
        data = None,
        args = None,
        size = "medium",
        timeout = None,
        tags = None,
        **kwargs):
    """sh_test against an auto-launched single-node Kafka broker.

    Args:
      name: target name.
      srcs: the test script (one file).
      topics: topics created before the test: "name" or "name:partitions".
      auto_create_topics: auto.create.topics.enable (default True).
      heap: the broker JVM's -Xmx (default "512m").
      jvm_flags: extra broker JVM options.
      properties: extra broker properties (dict).
      ready_timeout: seconds to wait for the broker.
      deps: forwarded to the inner sh_binary.
      data: runfiles for the test (on the inner sh_binary and the sh_test,
        so `args` may use $(rootpath) on them).
      args: arguments for the test script.
      size: sh_test size. Default "medium" (a JVM broker starts in seconds,
        more on a loaded machine).
      timeout: sh_test timeout.
      tags: tags for the sh_test (helper targets also get "manual").
      **kwargs: forwarded to sh_test (env, env_inherit, flaky, ...).
    """
    tags = tags or []
    server_name = name + "_kafka"
    kafka_server(
        name = server_name,
        topics = topics or [],
        auto_create_topics = auto_create_topics,
        heap = heap,
        jvm_flags = jvm_flags or [],
        properties = properties or {},
        ready_timeout = str(ready_timeout),
        tags = tags + ["manual"],
        testonly = True,
    )

    inner_name = name + "_inner"
    sh_binary(
        name = inner_name,
        srcs = srcs,
        deps = deps,
        data = data,
        tags = tags + ["manual"],
        testonly = True,
    )

    sh_test(
        name = name,
        srcs = [Label("//private:kafka_test_trampoline.sh")],
        args = [
            "$(rootpath :{})".format(server_name),
            "$(rootpath :{})".format(inner_name),
            server_name,
        ] + (args or []),
        data = [
            ":" + server_name,
            ":" + inner_name,
        ] + (data or []),
        size = size,
        timeout = timeout,
        tags = tags,
        **kwargs
    )
