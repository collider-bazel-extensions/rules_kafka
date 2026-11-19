"""kafka_server + kafka_server_health_check — a per-test Apache Kafka broker.

`kafka_server` is a long-running executable (see launcher.py): one
single-node KRaft broker+controller on two free 127.0.0.1 ports, its data
and logs in `$TEST_TMPDIR/<name>/`. Once the broker answers metadata and
the `topics` exist, it writes `$TEST_TMPDIR/<name>.env`:

    KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:<port>
    KAFKA_HOST, KAFKA_PORT, KAFKA_CONTROLLER_PORT, KAFKA_CLUSTER_ID,
    KAFKA_TOPICS, KAFKA_VERSION, KAFKA_PID, KAFKA_STATE_DIR, KAFKA_LOG,
    KAFKA_HOME, KAFKA_JAVA_HOME, KAFKA_TESTLIB

and runs until SIGTERM. `kafka_server_health_check` exits 0 iff that file
exists; it drops into `itest_service.health_check`.
"""

load(":common.bzl", "JavaRuntimeInfo", "runfiles_path", "shell_quote")
load(":distribution.bzl", "KafkaDistributionInfo")

KafkaServerInfo = provider(
    doc = "A kafka_server target.",
    fields = {
        "version": "string: the Kafka version it runs.",
        "topics": "dict: topic name -> partitions (0 = the broker default) created before it is ready.",
        "auto_create_topics": "bool: auto.create.topics.enable.",
    },
)

# Broker settings kafka_server owns: the single-node KRaft topology, the
# loopback listeners on free ports, and the paths under $TEST_TMPDIR.
PROTECTED_PROPERTIES = [
    "advertised.listeners",
    "auto.create.topics.enable",
    "controller.listener.names",
    "controller.quorum.bootstrap.servers",
    "controller.quorum.voters",
    "inter.broker.listener.name",
    "listener.security.protocol.map",
    "listeners",
    "log.dir",
    "log.dirs",
    "metadata.log.dir",
    "node.id",
    "process.roles",
]

_TOPIC_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"

def parse_topics(label, topics):
    """["name", "name:3"] -> {"name": 0, "name": 3}; fails on bad entries."""
    out = {}
    for t in topics:
        name, sep, parts = t.rpartition(":")
        if not sep:
            name, parts = t, ""
        if not name or len(name) > 249 or name in (".", "..") or [c for c in name.elems() if c not in _TOPIC_CHARS]:
            fail("{}: topics: {}: a topic name is 1-249 of [a-zA-Z0-9._-] (not \".\" or \"..\").".format(label, repr(t)))
        n = 0
        if sep:
            if not parts.isdigit() or int(parts) < 1:
                fail("{}: topics: {}: partitions must be a positive integer (\"name:3\").".format(label, repr(t)))
            n = int(parts)
        if name in out:
            fail("{}: topics: {} is listed twice.".format(label, repr(name)))
        out[name] = n
    return out

def check_seconds(label, attr, value):
    """Validate a seconds attribute: a positive number."""
    raw = value.strip()
    digits = raw.replace(".", "", 1)
    if not raw or not digits.isdigit() or not digits.replace("0", ""):
        fail("{}: {} = {}: expected a positive number of seconds.".format(label, attr, repr(value)))
    return raw

def check_heap(label, value):
    """Validate a heap size: digits plus an optional k/m/g (as -Xmx takes)."""
    v = value.strip()
    num = v[:-1] if v and v[-1] in "kKmMgG" else v
    if not num.isdigit() or not num.replace("0", ""):
        fail("{}: heap = {}: expected a JVM size like \"512m\" or \"1g\".".format(label, repr(value)))
    return v

def _kafka_server_impl(ctx):
    kafka = ctx.attr._kafka[KafkaDistributionInfo]
    jdk = ctx.attr._jdk[JavaRuntimeInfo]
    topics = parse_topics(ctx.label, ctx.attr.topics)
    for k in ctx.attr.properties:
        if k.strip() in PROTECTED_PROPERTIES:
            fail("{}: properties: {} is set by kafka_server{}.".format(
                ctx.label,
                repr(k),
                " (use auto_create_topics)" if k.strip() == "auto.create.topics.enable" else "",
            ))
        if "\n" in k or "=" in k or not k.strip():
            fail("{}: properties: {} is not a property name.".format(ctx.label, repr(k)))
    for v in ctx.attr.properties.values():
        if "\n" in v:
            fail("{}: properties: values must be one line.".format(ctx.label))
    for f in ctx.attr.jvm_flags:
        if not f.startswith("-"):
            fail("{}: jvm_flags: {} is not a JVM option (they start with \"-\").".format(ctx.label, repr(f)))

    opts = [
        "--heap",
        check_heap(ctx.label, ctx.attr.heap),
        "--ready-timeout",
        check_seconds(ctx.label, "ready_timeout", ctx.attr.ready_timeout),
        "--version",
        kafka.version,
    ]
    if not ctx.attr.auto_create_topics:
        opts.append("--no-auto-create-topics")
    for name in topics:
        opts += ["--topic", name + (":" + str(topics[name]) if topics[name] else "")]
    for k in sorted(ctx.attr.properties):
        opts += ["--property", "{}={}".format(k.strip(), ctx.attr.properties[k])]
    for f in ctx.attr.jvm_flags:
        opts.append("--jvm-flag=" + f)

    wrapper = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.expand_template(
        template = ctx.file._tmpl,
        output = wrapper,
        substitutions = {
            "__NAME__": ctx.label.name,
            "__LAUNCHER__": runfiles_path(ctx, ctx.file._launcher.short_path),
            "__KAFKA_START__": runfiles_path(ctx, kafka.server_start.short_path),
            "__JAVA__": runfiles_path(ctx, jdk.java_executable_runfiles_path),
            "__READY_JAR__": runfiles_path(ctx, ctx.file._ready.short_path),
            "__CLIENTS__": " ".join([shell_quote(runfiles_path(ctx, f.short_path)) for f in kafka.clients]),
            "__TESTLIB__": runfiles_path(ctx, ctx.file._testlib.short_path),
            "__OPTS__": " ".join([shell_quote(o) for o in opts]),
        },
        is_executable = True,
    )
    runfiles = ctx.runfiles(
        files = [wrapper, ctx.file._launcher, ctx.file._ready, ctx.file._testlib],
        transitive_files = depset(transitive = [kafka.files, jdk.files]),
    )
    return [
        DefaultInfo(executable = wrapper, runfiles = runfiles),
        KafkaServerInfo(
            version = kafka.version,
            topics = topics,
            auto_create_topics = ctx.attr.auto_create_topics,
        ),
    ]

kafka_server = rule(
    implementation = _kafka_server_impl,
    executable = True,
    doc = """\
A per-test Apache Kafka broker for tests and rules_itest compositions: one
single-node KRaft broker+controller on free 127.0.0.1 ports, storage
formatted with a random cluster id, data and logs under
`$TEST_TMPDIR/<name>/`. Writes `$TEST_TMPDIR/<name>.env` once the broker
answers metadata and the `topics` exist with their partition counts. If
the broker doesn't start, exits or isn't ready within `ready_timeout`, it
exits non-zero with the tail of server.log. SIGTERM / SIGINT stop the
broker (SIGTERM, then SIGKILL to its process group after 30 s), remove
the env file and exit 0.
""",
    attrs = {
        "topics": attr.string_list(
            doc = "Topics created before the broker counts as ready: " +
                  "\"name\" (the broker's default partition count, 1) or " +
                  "\"name:partitions\". Replication factor 1.",
        ),
        "auto_create_topics": attr.bool(
            default = True,
            doc = "auto.create.topics.enable: producing to (or fetching " +
                  "metadata for) a missing topic creates it.",
        ),
        "heap": attr.string(
            default = "512m",
            doc = "The broker JVM's maximum heap (-Xmx).",
        ),
        "jvm_flags": attr.string_list(
            doc = "Extra options for the broker JVM, after rules_kafka's own.",
        ),
        "properties": attr.string_dict(
            doc = "Extra broker properties (server.properties), overriding " +
                  "rules_kafka's single-node defaults, e.g. " +
                  "{\"log.retention.ms\": \"60000\"}. The listener, KRaft and " +
                  "path settings are kafka_server's own and can't be set.",
        ),
        "ready_timeout": attr.string(
            default = "120",
            doc = "Seconds to wait for the broker (and topics) to be ready.",
        ),
        "_kafka": attr.label(default = "//private:kafka", providers = [KafkaDistributionInfo]),
        "_jdk": attr.label(default = "//private:jdk", providers = [JavaRuntimeInfo]),
        "_ready": attr.label(default = "//private:kafka_ready", allow_single_file = [".jar"]),
        "_launcher": attr.label(default = "//private:launcher.py", allow_single_file = True),
        "_testlib": attr.label(default = "//private:testlib.sh", allow_single_file = True),
        "_tmpl": attr.label(default = "//private:server.sh.tmpl", allow_single_file = True),
    },
)

# ---- kafka_server_health_check -----------------------------------------------

def _kafka_server_health_check_impl(ctx):
    wrapper = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.expand_template(
        template = ctx.file._tmpl,
        output = wrapper,
        substitutions = {"__ENV_FILE_NAME__": ctx.attr.server.label.name + ".env"},
        is_executable = True,
    )
    return [DefaultInfo(executable = wrapper)]

kafka_server_health_check = rule(
    implementation = _kafka_server_health_check_impl,
    executable = True,
    doc = "Exits 0 iff the paired kafka_server has written its env file " +
          "(it does so only once the broker is ready). For " +
          "`itest_service.health_check`.",
    attrs = {
        "server": attr.label(
            mandatory = True,
            providers = [KafkaServerInfo],
            doc = "The kafka_server this check pairs with.",
        ),
        "_tmpl": attr.label(default = "//private:health_check.sh.tmpl", allow_single_file = True),
    },
)
