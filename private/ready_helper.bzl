"""kafka_ready_helper: compiles KafkaReady.java (kafka_server's readiness
step) with the pinned JDK's own javac against the distribution's
kafka-clients jar, so rules_kafka needs neither rules_java's compile
toolchain nor java_tools."""

load(":common.bzl", "JavaRuntimeInfo")
load(":distribution.bzl", "KafkaDistributionInfo")

def _kafka_ready_helper_impl(ctx):
    jdk = ctx.attr._jdk[JavaRuntimeInfo]
    kafka = ctx.attr.kafka[KafkaDistributionInfo]
    jar = ctx.actions.declare_file(ctx.label.name + ".jar")
    ctx.actions.run_shell(
        inputs = depset(ctx.files.srcs + kafka.clients, transitive = [jdk.files]),
        outputs = [jar],
        command = """\
set -euo pipefail
classes="$(mktemp -d)"
trap 'rm -rf "$classes"' EXIT
"$1/bin/javac" --release 17 -encoding UTF-8 -Xlint:-options -cp "$3" -d "$classes" "${@:4}"
"$1/bin/jar" --create --file "$2" --date=2000-01-01T00:00:00Z -C "$classes" .
""",
        arguments = [jdk.java_home, jar.path, ":".join([f.path for f in kafka.clients])] +
                    [f.path for f in ctx.files.srcs],
        mnemonic = "KafkaReadyHelper",
        progress_message = "Compiling the rules_kafka readiness helper %{label}",
    )
    return [DefaultInfo(files = depset([jar]))]

kafka_ready_helper = rule(
    implementation = _kafka_ready_helper_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".java"], mandatory = True),
        "kafka": attr.label(mandatory = True, providers = [KafkaDistributionInfo]),
        "_jdk": attr.label(
            default = "//private:jdk",
            cfg = "exec",
            providers = [JavaRuntimeInfo],
        ),
    },
)
