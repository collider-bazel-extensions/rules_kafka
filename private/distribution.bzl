"""kafka_distribution: the files of one Apache Kafka binary release (the
@kafka_dist repo's `:kafka` target)."""

KafkaDistributionInfo = provider(
    doc = "An Apache Kafka binary release.",
    fields = {
        "version": "string: the Kafka version, e.g. \"4.4.0\"",
        "server_start": "File: bin/kafka-server-start.sh (its directory's parent is KAFKA_HOME)",
        "files": "depset of File: bin/*.sh, config/**, libs/** (every runfiles tree that runs Kafka needs all of it)",
        "clients": "list of File: kafka-clients and slf4j-api jars (the admin helper's classpath)",
    },
)

def _kafka_distribution_impl(ctx):
    files = depset(ctx.files.files)
    return [
        DefaultInfo(files = files),
        KafkaDistributionInfo(
            version = ctx.attr.version,
            server_start = ctx.file.server_start,
            files = files,
            clients = ctx.files.clients,
        ),
    ]

kafka_distribution = rule(
    implementation = _kafka_distribution_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "server_start": attr.label(allow_single_file = True, mandatory = True),
        "files": attr.label_list(allow_files = True),
        "clients": attr.label_list(allow_files = [".jar"]),
    },
)
