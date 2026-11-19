"""The @kafka_dist repo: Apache Kafka's binary release (kafka_2.13-<ver>.tgz),
verified against its published sha512, exposed as a kafka_distribution
target `@kafka_dist//:kafka`.

Only bin/*.sh, config/ and libs/ are used; site-docs, licenses and the
Windows scripts stay out of runfiles.
"""

load(":integrity.bzl", "sha512_integrity")

_BUILD = """\
load("@rules_kafka//private:distribution.bzl", "kafka_distribution")

package(default_visibility = ["//visibility:public"])

kafka_distribution(
    name = "kafka",
    version = "{version}",
    server_start = "bin/kafka-server-start.sh",
    files = glob(["bin/*.sh", "config/**", "libs/**"]),
    clients = glob(["libs/kafka-clients-{version}.jar", "libs/slf4j-api-*.jar"]),
)
"""

def _kafka_dist_repository_impl(rctx):
    version = rctx.attr.version
    rctx.download_and_extract(
        url = rctx.attr.urls,
        integrity = sha512_integrity(rctx.attr.sha512),
        stripPrefix = rctx.attr.strip_prefix,
        type = "tar.gz",
    )
    for want in ["bin/kafka-server-start.sh", "bin/kafka-storage.sh", "libs/kafka-clients-{}.jar".format(version)]:
        if not rctx.path(want).exists:
            fail("rules_kafka: {} has no {} under {} (strip_prefix = \"{}\")".format(
                rctx.attr.urls[0],
                want,
                rctx.attr.strip_prefix,
                rctx.attr.strip_prefix,
            ))

    rctx.delete("site-docs")
    rctx.delete("bin/windows")
    rctx.file("BUILD.bazel", _BUILD.format(version = version))

kafka_dist_repository = repository_rule(
    implementation = _kafka_dist_repository_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "urls": attr.string_list(mandatory = True),
        "sha512": attr.string(mandatory = True),
        "strip_prefix": attr.string(mandatory = True),
    },
)
