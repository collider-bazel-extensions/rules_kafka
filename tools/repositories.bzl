"""Maintainer-only chart fetch."""

load("//tools:versions.bzl", "KAFKA_CHART_VERSIONS")

_BUILD = """\
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "files",
    srcs = glob(["**/*"]),
)
"""

def _impl(rctx):
    version = rctx.attr.version
    if version not in KAFKA_CHART_VERSIONS:
        fail("rules_kafka: unknown chart version '{}'. Known: {}".format(
            version, sorted(KAFKA_CHART_VERSIONS.keys()),
        ))
    pin = KAFKA_CHART_VERSIONS[version]
    rctx.download_and_extract(
        url    = pin["chart_url"],
        sha256 = pin["chart_sha256"],
    )
    rctx.file("WORKSPACE", "workspace(name = \"{}\")\n".format(rctx.name))
    rctx.file("BUILD.bazel", _BUILD)

kafka_chart_repository = repository_rule(
    implementation = _impl,
    attrs = {
        "version": attr.string(mandatory = True),
    },
)
