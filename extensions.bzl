"""Bzlmod extension. One `version` tag: creates `@kafka_dist`, the Apache
Kafka binary release kafka_server runs.

    kafka = use_extension("@rules_kafka//:extensions.bzl", "kafka")
    kafka.version(version = "4.4.0")

The root module's tag wins; without one, rules_kafka's own applies, so a
consumer declares nothing unless it wants another version. A version not
in private/versions.bzl needs its own sha512 (the release's published
kafka_2.13-<version>.tgz.sha512), as rules_argocd's
argocd.version(sha256 = ...) does.

The archive (and the JDK) are fetched only when a kafka_server /
kafka_test target is built: a consumer that only uses kafka_install
never downloads either.
"""

load("//private:repositories.bzl", "kafka_dist_repository")
load("//private:versions.bzl", "DEFAULT_VERSION", "KAFKA_VERSIONS")

_URLS = [
    "https://downloads.apache.org/kafka/{version}/kafka_{scala}-{version}.tgz",
    "https://archive.apache.org/dist/kafka/{version}/kafka_{scala}-{version}.tgz",
]

_version_tag = tag_class(attrs = {
    "version": attr.string(mandatory = True),  # e.g. "4.4.0"
    # Pins for a version not in KAFKA_VERSIONS (or to replace a known
    # one's): the archive's sha512 (hex, as in its .sha512 file). urls
    # default to downloads.apache.org then archive.apache.org;
    # strip_prefix to kafka_<scala_version>-<version>.
    "sha512": attr.string(),
    "urls": attr.string_list(),
    "scala_version": attr.string(default = "2.13"),
    "strip_prefix": attr.string(),
})

def _pins(tag):
    return struct(
        version = tag.version,
        sha512 = tag.sha512,
        urls = list(tag.urls),
        scala_version = tag.scala_version,
        strip_prefix = tag.strip_prefix,
    )

def _one(mod):
    pins = [_pins(t) for t in mod.tags.version]
    for p in pins[1:]:
        if p != pins[0]:
            fail("rules_kafka: {} sets kafka.version() more than once, differently ({} and {}); set one.".format(
                "the root module" if mod.is_root else mod.name,
                pins[0].version,
                p.version,
            ))
    return pins[0] if pins else None

def resolve_pins(pins):
    """The repository attributes for one kafka.version() tag (or fail)."""
    known = KAFKA_VERSIONS.get(pins.version)
    if known == None and not pins.sha512:
        fail(("rules_kafka: kafka.version(version = \"{}\") is not pinned. Pinned: {}. " +
              "For another version, pin it yourself: kafka.version(version = ..., " +
              "sha512 = <kafka_2.13-<version>.tgz.sha512>).").format(
            pins.version,
            ", ".join(sorted(KAFKA_VERSIONS.keys())),
        ))
    default_urls = [u.format(version = pins.version, scala = pins.scala_version) for u in _URLS]
    if pins.sha512:
        sha512 = pins.sha512
        urls = pins.urls or default_urls
        prefix = pins.strip_prefix or "kafka_{}-{}".format(pins.scala_version, pins.version)
    else:
        sha512 = known["sha512"]
        urls = pins.urls or known["urls"]
        prefix = pins.strip_prefix or known["strip_prefix"]
    return struct(version = pins.version, urls = urls, sha512 = sha512, strip_prefix = prefix)

def _impl(mctx):
    root = None
    own = None
    for mod in mctx.modules:
        if mod.is_root:
            root = _one(mod)
        elif mod.name == "rules_kafka":
            own = _one(mod)

    # Only one module's tag applies: the root's, else rules_kafka's own
    # (other modules' tags are ignored). The repo has no version in its
    # name, so there is one Kafka per build.
    pins = root or own or _pins(struct(
        version = DEFAULT_VERSION,
        sha512 = "",
        urls = [],
        scala_version = "2.13",
        strip_prefix = "",
    ))
    r = resolve_pins(pins)
    kafka_dist_repository(
        name = "kafka_dist",
        version = r.version,
        urls = r.urls,
        sha512 = r.sha512,
        strip_prefix = r.strip_prefix,
    )
    return mctx.extension_metadata(reproducible = True)

kafka = module_extension(
    implementation = _impl,
    tag_classes = {"version": _version_tag},
)
