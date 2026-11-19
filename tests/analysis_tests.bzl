"""Analysis-time tests: kafka_install (unchanged API), kafka_server /
kafka_server_health_check / kafka_test (provider, executables, runfiles,
launcher arguments) and the analysis errors for bad attributes, plus the
sha512 -> SRI conversion the Kafka download is verified with."""

load("@bazel_skylib//lib:unittest.bzl", "analysistest", "asserts", "unittest")
load("@rules_kafka//:defs.bzl", "KafkaServerInfo", "kafka_install", "kafka_server", "kafka_server_health_check", "kafka_test")
load("//private:integrity.bzl", "sha512_integrity")
load("//private:versions.bzl", "DEFAULT_VERSION")

# ---- kafka_install (v0.1 API) ------------------------------------------------

def _has_executable_impl(ctx):
    env = analysistest.begin(ctx)
    target = analysistest.target_under_test(env)
    asserts.true(
        env,
        target[DefaultInfo].files_to_run.executable != None,
        "expected target to expose an executable",
    )
    return analysistest.end(env)

_has_executable_test = analysistest.make(_has_executable_impl)

def kafka_install_test_suite(name):
    kafka_install(
        name = name + "_subject",
        tags = ["manual"],
    )
    _has_executable_test(name = name + "_executable", target_under_test = ":" + name + "_subject")
    native.test_suite(name = name, tests = [":" + name + "_executable"])

# ---- kafka_server ----------------------------------------------------------------

def _server_impl(ctx):
    env = analysistest.begin(ctx)
    target = analysistest.target_under_test(env)
    info = target[KafkaServerInfo]
    asserts.equals(env, DEFAULT_VERSION, info.version)
    asserts.equals(env, {"events": 3, "audit": 0}, info.topics)
    asserts.equals(env, False, info.auto_create_topics)
    d = target[DefaultInfo]
    asserts.true(env, d.files_to_run.executable != None, "expected an executable")
    names = [f.basename for f in d.default_runfiles.files.to_list()]
    for want in [
        "java",
        "kafka-server-start.sh",
        "kafka-storage.sh",
        "kafka-run-class.sh",
        "kafka-console-producer.sh",
        "kafka-console-consumer.sh",
        "kafka-topics.sh",
        "kafka-clients-{}.jar".format(DEFAULT_VERSION),
        "kafka_ready.jar",
        "launcher.py",
        "testlib.sh",
    ]:
        asserts.true(env, want in names, "runfiles lack " + want)
    asserts.false(env, "kafka-server-start.bat" in names, "runfiles have the Windows scripts")

    # The launcher's arguments, in the generated wrapper.
    tmpl = [a for a in analysistest.target_actions(env) if a.mnemonic == "TemplateExpand"][0]
    opts = tmpl.substitutions["__OPTS__"]
    for want in [
        "'--heap' '1g'",
        "'--ready-timeout' '90'",
        "'--no-auto-create-topics'",
        "'--topic' 'events:3' '--topic' 'audit'",
        "'--property' 'log.retention.ms=60000'",
        "'--jvm-flag=-XX:+ExitOnOutOfMemoryError'",
    ]:
        asserts.true(env, want in opts, "launcher options lack " + want + ": " + opts)
    return analysistest.end(env)

_server_test = analysistest.make(_server_impl)

def _defaults_impl(ctx):
    env = analysistest.begin(ctx)
    target = analysistest.target_under_test(env)
    asserts.equals(env, {}, target[KafkaServerInfo].topics)
    asserts.equals(env, True, target[KafkaServerInfo].auto_create_topics)
    tmpl = [a for a in analysistest.target_actions(env) if a.mnemonic == "TemplateExpand"][0]
    opts = tmpl.substitutions["__OPTS__"]
    asserts.true(env, "'--heap' '512m'" in opts, opts)
    asserts.true(env, "'--ready-timeout' '120'" in opts, opts)
    asserts.false(env, "--no-auto-create-topics" in opts, opts)
    return analysistest.end(env)

_defaults_test = analysistest.make(_defaults_impl)

def _health_impl(ctx):
    env = analysistest.begin(ctx)
    target = analysistest.target_under_test(env)
    asserts.true(env, target[DefaultInfo].files_to_run.executable != None, "expected an executable")
    tmpl = [a for a in analysistest.target_actions(env) if a.mnemonic == "TemplateExpand"][0]
    asserts.equals(env, ctx.attr.expect_env, tmpl.substitutions["__ENV_FILE_NAME__"])
    return analysistest.end(env)

_health_test = analysistest.make(_health_impl, attrs = {"expect_env": attr.string()})

def _test_macro_impl(ctx):
    env = analysistest.begin(ctx)
    target = analysistest.target_under_test(env)
    names = [f.basename for f in target[DefaultInfo].default_runfiles.files.to_list()]
    for want in ["kafka_test_trampoline.sh", "java", "kafka-server-start.sh", "kafka_ready.jar"]:
        asserts.true(env, want in names, "kafka_test runfiles lack " + want)
    return analysistest.end(env)

_test_macro_test = analysistest.make(_test_macro_impl)

def _failure_impl(ctx):
    env = analysistest.begin(ctx)
    asserts.expect_failure(env, ctx.attr.expect)
    return analysistest.end(env)

_failure_test = analysistest.make(
    _failure_impl,
    attrs = {"expect": attr.string()},
    expect_failure = True,
)

# ---- sha512 -> SRI -----------------------------------------------------------------

def _integrity_impl(ctx):
    env = unittest.begin(ctx)

    # Expected values from Python's base64.b64encode(bytes.fromhex(h)).
    asserts.equals(
        env,
        "sha512-9c66iiZatrstaDYrH2hZqV4V1g75K6g7Q/3wZbVX3BjpjWODwVuZdzIOJbOiyLbxHeyxew+mwPFjQjeToOuVyw==",
        sha512_integrity("f5ceba8a265ab6bb2d68362b1f6859a95e15d60ef92ba83b43fdf065b557dc18e98d6383c15b9977320e25b3a2c8b6f11decb17b0fa6c0f163423793a0eb95cb"),
    )

    # Upper case and the .sha512 file's spacing are accepted.
    asserts.equals(
        env,
        "sha512-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/w==",
        sha512_integrity(" ".join(["00000000"] * 15 + ["000000FF"])),
    )
    return unittest.end(env)

_integrity_test = unittest.make(_integrity_impl)

def analysis_test_suite(name):
    """All analysis tests, as one test_suite."""
    tests = []

    kafka_install_test_suite(name = name + "_install")
    tests.append(":" + name + "_install")

    kafka_server(
        name = name + "_server",
        topics = ["events:3", "audit"],
        auto_create_topics = False,
        heap = "1g",
        jvm_flags = ["-XX:+ExitOnOutOfMemoryError"],
        properties = {"log.retention.ms": "60000"},
        ready_timeout = "90",
        tags = ["manual"],
    )
    _server_test(name = name + "_server_test", target_under_test = ":" + name + "_server")
    tests.append(":" + name + "_server_test")

    kafka_server(name = name + "_defaults", tags = ["manual"])
    _defaults_test(name = name + "_defaults_test", target_under_test = ":" + name + "_defaults")
    tests.append(":" + name + "_defaults_test")

    kafka_server_health_check(
        name = name + "_health",
        server = ":" + name + "_server",
        tags = ["manual"],
    )
    _health_test(
        name = name + "_health_test",
        target_under_test = ":" + name + "_health",
        expect_env = name + "_server.env",
    )
    tests.append(":" + name + "_health_test")

    kafka_test(
        name = name + "_macro",
        srcs = ["never_runs.sh"],
        tags = ["manual"],
    )
    _test_macro_test(name = name + "_macro_test", target_under_test = ":" + name + "_macro")
    tests.append(":" + name + "_macro_test")

    failures = {
        "bad_topic_name": ({"topics": ["no spaces"]}, "a topic name is 1-249 of [a-zA-Z0-9._-]"),
        "dot_topic": ({"topics": [".."]}, "a topic name is 1-249"),
        "bad_partitions": ({"topics": ["t:0"]}, "partitions must be a positive integer"),
        "empty_partitions": ({"topics": ["t:"]}, "partitions must be a positive integer"),
        "duplicate_topic": ({"topics": ["t", "t:2"]}, "is listed twice"),
        "protected_property": ({"properties": {"listeners": "PLAINTEXT://0.0.0.0:9092"}}, "\"listeners\" is set by kafka_server"),
        "auto_create_property": ({"properties": {"auto.create.topics.enable": "false"}}, "(use auto_create_topics)"),
        "bad_property_name": ({"properties": {"a=b": "c"}}, "is not a property name"),
        "bad_heap": ({"heap": "lots"}, "expected a JVM size"),
        "zero_heap": ({"heap": "0m"}, "expected a JVM size"),
        "bad_timeout": ({"ready_timeout": "soon"}, "ready_timeout = \"soon\": expected a positive number of seconds"),
        "zero_timeout": ({"ready_timeout": "0"}, "expected a positive number of seconds"),
        "bad_jvm_flag": ({"jvm_flags": ["Xmx1g"]}, "is not a JVM option"),
    }
    for case, (attrs, msg) in failures.items():
        kafka_server(name = name + "_" + case, tags = ["manual"], **attrs)
        _failure_test(
            name = name + "_" + case + "_test",
            target_under_test = ":" + name + "_" + case,
            expect = msg,
        )
        tests.append(":" + name + "_" + case + "_test")

    _integrity_test(name = name + "_integrity_test")
    tests.append(":" + name + "_integrity_test")

    native.test_suite(name = name, tests = tests)
