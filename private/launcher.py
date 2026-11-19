#!/usr/bin/env python3
"""Starts a single-node KRaft Kafka broker for rules_kafka's kafka_server.

  launcher.py serve --name N --kafka-home DIR --java-home DIR
                    --ready-classpath CP --testlib FILE
                    --env-file PATH --state-dir DIR --version V
                    [--heap 512m] [--ready-timeout S] [--no-auto-create-topics]
                    [--topic NAME[:PARTITIONS]]... [--property K=V]...
                    [--jvm-flag=FLAG]...

Steps:
  1. Two free 127.0.0.1 ports (client listener, controller listener) and a
     random cluster id. server.properties under <state-dir>/config: KRaft
     combined broker+controller, node.id 1, listeners and advertised
     listener on 127.0.0.1 only, single-node replication settings,
     log.dirs under <state-dir>/data.
  2. `kafka-storage.sh format --standalone` (one-voter dynamic quorum).
  3. `kafka-server-start.sh` in its own session (process group), with
     LOG_DIR=<state-dir>/logs, java.io.tmpdir=<state-dir>/tmp, no JMX,
     no hsperfdata, on Linux PR_SET_PDEATHSIG=SIGKILL so the JVM dies with
     the launcher even if the launcher is SIGKILLed.
  4. The readiness helper (KafkaReady, one JVM): the broker answers
     metadata; the --topic topics are created and in the metadata with
     their partition counts and leaders.
  5. Writes the env file atomically (see write_env) and runs until SIGTERM /
     SIGINT: then SIGTERM to the broker JVM (Kafka's clean shutdown),
     SIGKILL to its process group after 30 s, removes the env file, exit 0.
     If the broker exits on its own, exits non-zero with server.log's tail.

A start that fails (format error, broker exit, port taken, timeout) prints
the tail of server.log (or the JVM's console output) and exits 1. A port
grabbed by someone else between choosing and binding is retried with new
ports (3 attempts).

Every path written is under --state-dir and --env-file's directory
($TEST_TMPDIR under bazel test).
"""

import argparse
import base64
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid

_ATTEMPTS = 3
_STOP_TIMEOUT = 30.0
_TAIL = 40


class Failed(Exception):
    """The broker could not be started (the message names the cause)."""


def _log(msg):
    print(f"[kafka] {msg}", file=sys.stderr, flush=True)


def _tail(path, n=_TAIL):
    try:
        with open(path, errors="replace") as f:
            lines = f.read().splitlines()
    except OSError:
        return None
    return "\n".join("  " + l for l in lines[-n:])


def _free_ports(n):
    socks = []
    try:
        for _ in range(n):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def random_cluster_id():
    """A Kafka Uuid string, as `kafka-storage.sh random-uuid` prints: 16
    random bytes, URL-safe base64 without padding, not starting with '-'
    (Uuid.randomUuid regenerates those)."""
    while True:
        s = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
        if not s.startswith("-"):
            return s


# Environment variables Kafka's scripts read; the caller's are dropped so
# a test's environment can't change the broker (or sneak in JMX, a debug
# port, another LOG_DIR or classpath).
_SCRIPT_ENV = ("CLASSPATH", "EXTRA_ARGS", "INCLUDE_TEST_JARS", "JAVA_DEBUG_OPTS",
               "JAVA_DEBUG_PORT", "JAVA_HOME", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS",
               "JMX_PORT", "LOG_DIR", "SCALA_BINARY_VERSION", "SCALA_VERSION",
               "_JAVA_OPTIONS")


def jvm_env(java_home, log_dir, tmp_dir, heap, opts=()):
    """The environment for one of Kafka's scripts: the pinned JDK, logs
    and java.io.tmpdir under the state dir, a small heap, SerialGC + C1
    (startup over throughput), no hsperfdata file (it would go to
    /tmp/hsperfdata_<user>), no JMX agent."""
    env = {k: v for k, v in os.environ.items()
           if k not in _SCRIPT_ENV and not k.startswith("KAFKA_")}
    env["JAVA_HOME"] = java_home
    env["LOG_DIR"] = log_dir
    env["KAFKA_HEAP_OPTS"] = f"-Xmx{heap}"
    env["KAFKA_JVM_PERFORMANCE_OPTS"] = " ".join([
        "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1", "-Xshare:auto",
        "-XX:-UsePerfData", "-Djava.awt.headless=true",
        f"-Djava.io.tmpdir={tmp_dir}",
    ])
    # kafka-run-class.sh enables the JMX agent unless KAFKA_JMX_OPTS is set
    # (non-empty; the jmxremote property's mere presence starts it), and
    # JMX_PORT (dropped above) would open a port.
    env["KAFKA_JMX_OPTS"] = "-Djava.awt.headless=true"
    env["KAFKA_OPTS"] = " ".join(opts)
    # kafka-server-start.sh's default adds -loggc (a GC log in LOG_DIR);
    # keep only the process name.
    env["EXTRA_ARGS"] = "-name kafkaServer"
    return env


def server_properties(args, port, cport, data_dir):
    p = {
        "process.roles": "broker,controller",
        "node.id": "1",
        "controller.quorum.bootstrap.servers": f"127.0.0.1:{cport}",
        "listeners": f"PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{cport}",
        "advertised.listeners": f"PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{cport}",
        "controller.listener.names": "CONTROLLER",
        "inter.broker.listener.name": "PLAINTEXT",
        "listener.security.protocol.map": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "log.dirs": data_dir,
        "auto.create.topics.enable": "false" if args.no_auto_create_topics else "true",
        # One node: every internal topic and default at replication 1.
        "num.partitions": "1",
        "default.replication.factor": "1",
        "min.insync.replicas": "1",
        "offsets.topic.replication.factor": "1",
        "transaction.state.log.replication.factor": "1",
        "transaction.state.log.min.isr": "1",
        "share.coordinator.state.topic.replication.factor": "1",
        "share.coordinator.state.topic.min.isr": "1",
        # Fewer internal partitions than production's 50: a test's first
        # consumer group / transaction / share group is ready sooner.
        "offsets.topic.num.partitions": "1",
        "transaction.state.log.num.partitions": "1",
        "share.coordinator.state.topic.num.partitions": "1",
        "group.initial.rebalance.delay.ms": "0",
        "num.network.threads": "2",
        "num.io.threads": "2",
        "background.threads": "2",
        "num.recovery.threads.per.data.dir": "1",
        "log.cleaner.threads": "1",
    }
    for kv in args.property:
        k, _, v = kv.partition("=")
        p[k] = v
    return "".join(f"{k}={v}\n" for k, v in p.items())


def _pdeathsig():
    """Linux: SIGKILL this child when the launcher dies (also by SIGKILL,
    which no handler sees). Kafka's scripts exec java, which keeps it."""
    if sys.platform.startswith("linux"):
        import ctypes
        try:
            ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL)  # PR_SET_PDEATHSIG
        except Exception:
            pass


def _kill_group(proc, sig):
    for target in ("pid", "group"):
        try:
            if target == "pid":
                os.kill(proc.pid, sig)
            else:
                os.killpg(proc.pid, sig)  # its own session: pgid == pid
        except (ProcessLookupError, PermissionError):
            pass


def stop(proc, timeout=_STOP_TIMEOUT):
    """SIGTERM the broker JVM (by PID; Kafka shuts down cleanly), then
    SIGKILL its process group if it is still running after `timeout`.
    Never by name/pattern."""
    if proc is None:
        return
    if proc.poll() is None:
        try:
            os.kill(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout)
        except subprocess.TimeoutExpired:
            _log(f"broker (pid {proc.pid}) still running {timeout:g}s after SIGTERM; SIGKILL")
    # Anything left in the group (normally nothing): SIGKILL by group.
    _kill_group(proc, signal.SIGKILL)
    try:
        proc.wait(5)
    except subprocess.TimeoutExpired:
        pass


def _explain(state_dir):
    for name in ("server.log", "kafka.out"):
        t = _tail(os.path.join(state_dir, "logs", name))
        if t:
            return f"last lines of {os.path.join(state_dir, 'logs', name)}:\n{t}"
    return "(no broker log)"


def _port_clash(state_dir):
    for name in ("server.log", "kafka.out"):
        try:
            with open(os.path.join(state_dir, "logs", name), errors="replace") as f:
                if "Address already in use" in f.read():
                    return True
        except OSError:
            pass
    return False


def _run(cmd, env, log_path):
    """Start a short-lived JVM (storage format / readiness helper), output
    to log_path, in its own session, dying with the launcher."""
    with open(log_path, "ab") as out:
        p = subprocess.Popen(cmd, env=env, stdin=subprocess.DEVNULL, stdout=out,
                             stderr=subprocess.STDOUT, start_new_session=True,
                             preexec_fn=_pdeathsig)
    return p


def start(args, procs, timings):
    """Format + start the broker + wait until ready. Returns (proc, port,
    cport, cluster_id). Raises Failed."""
    deadline = time.monotonic() + args.ready_timeout
    for attempt in range(1, _ATTEMPTS + 1):
        shutil.rmtree(args.state_dir, ignore_errors=True)
        dirs = {d: os.path.join(args.state_dir, d) for d in ("config", "data", "logs", "tmp")}
        for d in dirs.values():
            os.makedirs(d)
        port, cport = _free_ports(2)
        cluster_id = random_cluster_id()
        props = os.path.join(dirs["config"], "server.properties")
        with open(props, "w") as f:
            f.write(server_properties(args, port, cport, dirs["data"]))
        bin_dir = os.path.join(args.kafka_home, "bin")

        # Short-lived tool JVMs: a small heap.
        tool_env = jvm_env(args.java_home, dirs["logs"], dirs["tmp"], "256m")
        t0 = time.monotonic()
        fmt_log = os.path.join(dirs["logs"], "format.out")
        p = _run([os.path.join(bin_dir, "kafka-storage.sh"), "format", "--standalone",
                  "-t", cluster_id, "-c", props],
                 tool_env, fmt_log)
        procs.append(p)
        try:
            rc = p.wait(max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            raise Failed(f"kafka-storage.sh format did not finish within "
                         f"{args.ready_timeout:g}s:\n{_tail(fmt_log)}")
        finally:
            stop(p, 5)
            procs.remove(p)
        if rc != 0:
            raise Failed(f"kafka-storage.sh format failed (status {rc}):\n{_tail(fmt_log)}")
        timings["format"] = time.monotonic() - t0

        t1 = time.monotonic()
        env = jvm_env(args.java_home, dirs["logs"], dirs["tmp"], args.heap, args.jvm_flag)
        with open(os.path.join(dirs["logs"], "kafka.out"), "ab") as out:
            broker = subprocess.Popen(
                [os.path.join(bin_dir, "kafka-server-start.sh"), props],
                env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                start_new_session=True, preexec_fn=_pdeathsig)
        procs.append(broker)

        ready_log = os.path.join(dirs["logs"], "ready.out")
        java = os.path.join(args.java_home, "bin", "java")
        cmd = [java, "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1", "-Xshare:auto",
               "-XX:-UsePerfData", "-Xmx128m", f"-Djava.io.tmpdir={dirs['tmp']}",
               "-Dslf4j.internal.verbosity=ERROR",
               "-cp", args.ready_classpath, "rules_kafka.KafkaReady",
               "--bootstrap", f"127.0.0.1:{port}",
               "--timeout-ms", str(int(max(0.1, deadline - time.monotonic()) * 1000))]
        for t in args.topic:
            cmd += ["--topic", t]
        helper = _run(cmd, tool_env, ready_log)
        procs.append(helper)
        rc = None
        try:
            while True:
                rc = helper.poll()
                if rc is not None:
                    break
                if broker.poll() is not None:
                    break
                if time.monotonic() > deadline + 5:  # the helper has its own deadline
                    break
                time.sleep(0.05)
        finally:
            stop(helper, 2)
            procs.remove(helper)

        if broker.poll() is not None:
            brc = broker.returncode
            procs.remove(broker)
            if _port_clash(args.state_dir) and attempt < _ATTEMPTS:
                _log(f"port {port} or {cport} was taken before the broker bound it; retrying")
                continue
            raise Failed(f"the broker exited (status {brc}) before it was ready; "
                         + _explain(args.state_dir))
        if rc != 0:
            stop(broker)
            procs.remove(broker)
            why = _tail(ready_log) or ""
            raise Failed(f"the broker was not ready within {args.ready_timeout:g}s:\n{why}\n"
                         + _explain(args.state_dir))
        timings["broker"] = time.monotonic() - t1
        return broker, port, cport, cluster_id
    raise Failed("no free ports after retries")


def write_env(env_file, values):
    """Atomically: a test that sees the file sees all of it."""
    tmp = env_file + ".tmp"
    with open(tmp, "w") as f:
        for k, v in values.items():
            f.write(f"{k}={shlex.quote(str(v))}\n")
    os.replace(tmp, env_file)


def serve(args):
    procs = []
    stopping = []
    ready = []

    # Installed before the broker starts, so a signal during start-up
    # stops what has started.
    def _on_signal(signum, _frame):
        if stopping:
            return
        stopping.append(signum)
        _log(f"signal {signum}; stopping")
        if not ready:
            raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGHUP, _on_signal)

    def _stop_all():
        for p in list(procs):
            stop(p)

    timings = {}
    t0 = time.monotonic()
    try:
        broker, port, cport, cluster_id = start(args, procs, timings)
    except Failed as e:
        _stop_all()
        _log(str(e))
        return 1
    except SystemExit:
        _stop_all()
        raise
    total = time.monotonic() - t0

    values = {
        "KAFKA_BOOTSTRAP_SERVERS": f"127.0.0.1:{port}",
        "KAFKA_HOST": "127.0.0.1",
        "KAFKA_PORT": port,
        "KAFKA_CONTROLLER_PORT": cport,
        "KAFKA_CLUSTER_ID": cluster_id,
        "KAFKA_TOPICS": ",".join(t.rpartition(":")[0] if ":" in t else t for t in args.topic),
        "KAFKA_VERSION": args.version,
        "KAFKA_PID": broker.pid,
        "KAFKA_STATE_DIR": args.state_dir,
        "KAFKA_LOG": os.path.join(args.state_dir, "logs", "server.log"),
        "KAFKA_HOME": args.kafka_home,
        "KAFKA_JAVA_HOME": args.java_home,
        "KAFKA_TESTLIB": args.testlib,
    }
    write_env(args.env_file, values)
    ready.append(True)
    _log(f"Kafka {args.version} ready on 127.0.0.1:{port} (controller {cport}) in {total:.1f}s "
         f"(format {timings['format']:.1f}s, broker + readiness {timings['broker']:.1f}s"
         + (f", {len(args.topic)} topic(s)" if args.topic else "")
         + f"); env file {args.env_file}")

    try:
        while not stopping and broker.poll() is None:
            time.sleep(0.1)
    finally:
        crashed = not stopping and broker.poll() is not None
        rc = broker.returncode
        t = time.monotonic()
        _stop_all()
        try:
            os.remove(args.env_file)
        except FileNotFoundError:
            pass
    if not crashed:
        _log(f"broker stopped in {time.monotonic() - t:.1f}s")
        return 0
    _log(f"the broker exited (status {rc}) while serving; " + _explain(args.state_dir))
    return rc if rc and rc > 0 else 1


def main(argv):
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="mode", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--name", required=True)
    s.add_argument("--kafka-home", required=True)
    s.add_argument("--java-home", required=True)
    s.add_argument("--ready-classpath", required=True)
    s.add_argument("--testlib", required=True)
    s.add_argument("--env-file", required=True)
    s.add_argument("--state-dir", required=True)
    s.add_argument("--version", required=True)
    s.add_argument("--heap", default="512m")
    s.add_argument("--ready-timeout", type=float, default=120.0)
    s.add_argument("--no-auto-create-topics", action="store_true")
    s.add_argument("--topic", action="append", default=[])
    s.add_argument("--property", action="append", default=[])
    s.add_argument("--jvm-flag", action="append", default=[])
    args = p.parse_args(argv)
    if not 0 < args.ready_timeout < float("inf"):
        _log(f"--ready-timeout must be a positive number of seconds, got {args.ready_timeout}")
        return 1
    for a in ("kafka_home", "java_home", "testlib", "env_file", "state_dir"):
        setattr(args, a, os.path.abspath(getattr(args, a)))
    args.ready_classpath = ":".join(os.path.abspath(c) for c in args.ready_classpath.split(":"))
    return serve(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
