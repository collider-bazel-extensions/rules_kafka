"""Shared helpers: runfiles paths, shell quoting, the Java runtime."""

load("@rules_java//java/common:java_common.bzl", "java_common")

JavaRuntimeInfo = java_common.JavaRuntimeInfo

def runfiles_path(ctx, short_path):
    """The path of a file (by short_path) relative to the runfiles root."""
    if short_path.startswith("../"):
        return short_path[3:]
    return ctx.workspace_name + "/" + short_path

def shell_quote(s):
    return "'" + s.replace("'", "'\\''") + "'"
