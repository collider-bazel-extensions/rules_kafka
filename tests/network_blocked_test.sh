#!/usr/bin/env bash
# Runs with the same tag as every broker test (block-network): outbound
# connections fail. (Loopback works: every test talks to its broker over
# it.) If this fails, the broker tests aren't proven offline.
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }
for target in 140.82.112.3/443 1.1.1.1/443 8.8.8.8/53; do
  if timeout 5 bash -c "exec 3<>/dev/tcp/$target" 2>/dev/null; then
    fail "connected to $target: the network isn't blocked"
  fi
  echo "ok: no route to $target"
done
if timeout 5 bash -c 'exec 3<>/dev/tcp/downloads.apache.org/443' 2>/dev/null; then
  fail "resolved and connected to downloads.apache.org"
fi
echo "ok: downloads.apache.org unreachable"
echo PASS
