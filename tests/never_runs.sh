#!/usr/bin/env bash
# Must never run: the broker of the kafka_test using this fails to start.
echo "FAIL: the test body ran" >&2
exit 1
