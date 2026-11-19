#!/bin/sh
# The client against ElasticMQ in Docker; the output must match
# expected.txt. Needs docker.
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
slangc=${SLANGC:-slangc}
image=${ELASTICMQ_IMAGE:-softwaremill/elasticmq-native:1.6.12}
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/sqs-live.XXXXXX")
name=slang-sqs-live
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$run_dir"; }
trap cleanup EXIT HUP INT TERM

(cd "$run_dir" && "$slangc" "$repo/tests/live/main.sl" -o "$run_dir/live" > "$run_dir/build.log" 2>&1) ||
    { cat "$run_dir/build.log"; exit 1; }
docker run -d --name "$name" -p 19324:9324 "$image" >/dev/null
for _ in $(seq 1 60); do
    curl -s -o /dev/null http://localhost:19324/ && break
    sleep 0.5
done
"$run_dir/live" > "$run_dir/out.txt" 2>&1 || { cat "$run_dir/out.txt"; exit 1; }
if diff -u "$repo/tests/live/expected.txt" "$run_dir/out.txt"; then
    echo "PASS sqs live (ElasticMQ)"
else
    exit 1
fi
