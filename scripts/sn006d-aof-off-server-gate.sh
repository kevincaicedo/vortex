#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT/vortex-benchmark/manifests/examples/shared-nothing-aof-off-server-compare.yaml"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
OUTPUT_ROOT="${1:-$ROOT/.artifacts/benchmarks/shared-nothing-sn006d-server-compare-$STAMP}"

mkdir -p "$OUTPUT_ROOT"

for threads in 1 2 4 8; do
    port_base=$((20679 + threads * 10))
    output_dir="$OUTPUT_ROOT/service-threads-$threads"
    bash "$ROOT/vortex-benchmark/bin/vortex_bench_local" \
        --manifest "$MANIFEST" \
        --output-dir "$output_dir" \
        --report-title "SN-006D AOF-Off Server Comparison, ${threads} service thread(s)" \
        --label "sn006d-aof-off-t${threads}" \
        --threads "$threads" \
        --port-base "$port_base"
done

cat > "$OUTPUT_ROOT/README.md" <<EOF
# SN-006D AOF-Off Server Comparison

Generated at: $STAMP UTC

Manifest: \`$MANIFEST\`

Rows:

- service-threads-1
- service-threads-2
- service-threads-4
- service-threads-8

Each row starts two native Vortex services:

- \`vortex-shared-keyspace\`: \`--engine-topology shared-keyspace\`
- \`vortex-shared-nothing\`: \`--engine-topology shared-nothing\`

Open each \`service-threads-*/reports/latest/report.md\` for normalized
throughput and latency summaries. Raw summaries are under
\`service-threads-*/results/*-summary.json\`.
EOF

echo "SN-006D output root: $OUTPUT_ROOT"
