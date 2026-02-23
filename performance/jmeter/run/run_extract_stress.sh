#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RESULTS_DIR="${ROOT_DIR}/performance/jmeter/results/extract"
mkdir -p "${RESULTS_DIR}"
REPORT_DIR_ARG="-"
if [[ "${GENERATE_HTML_REPORT:-false}" == "true" ]]; then
  REPORT_DIR_ARG="/tests/performance/jmeter/results/extract/report"
fi

"${ROOT_DIR}/performance/jmeter/run/run_jmeter_docker.sh" \
  "/tests/performance/jmeter/plans/extract-frame-stress.jmx" \
  "/tests/performance/jmeter/results/extract/result.jtl" \
  "${REPORT_DIR_ARG}" \
  -JscriptPath="/tests/performance/jmeter/scripts/kafka_producer.groovy" \
  -JbootstrapServers="${BOOTSTRAP_SERVERS:-kafka:9092}" \
  -JextractRequestTopic="${EXTRACT_REQUEST_TOPIC:-fe_sample_frame_request__norm}" \
  -JvideoS3Path="${VIDEO_S3_PATH:-s3://bucket/videos/sample.mp4}" \
  -Jthreads="${THREADS:-20}" \
  -Jramp="${RAMP_SECONDS:-30}" \
  -Jduration="${DURATION_SECONDS:-300}"

echo "Extract stress JTL: ${RESULTS_DIR}/result.jtl"

if [[ "${ASSERT_RESULTS:-true}" == "true" ]]; then
  "${ROOT_DIR}/performance/jmeter/run/assert_jmeter_results.sh" "${RESULTS_DIR}/result.jtl"
fi
if [[ "${GENERATE_HTML_REPORT:-false}" == "true" ]]; then
  echo "Extract stress report: ${RESULTS_DIR}/report/index.html"
fi
