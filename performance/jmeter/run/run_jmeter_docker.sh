#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <plan_path> <result_jtl> <report_dir|-> [jmeter args...]"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PLAN_PATH="$1"
RESULT_JTL="$2"
REPORT_DIR="$3"
shift 3

if [[ "${RESULT_JTL}" == /tests/* ]]; then
  HOST_RESULT_JTL="${ROOT_DIR}/${RESULT_JTL#/tests/}"
else
  HOST_RESULT_JTL="${RESULT_JTL}"
fi

if [[ "${REPORT_DIR}" == /tests/* ]]; then
  HOST_REPORT_DIR="${ROOT_DIR}/${REPORT_DIR#/tests/}"
else
  HOST_REPORT_DIR="${REPORT_DIR}"
fi

LIB_DIR="${ROOT_DIR}/performance/jmeter/lib"
mkdir -p "$(dirname "${HOST_RESULT_JTL}")"
rm -f "${HOST_RESULT_JTL}"
if [[ "${REPORT_DIR}" != "-" ]]; then
  rm -rf "${HOST_REPORT_DIR}"
  mkdir -p "${HOST_REPORT_DIR}"
fi
mkdir -p "${LIB_DIR}"

if ! ls "${LIB_DIR}"/kafka-clients-*.jar >/dev/null 2>&1; then
  "${ROOT_DIR}/performance/jmeter/run/prepare_jmeter_libs.sh"
fi

IMAGE="${JMETER_DOCKER_IMAGE:-justb4/jmeter:latest}"
NETWORK="${JMETER_DOCKER_NETWORK:-video-manager_default}"

NETWORK_ARGS=()
if docker network inspect "${NETWORK}" >/dev/null 2>&1; then
  NETWORK_ARGS=(--network "${NETWORK}")
fi

if [[ "${REPORT_DIR}" != "-" ]]; then
  docker run --rm \
    -v "${ROOT_DIR}:/tests" \
    "${NETWORK_ARGS[@]}" \
    --add-host host.docker.internal:host-gateway \
    "${IMAGE}" \
    -n \
    -t "${PLAN_PATH}" \
    -l "${RESULT_JTL}" \
    -e -o "${REPORT_DIR}" \
    -Jsearch_paths=/tests/performance/jmeter/lib \
    "$@"
else
  docker run --rm \
    -v "${ROOT_DIR}:/tests" \
    "${NETWORK_ARGS[@]}" \
    --add-host host.docker.internal:host-gateway \
    "${IMAGE}" \
    -n \
    -t "${PLAN_PATH}" \
    -l "${RESULT_JTL}" \
    -Jsearch_paths=/tests/performance/jmeter/lib \
    "$@"
fi
