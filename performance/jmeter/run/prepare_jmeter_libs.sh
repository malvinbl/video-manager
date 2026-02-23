#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LIB_DIR="${ROOT_DIR}/performance/jmeter/lib"

mkdir -p "${LIB_DIR}"

cd "${ROOT_DIR}"
mvn -q -DskipTests dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory="${LIB_DIR}"

echo "Runtime jars copied to ${LIB_DIR}"
echo "Ensure these jars are available in JMeter classpath (e.g. copy to JMETER_HOME/lib)."
