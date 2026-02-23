# JMeter Stress Setup (Kafka-based)

This folder contains a concrete JMeter setup to stress-test `video-manager` through Kafka.

## Included assets

- `plans/extract-frame-stress.jmx`: load on extract-frame request topic.
- `plans/transcode-stress.jmx`: load on transcode request topic.
- `plans/mixed-stress.jmx`: combined load (extract + transcode).
- `scripts/kafka_producer.groovy`: Kafka producer logic used by JSR223 samplers.
- `run/run_extract_stress.sh`: run extract-frame stress.
- `run/run_transcode_stress.sh`: run transcode stress.
- `run/run_mixed_stress.sh`: run mixed stress.
- `run/prepare_jmeter_libs.sh`: downloads runtime jars and prepares JMeter classpath.

## Prerequisites

1. Local stack up (`video-manager`, Kafka, Redis, MinIO).
2. Docker available (`docker run`).
3. Source video already uploaded in MinIO:
   - `s3://bucket/videos/sample.mp4`

## Prepare runtime libs for JMeter

From repo root:

```bash
./performance/jmeter/run/prepare_jmeter_libs.sh
```

This copies runtime dependencies (including `kafka-clients`) to:

- `performance/jmeter/lib`

At runtime, the Docker scripts mount this folder into JMeter container classpath.

## Run stress tests (Docker)

By default scripts use Docker image:

- `justb4/jmeter:latest`

If you want another image, override `JMETER_DOCKER_IMAGE`.

### Extract-frame stress

```bash
./performance/jmeter/run/run_extract_stress.sh
```

### Transcode stress

```bash
./performance/jmeter/run/run_transcode_stress.sh
```

### Mixed stress

```bash
./performance/jmeter/run/run_mixed_stress.sh
```

By default scripts generate only JTL files under `performance/jmeter/results/...`.

To generate HTML report too:

```bash
GENERATE_HTML_REPORT=true ./performance/jmeter/run/run_extract_stress.sh
```

Result quality assertions are enabled by default (`ASSERT_RESULTS=true`).

Disable assertions:

```bash
ASSERT_RESULTS=false ./performance/jmeter/run/run_extract_stress.sh
```

Assertion thresholds (env vars):

- `MAX_ERROR_RATE` (default `0.01`)
- `MAX_P95_MS` (default `800`)
- `MIN_THROUGHPUT_RPS` (default `5`)
- `MIN_SAMPLES` (default `10`)

## Default test parameters

All plans support standard properties:

- `threads` (default `20`)
- `ramp` (default `30`)
- `duration` (default `300`)
- `bootstrapServers` (default `kafka:9092`)

Flow-specific properties:

- `extractRequestTopic` (default `fe_sample_frame_request__norm`)
- `transcodeRequestTopic` (default `fe_transcode_request__norm`)
- `videoS3Path` (default `s3://bucket/videos/sample.mp4`)

You can override any property with `-Jkey=value`.

## Example custom run

```bash
"./performance/jmeter/run/run_jmeter_docker.sh" \
  "/tests/performance/jmeter/plans/extract-frame-stress.jmx" \
  "/tests/performance/jmeter/results/extract/custom.jtl" \
  "/tests/performance/jmeter/results/extract/custom-report" \
  -JscriptPath="/tests/performance/jmeter/scripts/kafka_producer.groovy" \
  -Jthreads=50 \
  -Jramp=60 \
  -Jduration=600 \
  -JbootstrapServers=kafka:9092 \
  -JvideoS3Path=s3://bucket/videos/sample.mp4
```

To force a custom image:

```bash
JMETER_DOCKER_IMAGE=my-jmeter-image:tag ./performance/jmeter/run/run_extract_stress.sh
```

Docker network override (if your Kafka is in another network):

```bash
JMETER_DOCKER_NETWORK=video-manager_default ./performance/jmeter/run/run_extract_stress.sh
```

## What to watch while stressing

In Grafana (`Video Manager Overview`) track:

- `Kafka Consumed Rate by flow/result`
- `Error Ratio by Operation`
- `Processing Latency (p50/p95)`
- `External Calls Latency p95`
- `Jobs Processed`

## Suggested acceptance criteria

- Error ratio (`extract_frame`, `transcode_video`) under 1-2% for happy-path load.
- Stable p95 without uncontrolled growth over time.
- No sustained DOWN states in liveness/readiness/startup.
- Kafka publish/consume rates converge (no large backlog trend).
