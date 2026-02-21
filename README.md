# video-manager

## Summary

`video-manager` is a Java 21 microservice for asynchronous video processing over Kafka.

It receives requests from Kafka topics, downloads the source video from S3, runs local processing with FFmpeg, and publishes the result back to Kafka. It currently implements two main flows:

- `extract frame`: extract one frame from a video at a specific second.
- `video transcoding`: generate multiple transcoded outputs from predefined profiles.

The service follows a hexagonal architecture (`domain`, `application`, `infrastructure`) and includes startup recovery for jobs after unclean shutdowns.

## Main Technologies, Tools, and Concepts

### Core technologies

- Java 21
- Maven
- Apache Kafka (`kafka-clients`)
- FFmpeg
- MinIO (S3 compatible)
- Redis (job state and video refcount)
- Micrometer + Prometheus registry (metrics)
- Logback/SLF4J (logging)

### Testing stack

- JUnit 5
- AssertJ
- Mockito
- Awaitility
- ArchUnit (hexagonal architecture rules)
- Cucumber + Gherkin (ATDD)

### Applied concepts

- Hexagonal architecture
- Asynchronous messaging with Kafka
- Non-blocking processing with `CompletableFuture`
- Job idempotency and startup recovery
- Local workspace lifecycle with refcount-based cleanup
- Observability with health checks and metrics

## Run the Application Locally

### Requirements

- Java 21
- Maven 3.9+
- Docker + Docker Compose

### Docker environment

The `docker-compose.yml` stack includes:

- `app` (video-manager)
- `kafka`
- `redis`
- `minio`
- `minio-init` (creates initial bucket)

Start:

```bash
docker compose up -d --build
```

Stop:

```bash
docker compose down
```

Useful local endpoints:

- Kafka external broker: `localhost:19092`
- Redis: `localhost:6379`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001` (`minio-root-user` / `minio-root-password`)
- Observability: `http://localhost:8081`

### Start application (non-Docker local mode)

With defaults from `src/main/resources/application.properties`:

```bash
mvn -q exec:java -Dexec.mainClass=es.mblcu.videomanager.Application
```

Configuration resolution order:

1. Environment variable
2. Java system property (`-D...`)
3. `application.properties`
4. Embedded default

### Environment variables

- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `KAFKA_GROUP_ID` (default: `videomanager-extract-frame`)
- `KAFKA_TOPIC_EXTRACT_FRAME_REQUEST` (default: `fe_sample_frame_request__norm`)
- `KAFKA_TOPIC_EXTRACT_FRAME_DONE` (default: `fe_sample_frame_done__norm`)
- `KAFKA_GROUP_ID_TRANSCODE` (default: `videomanager-transcode`)
- `KAFKA_TOPIC_TRANSCODE_REQUEST` (default: `fe_transcode_request__norm`)
- `KAFKA_TOPIC_TRANSCODE_DONE` (default: `fe_transcode_done__norm`)
- `KAFKA_AUTO_OFFSET_RESET` (default: `latest`)
- `KAFKA_POLL_MILLIS` (default: `1000`)
- `REDIS_URI` (default: `redis://localhost:6379`)
- `REDIS_KEY_PREFIX` (default: `vm`)
- `FFMPEG_BIN` (default: `ffmpeg`)
- `FFMPEG_TIMEOUT_SECONDS` (default: `60`)
- `LOCAL_WORKSPACE_DIR` (default: `.work`)
- `VIDEO_MANAGER_S3_BUCKET` (default: `bucket`)
- `VIDEO_MANAGER_S3_ENDPOINT` (default: `http://localhost:9000`)
- `VIDEO_MANAGER_S3_ACCESS_KEY` (default: `minio-root-user`)
- `VIDEO_MANAGER_S3_SECRET_KEY` (default: `minio-root-password`)
- `OBSERVABILITY_PORT` (default: `8081`)
- `OBSERVABILITY_BIND_ADDRESS` (default: `0.0.0.0`)

### Run tests

Unit tests:

```bash
mvn -q test
```

Acceptance tests (ATDD):

```bash
mvn -q -Patdd verify
```

## Feature: Extract Frame

### Theoretical flow

1. Consume request from `extract-frame` input topic.
2. Validate/map payload into `ExtractFrameCommand`.
3. Download source video from S3 into local workspace only if not already cached.
4. Extract frame with FFmpeg at the requested second.
5. Upload generated frame to S3.
6. Delete temporary local frame file.
7. Decrement source video refcount and delete local source when no active task uses it.
8. Publish output message with `status=success` or `status=ERROR`.

Expected request JSON:

```json
{
  "videoId": 12345,
  "videoS3Path": "s3://bucket/videos/sample.mp4",
  "frameS3Path": "s3://bucket/frames/sample.png",
  "second": 1.0
}
```

Legacy fields `input` and `outputFile` are also accepted.

Response JSON:

```json
{
  "videoId": 12345,
  "frameS3Path": "s3://bucket/frames/sample.png",
  "status": "success",
  "errorDescription": null
}
```

### Practical test steps

1. Upload a sample video to MinIO at `s3://bucket/videos/sample.mp4` (you can use MinIO web console).
2. Publish a request to `fe_sample_frame_request__norm`.
3. Consume responses from `fe_sample_frame_done__norm`.
4. Verify `frames/sample.png` exists in MinIO.

Consume responses:

```bash
docker exec -i video-manager-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fe_sample_frame_done__norm \
  --from-beginning
```

Publish request:

```bash
docker exec -i video-manager-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic fe_sample_frame_request__norm <<'EOF'
{"videoId":12345,"videoS3Path":"s3://bucket/videos/sample.mp4","frameS3Path":"s3://bucket/frames/sample.png","second":1.0}
EOF
```

## Feature: Video Transcoding

### Theoretical flow

1. Consume request from transcode input topic.
2. Validate requested source dimensions against supported input dimensions.
3. Resolve configured output profiles for that source size.
4. Download source video from S3 (reusing local cached file when available).
5. Run FFmpeg for each output profile.
6. Upload generated files to S3.
7. Cleanup temporary files and source file according to shared refcount.
8. Publish output message:
   - `success` with `outputs` map (`profile -> s3Path`)
   - `ERROR` with error description

Expected request JSON:

```json
{
  "videoId": 98765,
  "videoS3Path": "s3://bucket/videos/sample.mp4",
  "outputS3Prefix": "s3://bucket/transcoded/98765",
  "width": 1280,
  "height": 720
}
```

Response JSON:

```json
{
  "videoId": 98765,
  "outputs": {
    "854x480": "s3://bucket/transcoded/98765/854x480.mp4",
    "640x360": "s3://bucket/transcoded/98765/640x360.mp4"
  },
  "status": "success",
  "errorDescription": null
}
```

Example allowed dimensions/profiles are configured in `application.properties`:

- `transcode.allowed-dimensions`
- `transcode.profiles.<width>x<height>`

### Practical test steps

1. Upload source video to `s3://bucket/videos/sample.mp4`.
2. Publish request to `fe_transcode_request__norm`.
3. Consume responses from `fe_transcode_done__norm`.
4. Verify generated MP4 files in MinIO under the configured output prefix.

Consume responses:

```bash
docker exec -i video-manager-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fe_transcode_done__norm \
  --from-beginning
```

Publish request:

```bash
docker exec -i video-manager-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic fe_transcode_request__norm <<'EOF'
{"videoId":98765,"videoS3Path":"s3://bucket/videos/sample.mp4","outputS3Prefix":"s3://bucket/transcoded/98765","width":1280,"height":720}
EOF
```

## Observability

The service exposes a dedicated HTTP server for metrics and health probes.

### Observability technologies

- Micrometer (`micrometer-core`) for metric instrumentation
- Prometheus registry (`micrometer-registry-prometheus`) for scrape format export
- JDK built-in `HttpServer` (`com.sun.net.httpserver`) for `/metrics` and health probe endpoints

Configuration:

- `observability.port` (env `OBSERVABILITY_PORT`, default `8081`)
- `observability.bind-address` (env `OBSERVABILITY_BIND_ADDRESS`, default `0.0.0.0`)

Endpoints:

- `GET /metrics`
- `GET /health`
- `GET /health/live`
- `GET /health/ready`
- `GET /health/startup`

Quick checks:

```bash
curl -s http://localhost:8081/health
curl -s http://localhost:8081/metrics | grep videomanager_
```

Metric families and tags:

- `videomanager_health_status`
  - tags: `probe`
- `videomanager_kafka_messages_consumed_total`
  - tags: `flow`, `topic`, `result`
- `videomanager_kafka_messages_published_total`
  - tags: `flow`, `topic`, `status`
- `videomanager_jobs_total`
  - tags: `flow`, `status`
- `videomanager_startup_recovered_jobs_total`
  - tags: `flow`
- `videomanager_processing_duration`
  - tags: `flow`, `operation`, `status`
- `videomanager_external_call_duration`
  - tags: `component`, `operation`, `status`

Observability unit tests are available at:

- `src/test/java/es/mblcu/videomanager/infrastructure/observability/ObservabilityTest.java`
