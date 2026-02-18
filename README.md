# video-manager

## Feature: extract-frame

Implemented with a hexagonal architecture:

- `domain`: entities/value objects, domain ports, domain exceptions
- `application`: use case orchestration
- `infrastructure`: adapters (Kafka input, FFmpeg output)

Kafka flow:

1. Consumer reads topic `KAFKA_TOPIC_EXTRACT_FRAME_REQUEST` (default: `fe_sample_frame_request__norm`)
2. Message payload is mapped to `ExtractFrameCommand`
3. Video is downloaded from S3 to local workspace only if not already cached
4. Frame is generated locally through FFmpeg
5. Frame is uploaded to S3 and local frame file is deleted
6. Local video is deleted when no active task is using it
7. A response is published to `KAFKA_TOPIC_EXTRACT_FRAME_DONE` (default: `fe_sample_frame_done__norm`)

Expected JSON payload:

```json
{
  "videoId": 12345,
  "videoS3Path": "s3://bucket/videos/sample.mp4",
  "frameS3Path": "s3://bucket/frames/sample.png",
  "second": 1.0
}
```

Legacy fields `input` and `outputFile` are still accepted for compatibility.

Response payload:

```json
{
  "videoId": 12345,
  "frameS3Path": "s3://bucket/frames/sample.png",
  "status": "success",
  "errorDescription": null
}
```

If processing fails:

```json
{
  "videoId": 12345,
  "frameS3Path": "s3://bucket/frames/sample.png",
  "status": "ERROR",
  "errorDescription": "detailed error message"
}
```

Environment variables:

- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `KAFKA_GROUP_ID` (default: `videomanager-extract-frame`)
- `KAFKA_TOPIC_EXTRACT_FRAME_REQUEST` (default: `fe_sample_frame_request__norm`)
- `KAFKA_TOPIC_EXTRACT_FRAME_DONE` (default: `fe_sample_frame_done__norm`)
- `KAFKA_AUTO_OFFSET_RESET` (default: `latest`)
- `KAFKA_POLL_MILLIS` (default: `1000`)
- `FFMPEG_BIN` (default: `ffmpeg`)
- `FFMPEG_TIMEOUT_SECONDS` (default: `60`)
- `LOCAL_WORKSPACE_DIR` (default: `.videomanager-work`)
- `VIDEO_MANAGER_S3_BUCKET` (default: `bucket`)
- `VIDEO_MANAGER_S3_ENDPOINT` (default: `http://localhost:9000`)
- `VIDEO_MANAGER_S3_ACCESS_KEY` (default: `minio-root-user`)
- `VIDEO_MANAGER_S3_SECRET_KEY` (default: `minio-root-password`)

Configuration file:

- `src/main/resources/application.properties`

Resolution order:

1. Environment variable
2. Java system property (`-D...`)
3. `application.properties`
4. Hardcoded default

Run service:

```bash
mvn -q exec:java -Dexec.mainClass=es.mblcu.videomanager.Application
```

## Local Docker Environment

Services included:

- `app` (video-manager)
- `kafka` (single-node KRaft)
- `redis`
- `minio` (S3-compatible)

Start:

```bash
docker compose up -d --build
```

Stop:

```bash
docker compose down
```

Run acceptance tests (Cucumber + Docker):

```bash
mvn -q -Patdd verify
```

Useful endpoints in local machine:

- Kafka: `localhost:19092`
- Redis: `localhost:6379`
- MinIO S3 API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001` (user: `minio-root-user`, password: `minio-root-password`)
