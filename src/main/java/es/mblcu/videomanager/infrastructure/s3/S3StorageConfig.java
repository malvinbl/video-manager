package es.mblcu.videomanager.infrastructure.s3;

import es.mblcu.videomanager.infrastructure.config.AppProperties;

public record S3StorageConfig(
    String defaultBucket,
    String endpoint,
    String accessKey,
    String secretKey
) {

    public static S3StorageConfig fromEnvironment() {
        final var properties = AppProperties.load();
        return new S3StorageConfig(
            properties.get("VIDEO_MANAGER_S3_BUCKET", "storage.s3.bucket", "bucket"),
            properties.get("VIDEO_MANAGER_S3_ENDPOINT", "storage.s3.endpoint", "http://localhost:9000"),
            properties.get("VIDEO_MANAGER_S3_ACCESS_KEY", "storage.s3.access-key", "minio-root-user"),
            properties.get("VIDEO_MANAGER_S3_SECRET_KEY", "storage.s3.secret-key", "minio-root-password")
        );
    }

}
