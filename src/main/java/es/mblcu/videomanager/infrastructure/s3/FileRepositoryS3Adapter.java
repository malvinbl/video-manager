package es.mblcu.videomanager.infrastructure.s3;

import es.mblcu.videomanager.domain.FileRepository;
import es.mblcu.videomanager.infrastructure.observability.Observability;
import io.minio.DownloadObjectArgs;
import io.minio.MinioAsyncClient;
import io.minio.UploadObjectArgs;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class FileRepositoryS3Adapter implements FileRepository {

    private final MinioAsyncClient minioAsyncClient;
    private final String defaultBucket;

    public FileRepositoryS3Adapter(S3StorageConfig config) {
        this(config.defaultBucket(), buildClient(config));
    }

    FileRepositoryS3Adapter(String defaultBucket, MinioAsyncClient minioAsyncClient) {
        this.defaultBucket = defaultBucket;
        this.minioAsyncClient = minioAsyncClient;
    }

    @Override
    public CompletableFuture<Void> download(String sourceS3Path, Path localTargetFile) {
        final var startedAt = Instant.now();
        final var s3Path = S3Path.parse(sourceS3Path, defaultBucket);

        try {
            final var request = DownloadObjectArgs.builder()
                .bucket(s3Path.bucket())
                .object(s3Path.key())
                .filename(localTargetFile.toString())
                .build();

            return minioAsyncClient.downloadObject(request)
                .thenApply(result -> (Void) null)
                .whenComplete((ignored, throwable) -> recordStorageCallMetric("download", startedAt, throwable));
        } catch (Exception ex) {
            recordStorageCallMetric("download", startedAt, ex);
            return CompletableFuture.failedFuture(
                new IllegalStateException("Cannot download from object storage: " + sourceS3Path, ex)
            );
        }
    }

    @Override
    public CompletableFuture<Void> upload(Path localSourceFile, String targetS3Path) {
        final var startedAt = Instant.now();
        final var s3Path = S3Path.parse(targetS3Path, defaultBucket);

        try {
            final var request = UploadObjectArgs.builder()
                .bucket(s3Path.bucket())
                .object(s3Path.key())
                .filename(localSourceFile.toString())
                .build();

            return minioAsyncClient.uploadObject(request)
                .thenApply(result -> (Void) null)
                .whenComplete((ignored, throwable) -> recordStorageCallMetric("upload", startedAt, throwable));
        } catch (Exception ex) {
            recordStorageCallMetric("upload", startedAt, ex);
            return CompletableFuture.failedFuture(
                new IllegalStateException("Cannot prepare upload from local file: " + localSourceFile, ex)
            );
        }
    }

    private static MinioAsyncClient buildClient(S3StorageConfig config) {
        return MinioAsyncClient.builder()
            .endpoint(config.endpoint())
            .credentials(config.accessKey(), config.secretKey())
            .build();
    }

    private void recordStorageCallMetric(String operation, Instant startedAt, Throwable throwable) {
        String status = throwable == null ? "success" : "error";
        Observability.recordExternalCallDuration(
            "s3",
            operation,
            status,
            Duration.between(startedAt, Instant.now())
        );
    }

}
