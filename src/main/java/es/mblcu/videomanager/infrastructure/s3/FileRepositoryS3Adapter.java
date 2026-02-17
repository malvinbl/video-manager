package es.mblcu.videomanager.infrastructure.s3;

import es.mblcu.videomanager.domain.FileRepository;
import io.minio.DownloadObjectArgs;
import io.minio.MinioAsyncClient;
import io.minio.UploadObjectArgs;

import java.nio.file.Path;
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
        final var s3Path = S3Path.parse(sourceS3Path, defaultBucket);

        try {
            final var request = DownloadObjectArgs.builder()
                .bucket(s3Path.bucket())
                .object(s3Path.key())
                .filename(localTargetFile.toString())
                .build();

            return minioAsyncClient.downloadObject(request)
                .thenApply(result -> null);
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Cannot download from object storage: " + sourceS3Path, ex)
            );
        }
    }

    @Override
    public CompletableFuture<Void> upload(Path localSourceFile, String targetS3Path) {
        final var s3Path = S3Path.parse(targetS3Path, defaultBucket);

        try {
            final var request = UploadObjectArgs.builder()
                .bucket(s3Path.bucket())
                .object(s3Path.key())
                .filename(localSourceFile.toString())
                .build();

            return minioAsyncClient.uploadObject(request)
                .thenApply(result -> null);
        } catch (Exception ex) {
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

}
