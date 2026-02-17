package es.mblcu.videomanager.infrastructure.s3;

import io.minio.DownloadObjectArgs;
import io.minio.MinioAsyncClient;
import io.minio.UploadObjectArgs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileRepositoryS3AdapterTest {

    @Mock
    private MinioAsyncClient minioAsyncClient;

    private FileRepositoryS3Adapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new FileRepositoryS3Adapter("video-bucket", minioAsyncClient);
    }

    @Test
    void shouldDownloadUsingBucketAndObjectFromS3Uri() throws Exception {
        when(minioAsyncClient.downloadObject(any(DownloadObjectArgs.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        assertDoesNotThrow(() -> adapter.download("s3://source-bucket/videos/input.mp4", Path.of("tmp/input.mp4")).join());

        final var requestCaptor = ArgumentCaptor.forClass(DownloadObjectArgs.class);
        verify(minioAsyncClient).downloadObject(requestCaptor.capture());

        final var request = requestCaptor.getValue();

        assertEquals("source-bucket", request.bucket());
        assertEquals("videos/input.mp4", request.object());
        assertEquals("tmp/input.mp4", request.filename());
    }

    @Test
    void shouldUploadUsingDefaultBucketWhenPathIsRelative() throws Exception {
        final var localSource = Files.createTempFile("frame-", ".png");

        when(minioAsyncClient.uploadObject(any(UploadObjectArgs.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        assertDoesNotThrow(() -> adapter.upload(localSource, "frames/output.png").join());

        final var requestCaptor = ArgumentCaptor.forClass(UploadObjectArgs.class);
        verify(minioAsyncClient).uploadObject(requestCaptor.capture());

        final var request = requestCaptor.getValue();

        assertEquals("video-bucket", request.bucket());
        assertEquals("frames/output.png", request.object());
        assertEquals(localSource.toString(), request.filename());
    }

    @Test
    void shouldWrapInvalidDownloadPath() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> adapter.download("s3://broken", Path.of("tmp/input.mp4"))
        );

        assertTrue(ex.getMessage().contains("Invalid S3 URI"));
    }

    @Test
    void shouldPropagateAsyncUploadFailure() throws Exception {
        final var rootCause = new RuntimeException("upload boom");
        final var localSource = Files.createTempFile("frame-", ".png");

        when(minioAsyncClient.uploadObject(any(UploadObjectArgs.class)))
            .thenReturn(CompletableFuture.failedFuture(rootCause));

        final var ex = assertThrows(
            CompletionException.class,
            () -> adapter.upload(localSource, "frames/output.png").join()
        );

        assertEquals(rootCause, ex.getCause());
    }

    @Test
    void shouldWrapSynchronousUploadFailure() throws Exception {
        final var rootCause = new RuntimeException("sync boom");
        final var localSource = Files.createTempFile("frame-", ".png");

        when(minioAsyncClient.uploadObject(any(UploadObjectArgs.class)))
            .thenThrow(rootCause);

        final var ex = assertThrows(
            CompletionException.class,
            () -> adapter.upload(localSource, "frames/output.png").join()
        );

        assertEquals(IllegalStateException.class, ex.getCause().getClass());
        assertTrue(ex.getCause().getMessage().contains("Cannot prepare upload"));
        assertEquals(rootCause, ex.getCause().getCause());
    }

}
