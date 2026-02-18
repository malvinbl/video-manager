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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

        assertThatCode(() -> adapter.download("s3://source-bucket/videos/input.mp4", Path.of("tmp/input.mp4")).join())
            .doesNotThrowAnyException();

        final var requestCaptor = ArgumentCaptor.forClass(DownloadObjectArgs.class);
        verify(minioAsyncClient).downloadObject(requestCaptor.capture());

        final var request = requestCaptor.getValue();

        assertThat(request.bucket()).isEqualTo("source-bucket");
        assertThat(request.object()).isEqualTo("videos/input.mp4");
        assertThat(request.filename()).isEqualTo("tmp/input.mp4");
    }

    @Test
    void shouldUploadUsingDefaultBucketWhenPathIsRelative() throws Exception {
        final var localSource = Files.createTempFile("frame-", ".png");

        when(minioAsyncClient.uploadObject(any(UploadObjectArgs.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        assertThatCode(() -> adapter.upload(localSource, "frames/output.png").join())
            .doesNotThrowAnyException();

        final var requestCaptor = ArgumentCaptor.forClass(UploadObjectArgs.class);
        verify(minioAsyncClient).uploadObject(requestCaptor.capture());

        final var request = requestCaptor.getValue();

        assertThat(request.bucket()).isEqualTo("video-bucket");
        assertThat(request.object()).isEqualTo("frames/output.png");
        assertThat(request.filename()).isEqualTo(localSource.toString());
    }

    @Test
    void shouldWrapInvalidDownloadPath() {
        assertThatThrownBy(() -> adapter.download("s3://broken", Path.of("tmp/input.mp4")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid S3 URI");
    }

    @Test
    void shouldPropagateAsyncUploadFailure() throws Exception {
        final var rootCause = new RuntimeException("upload boom");
        final var localSource = Files.createTempFile("frame-", ".png");

        when(minioAsyncClient.uploadObject(any(UploadObjectArgs.class)))
            .thenReturn(CompletableFuture.failedFuture(rootCause));

        assertThatThrownBy(() -> adapter.upload(localSource, "frames/output.png").join())
            .isInstanceOf(CompletionException.class)
            .hasCause(rootCause);
    }

    @Test
    void shouldWrapSynchronousUploadFailure() throws Exception {
        final var rootCause = new RuntimeException("sync boom");
        final var localSource = Files.createTempFile("frame-", ".png");

        when(minioAsyncClient.uploadObject(any(UploadObjectArgs.class)))
            .thenThrow(rootCause);

        assertThatThrownBy(() -> adapter.upload(localSource, "frames/output.png").join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(IllegalStateException.class)
            .rootCause()
            .isEqualTo(rootCause);
    }

}
