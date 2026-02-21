package es.mblcu.videomanager.application.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class LocalWorkspaceServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldResolveVideoFrameAndTranscodedPathsAndCreateDirectories() {
        final var service = new LocalWorkspaceService(tempDir);

        final var localVideo = service.resolveLocalVideoPath("s3://bucket/videos/input.mp4");
        final var localFrame = service.resolveLocalFramePath("s3://bucket/frames/output.png");
        final var localTranscoded = service.resolveLocalTranscodedPath("s3://bucket/transcoded/v1/854x480.mp4");

        assertThat(localVideo.getParent()).isEqualTo(tempDir.resolve("videos"));
        assertThat(localFrame.getParent()).isEqualTo(tempDir.resolve("frames"));
        assertThat(localTranscoded.getParent()).isEqualTo(tempDir.resolve("transcoded"));

        assertThat(localVideo.getFileName().toString()).endsWith("-input.mp4");
        assertThat(localFrame.getFileName().toString()).endsWith("-output.png");
        assertThat(localTranscoded.getFileName().toString()).endsWith("-854x480.mp4");

        assertThat(Files.isDirectory(tempDir.resolve("videos"))).isTrue();
        assertThat(Files.isDirectory(tempDir.resolve("frames"))).isTrue();
        assertThat(Files.isDirectory(tempDir.resolve("transcoded"))).isTrue();
    }

    @Test
    void shouldUseFallbackNameWhenPathHasNoFilename() {
        final var service = new LocalWorkspaceService(tempDir);

        final var local = service.resolveLocalVideoPath("s3://bucket/videos/");

        assertThat(local.getFileName().toString()).endsWith("-media.bin");
    }

    @Test
    void shouldEnsureParentDirectoryForNestedFile() {
        final var service = new LocalWorkspaceService(tempDir);
        final var nested = tempDir.resolve("a/b/c/file.bin");

        service.ensureParentDirectory(nested);

        assertThat(Files.isDirectory(tempDir.resolve("a/b/c"))).isTrue();
    }

    @Test
    void shouldCheckExistenceAndDeleteQuietly() throws Exception {
        final var service = new LocalWorkspaceService(tempDir);
        final var file = tempDir.resolve("to-delete.txt");
        Files.writeString(file, "content");

        assertThat(service.exists(file)).isTrue();

        service.deleteQuietly(file);

        assertThat(service.exists(file)).isFalse();

        // best-effort delete on non-existing file should not fail
        service.deleteQuietly(file);
        assertThat(service.exists(file)).isFalse();
    }

}
