package es.mblcu.videomanager.infrastructure.ffmpeg;

import es.mblcu.videomanager.domain.frame.exception.FrameExtractionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FfmpegFrameExtractionAdapterTest {

    @Test
    void shouldBuildExpectedCommand() {
        final var adapter = new FfmpegFrameExtractionAdapter("ffmpeg", Duration.ofSeconds(30));

        final var built = adapter.buildCommand(Path.of("in.mp4"), Path.of("out/frame.png"), 1.25);

        assertThat(built)
            .isEqualTo(List.of("ffmpeg", "-y", "-ss", "1.25", "-i", "in.mp4", "-frames:v", "1", "-q:v", "2", "out/frame.png"));
    }

    @Test
    void shouldFailWhenBinaryIsBlank() {
        assertThatThrownBy(() -> new FfmpegFrameExtractionAdapter("", Duration.ofSeconds(10)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailWhenTimeoutIsInvalid() {
        assertThatThrownBy(() -> new FfmpegFrameExtractionAdapter("ffmpeg", Duration.ZERO))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FfmpegFrameExtractionAdapter("ffmpeg", Duration.ofSeconds(-1)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldCreateOutputDirectory(@TempDir Path tempDir) {
        final var adapter = new FfmpegFrameExtractionAdapter("ffmpeg", Duration.ofSeconds(30));
        final var outputFile = tempDir.resolve("nested/dir/frame.png");

        adapter.ensureOutputDirectoryExists(outputFile);

        assertThat(Files.exists(outputFile.getParent())).isTrue();
    }

    @Test
    void shouldFailOnTimeoutAndDestroyProcess(@TempDir Path tempDir) {
        final var adapter = new FfmpegFrameExtractionAdapter("ffmpeg", Duration.ofSeconds(1));
        final var outputFile = tempDir.resolve("frame.png");
        final var process = new FakeProcess(false, 0, "running");

        assertThatThrownBy(() -> adapter.waitForFinish(outputFile, process))
            .isInstanceOf(FrameExtractionException.class)
            .hasMessageContaining("timeout");
        assertThat(process.destroyForciblyCalled).isTrue();
    }

    @Test
    void shouldFailWhenExitCodeIsNotZero(@TempDir Path tempDir) {
        final var adapter = new FfmpegFrameExtractionAdapter("ffmpeg", Duration.ofSeconds(30));
        final var outputFile = tempDir.resolve("frame.png");
        final var process = new FakeProcess(true, 2, "ffmpeg error line");

        assertThatThrownBy(() -> adapter.waitForFinish(outputFile, process))
            .isInstanceOf(FrameExtractionException.class)
            .hasMessageContaining("exit code 2")
            .hasMessageContaining("ffmpeg error line");
    }

    @Test
    void shouldFailWhenOutputFileWasNotGenerated(@TempDir Path tempDir) {
        final var adapter = new FfmpegFrameExtractionAdapter("ffmpeg", Duration.ofSeconds(30));
        final var outputFile = tempDir.resolve("missing.png");
        final var process = new FakeProcess(true, 0, "ok");

        assertThatThrownBy(() -> adapter.waitForFinish(outputFile, process))
            .isInstanceOf(FrameExtractionException.class)
            .hasMessageContaining("output file was not generated");
    }

    @Test
    void shouldSucceedWhenProcessFinishesAndOutputExists(@TempDir Path tempDir) throws Exception {
        final var adapter = new FfmpegFrameExtractionAdapter("ffmpeg", Duration.ofSeconds(30));
        final var outputFile = tempDir.resolve("frame.png");
        Files.createFile(outputFile);
        final var process = new FakeProcess(true, 0, "ok");

        adapter.waitForFinish(outputFile, process);

        assertThat(process.destroyForciblyCalled).isFalse();
    }

    private static final class FakeProcess extends Process {

        private final boolean finished;
        private final int exitCode;
        private final String output;
        private boolean destroyForciblyCalled;

        private FakeProcess(boolean finished, int exitCode, String output) {
            this.finished = finished;
            this.exitCode = exitCode;
            this.output = output;
        }

        @Override
        public OutputStream getOutputStream() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(output.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public InputStream getErrorStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public int waitFor() {
            return exitCode;
        }

        @Override
        public boolean waitFor(long timeout, TimeUnit unit) {
            return finished;
        }

        @Override
        public int exitValue() {
            return exitCode;
        }

        @Override
        public void destroy() {
        }

        @Override
        public Process destroyForcibly() {
            destroyForciblyCalled = true;
            return this;
        }
    }

}
