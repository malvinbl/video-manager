package es.mblcu.videomanager.infrastructure.ffmpeg;

import es.mblcu.videomanager.domain.transcode.TranscodeProfile;
import es.mblcu.videomanager.domain.transcode.TranscodeTarget;
import es.mblcu.videomanager.domain.transcode.exception.VideoTranscodingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FfmpegVideoTranscodingAdapterTest {

    @Test
    void should_build_expected_command() {
        final var adapter = new FfmpegVideoTranscodingAdapter("ffmpeg", Duration.ofSeconds(30));
        final var target = new TranscodeTarget(
            new TranscodeProfile("854x480", 854, 480, 1400),
            "s3://bucket/transcoded/10/854x480.mp4",
            Path.of("out/854x480.mp4")
        );

        final var command = adapter.buildCommand(Path.of("in.mp4"), target);

        assertThat(command).isEqualTo(List.of(
            "ffmpeg",
            "-y",
            "-i", "in.mp4",
            "-vf", "scale=854:480",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-b:v", "1400k",
            "-c:a", "aac",
            "-b:a", "128k",
            "out/854x480.mp4"
        ));
    }

    @Test
    void should_fail_when_binary_is_blank() {
        assertThatThrownBy(() -> new FfmpegVideoTranscodingAdapter("", Duration.ofSeconds(10)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void should_fail_when_timeout_is_invalid() {
        assertThatThrownBy(() -> new FfmpegVideoTranscodingAdapter("ffmpeg", Duration.ZERO))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void should_transcode_successfully(@TempDir Path tempDir) throws Exception {
        final var fakeFfmpeg = createExecutableScript(tempDir, "fake-ffmpeg-ok.sh", """
            #!/bin/sh
            for last; do :; done
            mkdir -p "$(dirname "$last")"
            echo "video-bytes" > "$last"
            exit 0
            """);

        final var adapter = new FfmpegVideoTranscodingAdapter(fakeFfmpeg.toString(), Duration.ofSeconds(5));
        final var input = tempDir.resolve("in.mp4");
        Files.writeString(input, "input-bytes");
        final var output = tempDir.resolve("out/854x480.mp4");
        final var target = new TranscodeTarget(
            new TranscodeProfile("854x480", 854, 480, 1400),
            "s3://bucket/transcoded/10/854x480.mp4",
            output
        );

        final var elapsed = adapter.transcode(input, List.of(target)).join();

        assertThat(elapsed.toMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(Files.exists(output)).isTrue();
    }

    @Test
    void should_fail_transcode_when_process_returns_error(@TempDir Path tempDir) throws Exception {
        final var fakeFfmpeg = createExecutableScript(tempDir, "fake-ffmpeg-fail.sh", """
            #!/bin/sh
            echo "boom" 1>&2
            exit 2
            """);

        final var adapter = new FfmpegVideoTranscodingAdapter(fakeFfmpeg.toString(), Duration.ofSeconds(5));
        final var input = tempDir.resolve("in.mp4");
        Files.writeString(input, "input-bytes");
        final var target = new TranscodeTarget(
            new TranscodeProfile("854x480", 854, 480, 1400),
            "s3://bucket/transcoded/10/854x480.mp4",
            tempDir.resolve("out/854x480.mp4")
        );

        assertThatThrownBy(() -> adapter.transcode(input, List.of(target)).join())
            .hasCauseInstanceOf(VideoTranscodingException.class);
    }

    private Path createExecutableScript(Path tempDir, String fileName, String content) throws IOException {
        final var script = tempDir.resolve(fileName);
        Files.writeString(script, content);

        boolean executable = script.toFile().setExecutable(true);
        if (!executable) {
            throw new IllegalStateException("Cannot mark test script as executable: " + script);
        }

        return script;
    }

}
