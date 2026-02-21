package es.mblcu.videomanager.infrastructure.ffmpeg;

import es.mblcu.videomanager.domain.transcode.TranscodeTarget;
import es.mblcu.videomanager.domain.transcode.VideoTranscodingPort;
import es.mblcu.videomanager.domain.transcode.exception.VideoTranscodingException;
import es.mblcu.videomanager.infrastructure.observability.Observability;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FfmpegVideoTranscodingAdapter implements VideoTranscodingPort {

    private final String ffmpegBinary;
    private final Duration timeout;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    public FfmpegVideoTranscodingAdapter(String ffmpegBinary, Duration timeout) {
        if (StringUtils.isBlank(ffmpegBinary)) {
            throw new IllegalArgumentException("ffmpegBinary cannot be null or blank");
        }

        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be > 0");
        }

        this.ffmpegBinary = ffmpegBinary;
        this.timeout = timeout;
    }

    @Override
    public CompletableFuture<Duration> transcode(Path localVideoFile, List<TranscodeTarget> targets) {
        return CompletableFuture.supplyAsync(() -> {
            final var startedAt = Instant.now();
            for (TranscodeTarget target : targets) {
                ensureOutputDirectoryExists(target.localOutputFile());
                final var command = buildCommand(localVideoFile, target);
                runAndValidate(command, target.localOutputFile(), startedAt);
            }
            final var elapsed = Duration.between(startedAt, Instant.now());
            Observability.recordExternalCallDuration("ffmpeg", "transcode_video", "success", elapsed);
            return elapsed;
        }, executorService);
    }

    List<String> buildCommand(Path localVideoFile, TranscodeTarget target) {
        return List.of(
            ffmpegBinary,
            "-y",
            "-i", localVideoFile.toString(),
            "-vf", "scale=" + target.profile().width() + ":" + target.profile().height(),
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-b:v", target.profile().videoBitrateKbps() + "k",
            "-c:a", "aac",
            "-b:a", "128k",
            target.localOutputFile().toString()
        );
    }

    private void runAndValidate(List<String> command, Path outputFile, Instant startedAt) {
        Process process;
        try {
            process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        } catch (IOException ex) {
            Observability.recordExternalCallDuration(
                "ffmpeg",
                "transcode_video",
                "error",
                Duration.between(startedAt, Instant.now())
            );
            throw new VideoTranscodingException("Cannot execute ffmpeg command", ex);
        }

        try {
            boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

            if (!finished) {
                process.destroyForcibly();
                Observability.recordExternalCallDuration(
                    "ffmpeg",
                    "transcode_video",
                    "error",
                    Duration.between(startedAt, Instant.now())
                );
                throw new VideoTranscodingException("ffmpeg timeout after " + timeout.toSeconds() + " seconds");
            }

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                Observability.recordExternalCallDuration(
                    "ffmpeg",
                    "transcode_video",
                    "error",
                    Duration.between(startedAt, Instant.now())
                );
                throw new VideoTranscodingException("ffmpeg failed with exit code " + exitCode + ". Output: " + output);
            }

            if (!Files.exists(outputFile)) {
                Observability.recordExternalCallDuration(
                    "ffmpeg",
                    "transcode_video",
                    "error",
                    Duration.between(startedAt, Instant.now())
                );
                throw new VideoTranscodingException("ffmpeg finished but output file was not generated: " + outputFile);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            Observability.recordExternalCallDuration(
                "ffmpeg",
                "transcode_video",
                "error",
                Duration.between(startedAt, Instant.now())
            );
            throw new VideoTranscodingException("Video transcoding interrupted", ex);
        } catch (IOException ex) {
            Observability.recordExternalCallDuration(
                "ffmpeg",
                "transcode_video",
                "error",
                Duration.between(startedAt, Instant.now())
            );
            throw new VideoTranscodingException("Cannot read ffmpeg output", ex);
        }
    }

    private void ensureOutputDirectoryExists(Path outputFile) {
        try {
            final var parent = outputFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException ex) {
            throw new VideoTranscodingException("Cannot create output directories", ex);
        }
    }

}
