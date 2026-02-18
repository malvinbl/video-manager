package es.mblcu.videomanager.infrastructure.ffmpeg;

import es.mblcu.videomanager.domain.frame.FrameExtractionPort;
import es.mblcu.videomanager.domain.frame.exception.FrameExtractionException;
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

public class FfmpegFrameExtractionAdapter implements FrameExtractionPort {

    private final String ffmpegBinary;
    private final Duration timeout;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    public FfmpegFrameExtractionAdapter(String ffmpegBinary, Duration timeout) {
        if (StringUtils.isEmpty(ffmpegBinary)) {
            throw new IllegalArgumentException("ffmpegBinary cannot be null or blank");
        }

        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be > 0");
        }

        this.ffmpegBinary = ffmpegBinary;
        this.timeout = timeout;
    }

    @Override
    public CompletableFuture<Duration> extractFrame(Path localVideoFile, Path localFrameFile, double second) {
        return CompletableFuture.supplyAsync(() -> {
            ensureOutputDirectoryExists(localFrameFile);

            List<String> ffmpegCommand = buildCommand(localVideoFile, localFrameFile, second);
            final var start = Instant.now();

            Process process;
            try {
                process = new ProcessBuilder(ffmpegCommand)
                    .redirectErrorStream(true)
                    .start();
            } catch (IOException ex) {
                throw new FrameExtractionException("Cannot execute ffmpeg command", ex);
            }

            waitForFinish(localFrameFile, process);

            return Duration.between(start, Instant.now());
        }, executorService);
    }

    void ensureOutputDirectoryExists(Path outputFile) {
        try {
            final var parent = outputFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException ex) {
            throw new FrameExtractionException("Cannot create output directories", ex);
        }
    }

    void waitForFinish(Path outputFile, Process process) {
        String output;
        try {
            boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

            if (!finished) {
                process.destroyForcibly();
                throw new FrameExtractionException("ffmpeg timeout after " + timeout.toSeconds() + " seconds");
            }

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                throw new FrameExtractionException("ffmpeg failed with exit code " + exitCode + ". Output: " + output);
            }

            if (!Files.exists(outputFile)) {
                throw new FrameExtractionException("ffmpeg finished but output file was not generated: " + outputFile);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FrameExtractionException("Frame extraction interrupted", ex);
        } catch (IOException ex) {
            throw new FrameExtractionException("Cannot read ffmpeg output", ex);
        }
    }

    List<String> buildCommand(Path localVideoFile, Path localFrameFile, double second) {
        return List.of(
            ffmpegBinary,
            "-y",
            "-ss", String.valueOf(second),
            "-i", localVideoFile.toString(),
            "-frames:v", "1",
            "-q:v", "2",
            localFrameFile.toString()
        );
    }

}
