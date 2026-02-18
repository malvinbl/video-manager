package es.mblcu.videomanager.atdd.support;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DockerComposeSupport {

    private final Path workdir;

    public DockerComposeSupport(Path workdir) {
        this.workdir = workdir;
    }

    public void up() {
        runOrFail(List.of("docker", "compose", "up", "-d", "--build"), Duration.ofMinutes(8));
    }

    public void down() {
        runOrFail(List.of("docker", "compose", "down", "-v"), Duration.ofMinutes(3));
    }

    public List<String> runningServices() {
        String output = runOrFail(List.of("docker", "compose", "ps", "--services", "--status", "running"), Duration.ofMinutes(1));
        return Arrays.stream(output.split("\\R"))
            .map(String::trim)
            .filter(line -> !line.isEmpty())
            .toList();
    }

    private String runOrFail(List<String> command, Duration timeout) {
        final var processBuilder = new ProcessBuilder(command);
        processBuilder.directory(workdir.toFile());
        processBuilder.redirectErrorStream(true);

        try {
            var process = processBuilder.start();
            boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

            if (!finished) {
                process.destroyForcibly();
                throw new IllegalStateException("Timeout running command: " + String.join(" ", command));
            }

            if (process.exitValue() != 0) {
                throw new IllegalStateException(
                    "Command failed (" + process.exitValue() + "): " + String.join(" ", command) + System.lineSeparator() + output
                );
            }

            return output;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Cannot run command: " + String.join(" ", command), ex);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot run command: " + String.join(" ", command), ex);
        }
    }

}
