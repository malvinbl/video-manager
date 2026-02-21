package es.mblcu.videomanager.application.usecase;

import es.mblcu.videomanager.application.service.LocalWorkspaceService;
import es.mblcu.videomanager.domain.FileRepository;
import es.mblcu.videomanager.domain.jobs.JobStateRepository;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import es.mblcu.videomanager.domain.transcode.TranscodeProfile;
import es.mblcu.videomanager.domain.transcode.TranscodeTarget;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoResult;
import es.mblcu.videomanager.domain.transcode.VideoTranscodingPort;
import es.mblcu.videomanager.domain.transcode.exception.TranscodeValidationException;
import es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobStateRepository;
import lombok.RequiredArgsConstructor;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
public class TranscodeVideoUseCase {

    private final VideoTranscodingPort videoTranscodingPort;
    private final FileRepository fileRepository;
    private final LocalWorkspaceService localWorkspaceService;
    private final JobStateRepository sharedVideoRefRepository;
    private final TranscodeJobStateRepository transcodeJobStateRepository;
    private final TranscodeProfileCatalog profileCatalog;

    private final ConcurrentHashMap<Path, CompletableFuture<Void>> inFlightDownloads = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> localLocks = new ConcurrentHashMap<>();

    public CompletableFuture<TranscodeVideoResult> execute(TranscodeVideoCommand command) {
        String jobId = buildJobId(command);

        if (!profileCatalog.supports(command.width(), command.height())) {
            return CompletableFuture.failedFuture(
                new TranscodeValidationException(
                    "Unsupported dimensions: " + command.width() + "x" + command.height()
                )
            );
        }

        return transcodeJobStateRepository.findJob(jobId)
            .thenCompose(existing -> {
                if (existing.isPresent() && existing.get().status() == JobStatus.SUCCESS) {
                    Duration elapsed = Duration.ofMillis(existing.get().elapsedMillis() == null ? 0 : existing.get().elapsedMillis());
                    return CompletableFuture.completedFuture(
                        new TranscodeVideoResult(command.videoId(), existing.get().outputs() == null ? Map.of() : existing.get().outputs(), elapsed)
                    );
                }

                final var localVideoFile = localWorkspaceService.resolveLocalVideoPath(command.videoS3Path());
                final var profiles = profileCatalog.profilesFor(command.width(), command.height());
                final var targets = buildTargets(command.outputS3Prefix(), profiles);

                return transcodeJobStateRepository.markRunning(jobId, command)
                    .thenCompose(ignored -> sharedVideoRefRepository.incrementVideoRef(command.videoS3Path()))
                    .thenCompose(ignored -> ensureVideoDownloaded(command.videoS3Path(), localVideoFile))
                    .thenCompose(ignored -> videoTranscodingPort.transcode(localVideoFile, targets))
                    .thenCompose(elapsed -> uploadTargets(targets).thenApply(v ->
                        new TranscodeVideoResult(command.videoId(), outputsMap(targets), elapsed)))
                    .thenCompose(result -> transcodeJobStateRepository.markSuccess(jobId, result).thenApply(v -> result))
                    .exceptionallyCompose(ex -> transcodeJobStateRepository
                        .markError(jobId, command, rootMessage(ex))
                        .thenCompose(v -> CompletableFuture.failedFuture(ex)))
                    .whenComplete((result, throwable) -> {
                        cleanupOutputs(targets);
                        sharedVideoRefRepository.decrementVideoRef(command.videoS3Path())
                            .thenAccept(ref -> {
                                if (ref <= 0 && !inFlightDownloads.containsKey(localVideoFile)) {
                                    localWorkspaceService.deleteQuietly(localVideoFile);
                                }
                            });
                    });
            });
    }

    private List<TranscodeTarget> buildTargets(String outputS3Prefix, List<TranscodeProfile> profiles) {
        final var normalizedPrefix = outputS3Prefix.endsWith("/") ? outputS3Prefix.substring(0, outputS3Prefix.length() - 1) : outputS3Prefix;
        final var targets = new ArrayList<TranscodeTarget>(profiles.size());

        for (TranscodeProfile profile : profiles) {
            String outputS3Path = normalizedPrefix + "/" + profile.name() + ".mp4";
            Path localOutputFile = localWorkspaceService.resolveLocalTranscodedPath(outputS3Path);
            targets.add(new TranscodeTarget(profile, outputS3Path, localOutputFile));
        }
        return targets;
    }

    private CompletableFuture<Void> uploadTargets(List<TranscodeTarget> targets) {
        CompletableFuture<?>[] uploads = targets.stream()
            .map(target -> fileRepository.upload(target.localOutputFile(), target.outputS3Path()))
            .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(uploads);
    }

    private void cleanupOutputs(List<TranscodeTarget> targets) {
        for (TranscodeTarget target : targets) {
            localWorkspaceService.deleteQuietly(target.localOutputFile());
        }
    }

    private Map<String, String> outputsMap(List<TranscodeTarget> targets) {
        final var outputs = new LinkedHashMap<String, String>();
        for (TranscodeTarget target : targets) {
            outputs.put(target.profile().name(), target.outputS3Path());
        }
        return outputs;
    }

    private CompletableFuture<Void> ensureVideoDownloaded(String videoS3Path, Path localVideoFile) {
        if (localWorkspaceService.exists(localVideoFile)) {
            return CompletableFuture.completedFuture(null);
        }

        Object lock = localLocks.computeIfAbsent(localVideoFile.toString(), ignored -> new Object());
        synchronized (lock) {
            if (localWorkspaceService.exists(localVideoFile)) {
                return CompletableFuture.completedFuture(null);
            }

            CompletableFuture<Void> existingDownload = inFlightDownloads.get(localVideoFile);
            if (existingDownload != null) {
                return existingDownload;
            }

            localWorkspaceService.ensureParentDirectory(localVideoFile);

            CompletableFuture<Void> downloadFuture = fileRepository.download(videoS3Path, localVideoFile);
            inFlightDownloads.put(localVideoFile, downloadFuture);
            downloadFuture.whenComplete((result, throwable) -> inFlightDownloads.remove(localVideoFile, downloadFuture));

            return downloadFuture;
        }
    }

    private String buildJobId(TranscodeVideoCommand command) {
        return command.videoId() + "|" + command.outputS3Prefix() + "|" + command.width() + "x" + command.height();
    }

    private String rootMessage(Throwable throwable) {
        var current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? "Unknown error" : current.getMessage();
    }

}
