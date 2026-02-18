package es.mblcu.videomanager.application.usecase;

import es.mblcu.videomanager.application.service.LocalWorkspaceService;
import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import es.mblcu.videomanager.domain.FileRepository;
import es.mblcu.videomanager.domain.frame.FrameExtractionPort;
import es.mblcu.videomanager.domain.jobs.JobStateRepository;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import lombok.RequiredArgsConstructor;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
public class ExtractFrameUseCase {

    private final FrameExtractionPort frameExtractionPort;
    private final FileRepository fileRepository;
    private final LocalWorkspaceService localWorkspaceService;
    private final JobStateRepository jobStateRepository;

    private final ConcurrentHashMap<Path, CompletableFuture<Void>> inFlightDownloads = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> localLocks = new ConcurrentHashMap<>();

    public CompletableFuture<ExtractFrameResult> execute(ExtractFrameCommand command) {
        String jobId = buildJobId(command);

        return jobStateRepository.findJob(jobId)
            .thenCompose(existing -> {
                if (existing.isPresent() && existing.get().status() == JobStatus.SUCCESS) {
                    Duration elapsed = Duration.ofMillis(existing.get().elapsedMillis() == null ? 0 : existing.get().elapsedMillis());
                    return CompletableFuture.completedFuture(
                        new ExtractFrameResult(command.videoId(), command.frameS3Path(), elapsed)
                    );
                }

                final var localVideoFile = localWorkspaceService.resolveLocalVideoPath(command.videoS3Path());
                final var localFrameFile = localWorkspaceService.resolveLocalFramePath(command.frameS3Path());

                return jobStateRepository.markRunning(jobId, command)
                    .thenCompose(ignored -> jobStateRepository.incrementVideoRef(command.videoS3Path()))
                    .thenCompose(ignored -> ensureVideoDownloaded(command.videoS3Path(), localVideoFile))
                    .thenCompose(ignored -> frameExtractionPort.extractFrame(localVideoFile, localFrameFile, command.second()))
                    .thenCompose(elapsed -> fileRepository
                        .upload(localFrameFile, command.frameS3Path())
                        .thenApply(v -> new ExtractFrameResult(command.videoId(), command.frameS3Path(), elapsed))
                    )
                    .thenCompose(result -> jobStateRepository.markSuccess(jobId, result).thenApply(v -> result))
                    .exceptionallyCompose(ex -> {
                        String message = rootMessage(ex);
                        return jobStateRepository.markError(jobId, command, message)
                            .thenCompose(v -> CompletableFuture.failedFuture(ex));
                    })
                    .whenComplete((result, throwable) -> {
                        localWorkspaceService.deleteQuietly(localFrameFile);

                        jobStateRepository.decrementVideoRef(command.videoS3Path())
                            .thenAccept(ref -> {
                                if (ref <= 0 && !inFlightDownloads.containsKey(localVideoFile)) {
                                    localWorkspaceService.deleteQuietly(localVideoFile);
                                }
                            });
                    });
            });
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

    private String buildJobId(ExtractFrameCommand command) {
        return command.videoId() + "|" + command.frameS3Path() + "|" + command.second();
    }

    private String rootMessage(Throwable throwable) {
        var current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? "Unknown error" : current.getMessage();
    }

}
