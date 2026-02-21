package es.mblcu.videomanager.application.usecase;

import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobState;
import es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobStateRepository;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class TranscodeStartupRecoveryUseCase {

    private final TranscodeJobStateRepository repository;

    public CompletableFuture<Integer> recoverRunningJobs() {
        return repository.findJobsByStatus(JobStatus.RUNNING)
            .thenCompose(runningJobs -> {
                if (runningJobs.isEmpty()) {
                    return CompletableFuture.completedFuture(0);
                }

                CompletableFuture<?>[] recoveries = runningJobs.stream()
                    .map(this::markAsRecoveredError)
                    .toArray(CompletableFuture[]::new);

                return CompletableFuture.allOf(recoveries).thenApply(v -> runningJobs.size());
            });
    }

    private CompletableFuture<Void> markAsRecoveredError(TranscodeJobState job) {
        final var command = new TranscodeVideoCommand(
            job.videoId(),
            StringUtils.defaultIfBlank(job.videoS3Path(), "unknown"),
            StringUtils.defaultIfBlank(job.outputS3Prefix(), "unknown"),
            job.inputWidth() == null ? 1 : job.inputWidth(),
            job.inputHeight() == null ? 1 : job.inputHeight()
        );

        return repository.markError(
            job.jobId(),
            command,
            "Recovered at startup after previous unclean shutdown"
        );
    }

}
