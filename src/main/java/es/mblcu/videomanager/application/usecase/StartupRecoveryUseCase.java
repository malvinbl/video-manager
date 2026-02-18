package es.mblcu.videomanager.application.usecase;

import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.jobs.JobState;
import es.mblcu.videomanager.domain.jobs.JobStateRepository;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class StartupRecoveryUseCase {

    private final JobStateRepository jobStateRepository;

    public CompletableFuture<Integer> recoverRunningJobs() {
        return jobStateRepository.findJobsByStatus(JobStatus.RUNNING)
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

    private CompletableFuture<Void> markAsRecoveredError(JobState runningJob) {
        final var command = new ExtractFrameCommand(
            runningJob.videoId(),
            runningJob.videoS3Path(),
            runningJob.frameS3Path(),
            secondFromJobId(runningJob.jobId())
        );

        return jobStateRepository.markError(
            runningJob.jobId(),
            command,
            "Recovered at startup after previous unclean shutdown"
        );
    }

    private double secondFromJobId(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return 1.0d;
        }

        int idx = jobId.lastIndexOf('|');
        if (idx < 0 || idx == jobId.length() - 1) {
            return 1.0d;
        }

        String raw = jobId.substring(idx + 1);
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException ignored) {
            return 1.0d;
        }
    }

}
