package es.mblcu.videomanager.domain.transcode.jobs;

import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoResult;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface TranscodeJobStateRepository {

    CompletableFuture<Optional<TranscodeJobState>> findJob(String jobId);

    CompletableFuture<List<TranscodeJobState>> findJobsByStatus(JobStatus status);

    CompletableFuture<Void> markRunning(String jobId, TranscodeVideoCommand command);

    CompletableFuture<Void> markSuccess(String jobId, TranscodeVideoResult result);

    CompletableFuture<Void> markError(String jobId, TranscodeVideoCommand command, String errorDescription);

}
