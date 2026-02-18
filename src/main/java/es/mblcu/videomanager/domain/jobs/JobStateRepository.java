package es.mblcu.videomanager.domain.jobs;

import java.util.Optional;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;

public interface JobStateRepository {

    CompletableFuture<Optional<JobState>> findJob(String jobId);

    CompletableFuture<List<JobState>> findJobsByStatus(JobStatus status);

    CompletableFuture<Void> markRunning(String jobId, ExtractFrameCommand command);

    CompletableFuture<Void> markSuccess(String jobId, ExtractFrameResult result);

    CompletableFuture<Void> markError(String jobId, ExtractFrameCommand command, String errorDescription);

    CompletableFuture<Long> incrementVideoRef(String videoS3Path);

    CompletableFuture<Long> decrementVideoRef(String videoS3Path);

}
