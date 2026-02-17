package es.mblcu.videomanager.domain.jobs;

import es.mblcu.videomanager.domain.jobs.vo.JobStatus;

public record JobState(
    String jobId,
    Long videoId,
    String videoS3Path,
    String frameS3Path,
    JobStatus status,
    Long elapsedMillis,
    String errorDescription
) {
}
