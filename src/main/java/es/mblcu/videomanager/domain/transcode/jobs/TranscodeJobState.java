package es.mblcu.videomanager.domain.transcode.jobs;

import es.mblcu.videomanager.domain.jobs.vo.JobStatus;

import java.util.Map;

public record TranscodeJobState(
    String jobId,
    Long videoId,
    String videoS3Path,
    String outputS3Prefix,
    Integer inputWidth,
    Integer inputHeight,
    JobStatus status,
    Long elapsedMillis,
    String errorDescription,
    Map<String, String> outputs
) {
}
