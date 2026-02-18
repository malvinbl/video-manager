package es.mblcu.videomanager.infrastructure.kafka;

public record ExtractFrameResponseMessage(
    Long videoId,
    String frameS3Path,
    String status,
    String errorDescription
) {
}
