package es.mblcu.videomanager.infrastructure.kafka;

public record TranscodeRequestMessage(
    Long videoId,
    String videoS3Path,
    String outputS3Prefix,
    Integer width,
    Integer height
) {
}
