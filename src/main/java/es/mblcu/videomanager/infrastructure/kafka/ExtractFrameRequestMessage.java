package es.mblcu.videomanager.infrastructure.kafka;

public record ExtractFrameRequestMessage(
    Long videoId,
    String videoS3Path,
    String frameS3Path,
    String input,
    String outputFile,
    Double second
) {
}
