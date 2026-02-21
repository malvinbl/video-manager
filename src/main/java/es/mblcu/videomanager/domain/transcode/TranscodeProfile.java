package es.mblcu.videomanager.domain.transcode;

public record TranscodeProfile(
    String name,
    int width,
    int height,
    int videoBitrateKbps
) {
}
