package es.mblcu.videomanager.infrastructure.kafka;

import java.util.Map;

public record TranscodeResponseMessage(
    Long videoId,
    Map<String, String> outputs,
    String status,
    String errorDescription
) {
}
