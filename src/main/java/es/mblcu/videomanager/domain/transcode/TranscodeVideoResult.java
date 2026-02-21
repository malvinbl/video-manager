package es.mblcu.videomanager.domain.transcode;

import java.time.Duration;
import java.util.Map;

public record TranscodeVideoResult(
    Long videoId,
    Map<String, String> outputs,
    Duration elapsed
) {
}
