package es.mblcu.videomanager.domain.frame;

import java.time.Duration;

public record ExtractFrameResult(Long videoId, String frameS3Path, Duration elapsed) {
}
