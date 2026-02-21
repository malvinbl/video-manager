package es.mblcu.videomanager.domain.transcode;

import java.nio.file.Path;

public record TranscodeTarget(
    TranscodeProfile profile,
    String outputS3Path,
    Path localOutputFile
) {
}
