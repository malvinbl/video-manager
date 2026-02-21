package es.mblcu.videomanager.domain.transcode;

import org.apache.commons.lang3.StringUtils;

public record TranscodeVideoCommand(
    Long videoId,
    String videoS3Path,
    String outputS3Prefix,
    int width,
    int height
) {

    public TranscodeVideoCommand {
        if (videoId == null) {
            throw new IllegalArgumentException("videoId cannot be null");
        }
        if (StringUtils.isBlank(videoS3Path)) {
            throw new IllegalArgumentException("videoS3Path cannot be null or blank");
        }
        if (StringUtils.isBlank(outputS3Prefix)) {
            throw new IllegalArgumentException("outputS3Prefix cannot be null or blank");
        }
        if (width <= 0 || height <= 0) {
            throw new IllegalArgumentException("width and height must be > 0");
        }
    }

}
