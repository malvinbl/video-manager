package es.mblcu.videomanager.domain.frame;

public record ExtractFrameCommand(Long videoId, String videoS3Path, String frameS3Path, double second) {

    public ExtractFrameCommand {
        if (videoId == null || videoId <= 0) {
            throw new IllegalArgumentException("videoId must be > 0");
        }

        if (videoS3Path == null || videoS3Path.isBlank()) {
            throw new IllegalArgumentException("videoS3Path cannot be null or blank");
        }

        if (frameS3Path == null || frameS3Path.isBlank()) {
            throw new IllegalArgumentException("frameS3Path cannot be null or blank");
        }

        if (second < 0) {
            throw new IllegalArgumentException("second must be >= 0");
        }
    }

}
