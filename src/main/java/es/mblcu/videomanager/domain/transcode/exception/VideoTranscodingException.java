package es.mblcu.videomanager.domain.transcode.exception;

public class VideoTranscodingException extends RuntimeException {

    public VideoTranscodingException(String message) {
        super(message);
    }

    public VideoTranscodingException(String message, Throwable cause) {
        super(message, cause);
    }

}
