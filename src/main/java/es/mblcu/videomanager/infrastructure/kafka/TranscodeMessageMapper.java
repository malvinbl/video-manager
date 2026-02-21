package es.mblcu.videomanager.infrastructure.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;

import java.io.IOException;

public class TranscodeMessageMapper {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public TranscodeVideoCommand toCommand(String payload) {
        final TranscodeRequestMessage message;
        try {
            message = objectMapper.readValue(payload, TranscodeRequestMessage.class);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Invalid JSON payload", ex);
        }

        return new TranscodeVideoCommand(
            message.videoId(),
            message.videoS3Path(),
            message.outputS3Prefix(),
            message.width() == null ? 0 : message.width(),
            message.height() == null ? 0 : message.height()
        );
    }

}
