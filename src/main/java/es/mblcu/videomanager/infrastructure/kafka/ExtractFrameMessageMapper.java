package es.mblcu.videomanager.infrastructure.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;

import java.io.IOException;

public class ExtractFrameMessageMapper {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public ExtractFrameCommand toCommand(String payload) {
        ExtractFrameRequestMessage message;
        try {
            message = objectMapper.readValue(payload, ExtractFrameRequestMessage.class);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Invalid JSON payload", ex);
        }

        String videoPath = firstNonBlank(message.videoS3Path(), message.input());
        String framePath = firstNonBlank(message.frameS3Path(), message.outputFile());
        double second = message.second() == null ? 1.0d : message.second();

        return new ExtractFrameCommand(message.videoId(), videoPath, framePath, second);
    }

    private String firstNonBlank(String value, String fallback) {
        if (value != null && !value.isBlank()) {
            return value;
        }
        return fallback;
    }

}
