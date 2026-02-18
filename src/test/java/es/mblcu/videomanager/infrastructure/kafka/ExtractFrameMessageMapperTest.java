package es.mblcu.videomanager.infrastructure.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExtractFrameMessageMapperTest {

    private final ExtractFrameMessageMapper mapper = new ExtractFrameMessageMapper();

    @Test
    void shouldMapValidMessage() {
        String payload = """
            {
              "videoId": 1001,
              "videoS3Path": "s3://bucket/videos/video.mp4",
              "frameS3Path": "s3://bucket/frames/frame.png",
              "second": 2.5
            }
            """;

        final var command = mapper.toCommand(payload);

        assertEquals(1001L, command.videoId());
        assertEquals("s3://bucket/videos/video.mp4", command.videoS3Path());
        assertEquals("s3://bucket/frames/frame.png", command.frameS3Path());
        assertEquals(2.5d, command.second());
    }

    @Test
    void shouldMapLegacyFieldsForCompatibility() {
        String payload = """
            {
              "videoId": 1002,
              "input": "s3://bucket/videos/video.mp4",
              "outputFile": "s3://bucket/frames/frame.png"
            }
            """;

        final var command = mapper.toCommand(payload);

        assertEquals(1002L, command.videoId());
        assertEquals("s3://bucket/videos/video.mp4", command.videoS3Path());
        assertEquals("s3://bucket/frames/frame.png", command.frameS3Path());
        assertEquals(1.0d, command.second());
    }

    @Test
    void shouldFailWithInvalidJson() {
        assertThrows(IllegalArgumentException.class, () -> mapper.toCommand("not-json"));
    }

}
