package es.mblcu.videomanager.infrastructure.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExtractFrameMessageMapperTest {

    private final ExtractFrameMessageMapper mapper = new ExtractFrameMessageMapper();

    @Test
    void should_map_valid_message() {
        String payload = """
            {
              "videoId": 1001,
              "videoS3Path": "s3://bucket/videos/video.mp4",
              "frameS3Path": "s3://bucket/frames/frame.png",
              "second": 2.5
            }
            """;

        final var command = mapper.toCommand(payload);

        assertThat(command.videoId()).isEqualTo(1001L);
        assertThat(command.videoS3Path()).isEqualTo("s3://bucket/videos/video.mp4");
        assertThat(command.frameS3Path()).isEqualTo("s3://bucket/frames/frame.png");
        assertThat(command.second()).isEqualTo(2.5d);
    }

    @Test
    void should_map_legacy_fields_for_compatibility() {
        String payload = """
            {
              "videoId": 1002,
              "input": "s3://bucket/videos/video.mp4",
              "outputFile": "s3://bucket/frames/frame.png"
            }
            """;

        final var command = mapper.toCommand(payload);

        assertThat(command.videoId()).isEqualTo(1002L);
        assertThat(command.videoS3Path()).isEqualTo("s3://bucket/videos/video.mp4");
        assertThat(command.frameS3Path()).isEqualTo("s3://bucket/frames/frame.png");
        assertThat(command.second()).isEqualTo(1.0d);
    }

    @Test
    void should_fail_with_invalid_json() {
        assertThatThrownBy(() -> mapper.toCommand("not-json"))
            .isInstanceOf(IllegalArgumentException.class);
    }

}
