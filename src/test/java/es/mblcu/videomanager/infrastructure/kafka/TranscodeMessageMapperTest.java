package es.mblcu.videomanager.infrastructure.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TranscodeMessageMapperTest {

    private final TranscodeMessageMapper mapper = new TranscodeMessageMapper();

    @Test
    void should_map_valid_message() {
        String payload = """
            {
              "videoId": 2001,
              "videoS3Path": "s3://bucket/videos/in.mp4",
              "outputS3Prefix": "s3://bucket/transcoded/2001",
              "width": 1280,
              "height": 720
            }
            """;

        final var command = mapper.toCommand(payload);

        assertThat(command.videoId()).isEqualTo(2001L);
        assertThat(command.videoS3Path()).isEqualTo("s3://bucket/videos/in.mp4");
        assertThat(command.outputS3Prefix()).isEqualTo("s3://bucket/transcoded/2001");
        assertThat(command.width()).isEqualTo(1280);
        assertThat(command.height()).isEqualTo(720);
    }

    @Test
    void should_fail_with_invalid_json() {
        assertThatThrownBy(() -> mapper.toCommand("not-json"))
            .isInstanceOf(IllegalArgumentException.class);
    }

}
