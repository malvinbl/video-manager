package es.mblcu.videomanager.infrastructure.s3;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class S3PathTest {

    @Test
    void should_parse_s3_uri() {
        S3Path path = S3Path.parse("s3://my-bucket/videos/sample.mp4", "default-bucket");

        assertThat(path.bucket()).isEqualTo("my-bucket");
        assertThat(path.key()).isEqualTo("videos/sample.mp4");
    }

    @Test
    void should_use_default_bucket_for_raw_key() {
        S3Path path = S3Path.parse("videos/sample.mp4", "default-bucket");

        assertThat(path.bucket()).isEqualTo("default-bucket");
        assertThat(path.key()).isEqualTo("videos/sample.mp4");
    }

}
