package es.mblcu.videomanager.infrastructure.s3;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3PathTest {

    @Test
    void shouldParseS3Uri() {
        S3Path path = S3Path.parse("s3://my-bucket/videos/sample.mp4", "default-bucket");

        assertEquals("my-bucket", path.bucket());
        assertEquals("videos/sample.mp4", path.key());
    }

    @Test
    void shouldUseDefaultBucketForRawKey() {
        S3Path path = S3Path.parse("videos/sample.mp4", "default-bucket");

        assertEquals("default-bucket", path.bucket());
        assertEquals("videos/sample.mp4", path.key());
    }

}
