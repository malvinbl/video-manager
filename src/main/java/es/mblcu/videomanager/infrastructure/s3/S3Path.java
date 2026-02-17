package es.mblcu.videomanager.infrastructure.s3;

import org.apache.commons.lang3.StringUtils;

public record S3Path(String bucket, String key) {

    public static S3Path parse(String raw, String defaultBucket) {
        if (StringUtils.isEmpty(raw)) {
            throw new IllegalArgumentException("S3 path cannot be null or blank");
        }

        if (raw.startsWith("s3://")) {
            String withoutScheme = raw.substring("s3://".length());
            int slash = withoutScheme.indexOf('/');

            if (slash <= 0 || slash == withoutScheme.length() - 1) {
                throw new IllegalArgumentException("Invalid S3 URI: " + raw);
            }

            return new S3Path(withoutScheme.substring(0, slash), withoutScheme.substring(slash + 1));
        }

        String key = raw.startsWith("/") ? raw.substring(1) : raw;

        return new S3Path(defaultBucket, key);
    }

}
