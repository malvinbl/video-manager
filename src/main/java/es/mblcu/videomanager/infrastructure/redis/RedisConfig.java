package es.mblcu.videomanager.infrastructure.redis;

import es.mblcu.videomanager.infrastructure.config.AppProperties;

public record RedisConfig(String uri, String keyPrefix) {

    public static RedisConfig fromProperties() {
        final var properties = AppProperties.load();

        return new RedisConfig(
            properties.get("REDIS_URI", "redis.uri", "redis://localhost:6379"),
            properties.get("REDIS_KEY_PREFIX", "redis.key-prefix", "vm")
        );
    }

}
