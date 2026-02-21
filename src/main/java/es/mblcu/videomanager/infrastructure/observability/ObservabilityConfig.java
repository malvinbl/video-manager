package es.mblcu.videomanager.infrastructure.observability;

import es.mblcu.videomanager.infrastructure.config.AppProperties;

public record ObservabilityConfig(
    int port,
    String bindAddress
) {

    public static ObservabilityConfig fromProperties() {
        final var properties = AppProperties.load();
        return new ObservabilityConfig(
            (int) properties.getLong("OBSERVABILITY_PORT", "observability.port", 8081),
            properties.get("OBSERVABILITY_BIND_ADDRESS", "observability.bind-address", "0.0.0.0")
        );
    }

}
