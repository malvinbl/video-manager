package es.mblcu.videomanager.infrastructure.observability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

@Slf4j
public final class Observability {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HealthState HEALTH_STATE = new HealthState();
    private static final PrometheusMeterRegistry REGISTRY = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private static volatile HttpServer server;

    static {
        Gauge.builder("videomanager_health_status", HEALTH_STATE, state -> state.isLivenessUp() ? 1 : 0)
            .description("1 if liveness is UP, 0 otherwise")
            .tag("probe", "liveness")
            .register(REGISTRY);
        Gauge.builder("videomanager_health_status", HEALTH_STATE, state -> state.isReadinessUp() ? 1 : 0)
            .description("1 if readiness is UP, 0 otherwise")
            .tag("probe", "readiness")
            .register(REGISTRY);
        Gauge.builder("videomanager_health_status", HEALTH_STATE, state -> state.isStartupUp() ? 1 : 0)
            .description("1 if startup is complete, 0 otherwise")
            .tag("probe", "startup")
            .register(REGISTRY);
    }

    public static synchronized void initialize(ObservabilityConfig config) {
        if (server != null) {
            return;
        }

        try {
            server = HttpServer.create(new InetSocketAddress(config.bindAddress(), config.port()), 0);
            server.createContext("/metrics", Observability::handleMetrics);
            server.createContext("/health", exchange -> handleHealth(exchange, "all"));
            server.createContext("/health/live", exchange -> handleHealth(exchange, "liveness"));
            server.createContext("/health/ready", exchange -> handleHealth(exchange, "readiness"));
            server.createContext("/health/startup", exchange -> handleHealth(exchange, "startup"));
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.start();
            log.info("Observability HTTP server started at {}:{}", config.bindAddress(), config.port());
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot start observability HTTP server", ex);
        }
    }

    public static synchronized void shutdown() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    public static void markLive(boolean value) {
        HEALTH_STATE.markLive(value);
    }

    public static void markReady(boolean value) {
        HEALTH_STATE.markReady(value);
    }

    public static void markStarted(boolean value) {
        HEALTH_STATE.markStarted(value);
    }

    public static void incrementKafkaConsumed(String flow, String topic, String result) {
        Counter.builder("videomanager_kafka_messages_consumed_total")
            .description("Kafka messages consumed by this service")
            .tag("flow", flow)
            .tag("topic", topic)
            .tag("result", result)
            .register(REGISTRY)
            .increment();
    }

    public static void incrementKafkaPublished(String flow, String topic, String status) {
        Counter.builder("videomanager_kafka_messages_published_total")
            .description("Kafka messages published by this service")
            .tag("flow", flow)
            .tag("topic", topic)
            .tag("status", status)
            .register(REGISTRY)
            .increment();
    }

    public static void incrementJobs(String flow, String status) {
        Counter.builder("videomanager_jobs_total")
            .description("Jobs processed by service")
            .tag("flow", flow)
            .tag("status", status)
            .register(REGISTRY)
            .increment();
    }

    public static void incrementStartupRecoveredJobs(String flow, int value) {
        Counter.builder("videomanager_startup_recovered_jobs_total")
            .description("Jobs recovered in startup")
            .tag("flow", flow)
            .register(REGISTRY)
            .increment(value);
    }

    public static void recordProcessingDuration(String flow, String operation, String status, Duration duration) {
        Timer.builder("videomanager_processing_duration")
            .description("Main processing duration")
            .tag("flow", flow)
            .tag("operation", operation)
            .tag("status", status)
            .register(REGISTRY)
            .record(duration);
    }

    public static void recordExternalCallDuration(String component, String operation, String status, Duration duration) {
        Timer.builder("videomanager_external_call_duration")
            .description("External call duration")
            .tag("component", component)
            .tag("operation", operation)
            .tag("status", status)
            .register(REGISTRY)
            .record(duration);
    }

    static int healthStatusCodeForProbe(String probe) {
        return isProbeUp(probe) ? 200 : 503;
    }

    static Map<String, Object> healthPayloadForProbe(String probe) {
        boolean live = HEALTH_STATE.isLivenessUp();
        boolean ready = HEALTH_STATE.isReadinessUp();
        boolean startup = HEALTH_STATE.isStartupUp();
        boolean up = isProbeUp(probe);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", up ? "UP" : "DOWN");
        payload.put("liveness", live ? "UP" : "DOWN");
        payload.put("readiness", ready ? "UP" : "DOWN");
        payload.put("startup", startup ? "UP" : "DOWN");
        return payload;
    }

    static String scrapeMetricsForTests() {
        return REGISTRY.scrape();
    }

    private static void handleMetrics(HttpExchange exchange) throws IOException {
        byte[] response = REGISTRY.scrape().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private static void handleHealth(HttpExchange exchange, String probe) throws IOException {
        int statusCode = healthStatusCodeForProbe(probe);
        Map<String, Object> payload = healthPayloadForProbe(probe);

        byte[] response = OBJECT_MAPPER.writeValueAsBytes(payload);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private static boolean isProbeUp(String probe) {
        boolean live = HEALTH_STATE.isLivenessUp();
        boolean ready = HEALTH_STATE.isReadinessUp();
        boolean startup = HEALTH_STATE.isStartupUp();
        return switch (probe) {
            case "liveness" -> live;
            case "readiness" -> ready;
            case "startup" -> startup;
            default -> live && ready && startup;
        };
    }

}
