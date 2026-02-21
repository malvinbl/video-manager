package es.mblcu.videomanager.infrastructure.observability;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import static org.assertj.core.api.Assertions.assertThat;

class ObservabilityTest {

    @BeforeEach
    void setUp() {
        Observability.shutdown();
        Observability.markLive(false);
        Observability.markReady(false);
        Observability.markStarted(false);
    }

    @Test
    void should_report_health_as_down_when_flags_are_not_marked_up() {
        assertThat(Observability.healthStatusCodeForProbe("all")).isEqualTo(503);
        assertThat(Observability.healthStatusCodeForProbe("liveness")).isEqualTo(503);
        assertThat(Observability.healthStatusCodeForProbe("readiness")).isEqualTo(503);
        assertThat(Observability.healthStatusCodeForProbe("startup")).isEqualTo(503);
        assertThat(Observability.healthPayloadForProbe("all"))
            .containsEntry("status", "DOWN")
            .containsEntry("liveness", "DOWN")
            .containsEntry("readiness", "DOWN")
            .containsEntry("startup", "DOWN");
    }

    @Test
    void should_report_health_as_up_when_all_flags_are_marked_up() {
        Observability.markLive(true);
        Observability.markReady(true);
        Observability.markStarted(true);

        assertThat(Observability.healthStatusCodeForProbe("all")).isEqualTo(200);
        assertThat(Observability.healthStatusCodeForProbe("liveness")).isEqualTo(200);
        assertThat(Observability.healthStatusCodeForProbe("readiness")).isEqualTo(200);
        assertThat(Observability.healthStatusCodeForProbe("startup")).isEqualTo(200);
        assertThat(Observability.healthPayloadForProbe("all"))
            .containsEntry("status", "UP")
            .containsEntry("liveness", "UP")
            .containsEntry("readiness", "UP")
            .containsEntry("startup", "UP");
    }

    @Test
    void should_expose_metrics_with_expected_names_and_tags() {
        Observability.incrementKafkaConsumed("frame", "frame-request-topic", "success");
        Observability.incrementKafkaPublished("transcode", "transcode-response-topic", "ERROR");
        Observability.incrementJobs("frame", "success");
        Observability.incrementStartupRecoveredJobs("frame", 2);
        Observability.recordProcessingDuration("frame", "extract_frame", "success", Duration.ofMillis(25));
        Observability.recordExternalCallDuration("s3", "download", "success", Duration.ofMillis(5));

        final var metrics = Observability.scrapeMetricsForTests();

        assertThat(metrics).contains("videomanager_health_status");
        assertThat(metrics).contains("videomanager_kafka_messages_consumed_total");
        assertThat(metrics).contains("flow=\"frame\"");
        assertThat(metrics).contains("topic=\"frame-request-topic\"");
        assertThat(metrics).contains("result=\"success\"");
        assertThat(metrics).contains("videomanager_kafka_messages_published_total");
        assertThat(metrics).contains("status=\"ERROR\"");
        assertThat(metrics).contains("videomanager_jobs_total");
        assertThat(metrics).contains("videomanager_startup_recovered_jobs_total");
        assertThat(metrics).contains("videomanager_processing_duration_seconds_count");
        assertThat(metrics).contains("operation=\"extract_frame\"");
        assertThat(metrics).contains("videomanager_external_call_duration_seconds_count");
        assertThat(metrics).contains("component=\"s3\"");
    }

}
