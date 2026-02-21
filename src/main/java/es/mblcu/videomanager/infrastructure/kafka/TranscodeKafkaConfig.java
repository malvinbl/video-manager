package es.mblcu.videomanager.infrastructure.kafka;

import es.mblcu.videomanager.infrastructure.config.AppProperties;

public record TranscodeKafkaConfig(
    String bootstrapServers,
    String groupId,
    String requestTopic,
    String responseTopic,
    String autoOffsetReset,
    long pollMillis
) {

    public static TranscodeKafkaConfig fromEnvironment() {
        final var properties = AppProperties.load();
        return new TranscodeKafkaConfig(
            properties.get("KAFKA_BOOTSTRAP_SERVERS", "kafka.bootstrap.servers", "localhost:9092"),
            properties.get("KAFKA_GROUP_ID_TRANSCODE", "kafka.group.id.transcode", "videomanager-transcode"),
            properties.get("KAFKA_TOPIC_TRANSCODE_REQUEST", "kafka.topic.transcode.request", "fe_transcode_request__norm"),
            properties.get("KAFKA_TOPIC_TRANSCODE_DONE", "kafka.topic.transcode.done", "fe_transcode_done__norm"),
            properties.get("KAFKA_AUTO_OFFSET_RESET", "kafka.auto-offset-reset", "latest"),
            properties.getLong("KAFKA_POLL_MILLIS", "kafka.poll.millis", 1000)
        );
    }

}
