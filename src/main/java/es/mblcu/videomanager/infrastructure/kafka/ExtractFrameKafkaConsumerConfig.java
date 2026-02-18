package es.mblcu.videomanager.infrastructure.kafka;

import es.mblcu.videomanager.infrastructure.config.AppProperties;

public record ExtractFrameKafkaConsumerConfig(
    String bootstrapServers,
    String groupId,
    String requestTopic,
    String responseTopic,
    String autoOffsetReset,
    long pollMillis,
    String ffmpegBinary,
    long ffmpegTimeoutSeconds,
    String localWorkspaceDir
) {

    public static ExtractFrameKafkaConsumerConfig fromEnvironment() {
        var properties = AppProperties.load();
        return new ExtractFrameKafkaConsumerConfig(
            properties.get("KAFKA_BOOTSTRAP_SERVERS", "kafka.bootstrap.servers", "localhost:9092"),
            properties.get("KAFKA_GROUP_ID", "kafka.group.id", "videomanager-extract-frame"),
            properties.get("KAFKA_TOPIC_EXTRACT_FRAME_REQUEST", "kafka.topic.extract-frame.request", "fe_sample_frame_request__norm"),
            properties.get("KAFKA_TOPIC_EXTRACT_FRAME_DONE", "kafka.topic.extract-frame.done", "fe_sample_frame_done__norm"),
            properties.get("KAFKA_AUTO_OFFSET_RESET", "kafka.auto-offset-reset", "latest"),
            properties.getLong("KAFKA_POLL_MILLIS", "kafka.poll.millis", 1000),
            properties.get("FFMPEG_BIN", "ffmpeg.bin", "ffmpeg"),
            properties.getLong("FFMPEG_TIMEOUT_SECONDS", "ffmpeg.timeout.seconds", 60),
            properties.get("LOCAL_WORKSPACE_DIR", "local.workspace.dir", ".videomanager-work")
        );
    }

}
