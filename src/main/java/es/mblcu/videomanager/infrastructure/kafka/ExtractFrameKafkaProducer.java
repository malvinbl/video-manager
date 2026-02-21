package es.mblcu.videomanager.infrastructure.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import es.mblcu.videomanager.infrastructure.observability.Observability;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.time.Duration;
import java.time.Instant;

public class ExtractFrameKafkaProducer implements AutoCloseable {

    private final String responseTopic;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public ExtractFrameKafkaProducer(ExtractFrameKafkaConsumerConfig config) {
        this(
            config.responseTopic(),
            new KafkaProducer<>(producerProperties(config.bootstrapServers())),
            new ObjectMapper()
        );
    }

    ExtractFrameKafkaProducer(String responseTopic, KafkaProducer<String, String> producer, ObjectMapper objectMapper) {
        this.responseTopic = responseTopic;
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<Void> publishSuccess(ExtractFrameResult result) {
        final var message = new ExtractFrameResponseMessage(
            result.videoId(),
            result.frameS3Path(),
            "success",
            null
        );
        return send(message);
    }

    public CompletableFuture<Void> publishError(ExtractFrameCommand command, Throwable throwable) {
        String message = throwable == null ? "Unknown error" : throwable.getMessage();

        final var responseMessage = new ExtractFrameResponseMessage(
            command.videoId(),
            command.frameS3Path(),
            "ERROR",
            message
        );

        return send(responseMessage);
    }

    private CompletableFuture<Void> send(ExtractFrameResponseMessage responseMessage) {
        final var startedAt = Instant.now();
        String payload;
        try {
            payload = objectMapper.writeValueAsString(responseMessage);
        } catch (JsonProcessingException ex) {
            Observability.incrementKafkaPublished("frame", responseTopic, "error");
            Observability.recordExternalCallDuration("kafka", "publish_frame_response", "error",
                Duration.between(startedAt, Instant.now()));
            return CompletableFuture.failedFuture(ex);
        }

        String key = String.valueOf(responseMessage.videoId());
        ProducerRecord<String, String> record = new ProducerRecord<>(responseTopic, key, payload);

        CompletableFuture<Void> future = new CompletableFuture<>();
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                Observability.incrementKafkaPublished("frame", responseTopic, "error");
                Observability.recordExternalCallDuration("kafka", "publish_frame_response", "error",
                    Duration.between(startedAt, Instant.now()));
                future.completeExceptionally(exception);
                return;
            }
            Observability.incrementKafkaPublished("frame", responseTopic, responseMessage.status());
            Observability.recordExternalCallDuration("kafka", "publish_frame_response", "success",
                Duration.between(startedAt, Instant.now()));
            future.complete(null);
        });

        return future;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private static Properties producerProperties(String bootstrapServers) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

}
