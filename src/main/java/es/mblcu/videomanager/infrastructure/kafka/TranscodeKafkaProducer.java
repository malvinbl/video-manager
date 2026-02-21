package es.mblcu.videomanager.infrastructure.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoResult;
import es.mblcu.videomanager.infrastructure.observability.Observability;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class TranscodeKafkaProducer implements AutoCloseable {

    private final String responseTopic;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public TranscodeKafkaProducer(TranscodeKafkaConfig config) {
        this(
            config.responseTopic(),
            new KafkaProducer<>(producerProperties(config.bootstrapServers())),
            new ObjectMapper()
        );
    }

    TranscodeKafkaProducer(String responseTopic, KafkaProducer<String, String> producer, ObjectMapper objectMapper) {
        this.responseTopic = responseTopic;
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<Void> publishSuccess(TranscodeVideoResult result) {
        final var message = new TranscodeResponseMessage(
            result.videoId(),
            result.outputs(),
            "success",
            null
        );
        return send(message);
    }

    public CompletableFuture<Void> publishError(TranscodeVideoCommand command, Throwable throwable) {
        String errorDescription = throwable == null ? "Unknown error" : throwable.getMessage();
        final var message = new TranscodeResponseMessage(
            command.videoId(),
            Map.of(),
            "ERROR",
            errorDescription
        );
        return send(message);
    }

    private CompletableFuture<Void> send(TranscodeResponseMessage responseMessage) {
        final var startedAt = Instant.now();
        final String payload;
        try {
            payload = objectMapper.writeValueAsString(responseMessage);
        } catch (JsonProcessingException ex) {
            Observability.incrementKafkaPublished("transcode", responseTopic, "error");
            Observability.recordExternalCallDuration("kafka", "publish_transcode_response", "error",
                Duration.between(startedAt, Instant.now()));
            return CompletableFuture.failedFuture(ex);
        }

        final var record = new ProducerRecord<>(
            responseTopic,
            String.valueOf(responseMessage.videoId()),
            payload
        );

        final var future = new CompletableFuture<Void>();
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                Observability.incrementKafkaPublished("transcode", responseTopic, "error");
                Observability.recordExternalCallDuration("kafka", "publish_transcode_response", "error",
                    Duration.between(startedAt, Instant.now()));
                future.completeExceptionally(exception);
                return;
            }
            Observability.incrementKafkaPublished("transcode", responseTopic, responseMessage.status());
            Observability.recordExternalCallDuration("kafka", "publish_transcode_response", "success",
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
