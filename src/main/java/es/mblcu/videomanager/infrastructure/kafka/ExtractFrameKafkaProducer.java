package es.mblcu.videomanager.infrastructure.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class ExtractFrameKafkaProducer implements AutoCloseable {

    private final String responseTopic;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ExtractFrameKafkaProducer(ExtractFrameKafkaConsumerConfig config) {
        this.responseTopic = config.responseTopic();
        this.producer = new KafkaProducer<>(producerProperties(config.bootstrapServers()));
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
        String payload;
        try {
            payload = objectMapper.writeValueAsString(responseMessage);
        } catch (JsonProcessingException ex) {
            return CompletableFuture.failedFuture(ex);
        }

        String key = String.valueOf(responseMessage.videoId());
        ProducerRecord<String, String> record = new ProducerRecord<>(responseTopic, key, payload);

        CompletableFuture<Void> future = new CompletableFuture<>();
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(exception);
                return;
            }
            future.complete(null);
        });

        return future;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private Properties producerProperties(String bootstrapServers) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

}
