package es.mblcu.videomanager.infrastructure.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ExtractFrameKafkaProducerTest {

    private ObjectMapper objectMapper;

    @Mock
    private KafkaProducer<String, String> kafkaProducer;

    private ExtractFrameKafkaProducer producer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        producer = new ExtractFrameKafkaProducer("response-topic", kafkaProducer, objectMapper);
    }

    @Test
    void shouldPublishSuccessMessage() throws Exception {
        final var result = new ExtractFrameResult(42L, "s3://bucket/frames/frame.png", Duration.ofMillis(12));

        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, null);
            return null;
        }).when(kafkaProducer).send(any(ProducerRecord.class), any(Callback.class));

        producer.publishSuccess(result).join();

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture(), any(Callback.class));
        ProducerRecord<String, String> record = captor.getValue();

        assertEquals("response-topic", record.topic());
        assertEquals("42", record.key());

        final var json = objectMapper.readTree(record.value());

        assertEquals(42L, json.get("videoId").asLong());
        assertEquals("s3://bucket/frames/frame.png", json.get("frameS3Path").asText());
        assertEquals("success", json.get("status").asText());
    }

    @Test
    void shouldPublishErrorMessageWithUnknownErrorWhenThrowableIsNull() throws Exception {
        final var command = new ExtractFrameCommand(7L, "s3://bucket/videos/in.mp4", "s3://bucket/frames/f.png", 1.0);

        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, null);
            return null;
        }).when(kafkaProducer).send(any(ProducerRecord.class), any(Callback.class));

        producer.publishError(command, null).join();

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture(), any(Callback.class));

        final var json = objectMapper.readTree(captor.getValue().value());

        assertEquals("ERROR", json.get("status").asText());
        assertEquals("Unknown error", json.get("errorDescription").asText());
    }

    @Test
    void shouldCompleteExceptionallyWhenKafkaSendFails() {
        final var kafkaError = new RuntimeException("kafka down");

        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, kafkaError);
            return null;
        }).when(kafkaProducer).send(any(ProducerRecord.class), any(Callback.class));

        final var result = new ExtractFrameResult(42L, "s3://bucket/frames/frame.png", Duration.ofMillis(12));

        final var ex = assertThrows(CompletionException.class, () -> producer.publishSuccess(result).join());
        assertEquals(kafkaError, ex.getCause());
    }

}
