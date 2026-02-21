package es.mblcu.videomanager.infrastructure.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoResult;
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
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TranscodeKafkaProducerTest {

    private ObjectMapper objectMapper;

    @Mock
    private KafkaProducer<String, String> kafkaProducer;

    private TranscodeKafkaProducer producer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        producer = new TranscodeKafkaProducer("transcode-response-topic", kafkaProducer, objectMapper);
    }

    @Test
    void shouldPublishSuccessMessage() throws Exception {
        final var result = new TranscodeVideoResult(
            42L,
            Map.of("854x480", "s3://bucket/transcoded/42/854x480.mp4"),
            Duration.ofMillis(120)
        );

        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, null);
            return null;
        }).when(kafkaProducer).send(any(ProducerRecord.class), any(Callback.class));

        producer.publishSuccess(result).join();

        final var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture(), any(Callback.class));
        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("transcode-response-topic");
        assertThat(record.key()).isEqualTo("42");

        final var json = objectMapper.readTree(record.value());

        assertThat(json.get("videoId").asLong()).isEqualTo(42L);
        assertThat(json.get("status").asText()).isEqualTo("success");
        assertThat(json.get("outputs").get("854x480").asText()).isEqualTo("s3://bucket/transcoded/42/854x480.mp4");
    }

    @Test
    void shouldPublishErrorMessage() throws Exception {
        final var command = new TranscodeVideoCommand(9L, "s3://bucket/videos/v.mp4", "s3://bucket/transcoded/9", 1280, 720);

        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, null);
            return null;
        }).when(kafkaProducer).send(any(ProducerRecord.class), any(Callback.class));

        producer.publishError(command, new RuntimeException("boom")).join();

        final var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture(), any(Callback.class));

        final var json = objectMapper.readTree((String) captor.getValue().value());

        assertThat(json.get("status").asText()).isEqualTo("ERROR");
        assertThat(json.get("errorDescription").asText()).isEqualTo("boom");
    }

    @Test
    void shouldCompleteExceptionallyWhenKafkaSendFails() {
        final var kafkaError = new RuntimeException("kafka down");

        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, kafkaError);
            return null;
        }).when(kafkaProducer).send(any(ProducerRecord.class), any(Callback.class));

        final var result = new TranscodeVideoResult(1L, Map.of(), Duration.ofMillis(1));

        assertThatThrownBy(() -> producer.publishSuccess(result).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(kafkaError);
    }

}
