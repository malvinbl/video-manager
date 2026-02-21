package es.mblcu.videomanager.infrastructure.kafka;

import es.mblcu.videomanager.application.usecase.TranscodeVideoUseCase;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TranscodeKafkaConsumerTest {

    @Mock
    private TranscodeVideoUseCase useCase;

    @Mock
    private TranscodeKafkaProducer kafkaProducer;

    private TranscodeKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        final var config = new TranscodeKafkaConfig(
            "localhost:9092",
            "videomanager-transcode-test",
            "transcode-request-topic",
            "transcode-response-topic",
            "latest",
            1000
        );
        consumer = new TranscodeKafkaConsumer(config, useCase, kafkaProducer);
    }

    @Test
    void shouldPublishSuccessWhenUseCaseCompletes() {
        String payload = """
            {
              "videoId": 3001,
              "videoS3Path": "s3://bucket/videos/in.mp4",
              "outputS3Prefix": "s3://bucket/transcoded/3001",
              "width": 1280,
              "height": 720
            }
            """;
        ConsumerRecord<String, String> record = new ConsumerRecord<>("transcode-request-topic", 0, 10L, "3001", payload);
        final var result = new TranscodeVideoResult(3001L, Map.of("854x480", "s3://bucket/transcoded/3001/854x480.mp4"), Duration.ofMillis(33));

        when(useCase.execute(any(TranscodeVideoCommand.class))).thenReturn(CompletableFuture.completedFuture(result));
        when(kafkaProducer.publishSuccess(result)).thenReturn(CompletableFuture.completedFuture(null));

        consumer.processRecordAsync(record).join();

        verify(kafkaProducer).publishSuccess(result);
        verify(kafkaProducer, never()).publishError(any(), any());
    }

    @Test
    void shouldPublishErrorWhenUseCaseFails() {
        String payload = """
            {
              "videoId": 3002,
              "videoS3Path": "s3://bucket/videos/in.mp4",
              "outputS3Prefix": "s3://bucket/transcoded/3002",
              "width": 999,
              "height": 999
            }
            """;
        ConsumerRecord<String, String> record = new ConsumerRecord<>("transcode-request-topic", 1, 20L, "3002", payload);
        final var processingError = new RuntimeException("validation failed");

        when(useCase.execute(any(TranscodeVideoCommand.class))).thenReturn(CompletableFuture.failedFuture(processingError));
        when(kafkaProducer.publishError(any(TranscodeVideoCommand.class), any(Throwable.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        consumer.processRecordAsync(record).join();

        final var commandCaptor = ArgumentCaptor.forClass(TranscodeVideoCommand.class);
        final var throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(kafkaProducer).publishError(commandCaptor.capture(), throwableCaptor.capture());
        assertThat(commandCaptor.getValue().videoId()).isEqualTo(3002L);
        assertThat(throwableCaptor.getValue()).isEqualTo(processingError);
    }

    @Test
    void shouldPublishErrorWhenPayloadIsInvalidJson() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("transcode-request-topic", 0, 30L, "3010", "not-json");

        when(kafkaProducer.publishError(any(TranscodeVideoCommand.class), any(Throwable.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        consumer.processRecordAsync(record).join();

        final var commandCaptor = ArgumentCaptor.forClass(TranscodeVideoCommand.class);
        verify(kafkaProducer).publishError(commandCaptor.capture(), any(Throwable.class));
        assertThat(commandCaptor.getValue().videoId()).isEqualTo(3010L);
        verify(useCase, never()).execute(any());
    }

}
