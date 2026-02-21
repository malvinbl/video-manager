package es.mblcu.videomanager.infrastructure.kafka;

import es.mblcu.videomanager.application.usecase.ExtractFrameUseCase;
import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtractFrameKafkaConsumerTest {

    @Mock
    private ExtractFrameUseCase useCase;

    @Mock
    private ExtractFrameKafkaProducer kafkaProducer;

    private ExtractFrameKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        final var config = new ExtractFrameKafkaConsumerConfig(
            "localhost:9092",
            "videomanager-test",
            "request-topic",
            "response-topic",
            "latest",
            1000,
            "ffmpeg",
            60,
            ".videomanager-work"
        );
        consumer = new ExtractFrameKafkaConsumer(config, useCase, kafkaProducer);
    }

    @Test
    void should_publish_success_when_use_case_completes() {
        String payload = """
            {
              "videoId": 1001,
              "videoS3Path": "s3://bucket/videos/in.mp4",
              "frameS3Path": "s3://bucket/frames/out.png",
              "second": 1.5
            }
            """;
        ConsumerRecord<String, String> record = new ConsumerRecord<>("request-topic", 0, 10L, "1001", payload);
        final var result = new ExtractFrameResult(1001L, "s3://bucket/frames/out.png", Duration.ofMillis(30));

        when(useCase.execute(any(ExtractFrameCommand.class))).thenReturn(CompletableFuture.completedFuture(result));
        when(kafkaProducer.publishSuccess(result)).thenReturn(CompletableFuture.completedFuture(null));

        consumer.processRecordAsync(record).join();

        verify(kafkaProducer).publishSuccess(result);
        verify(kafkaProducer, never()).publishError(any(), any());
    }

    @Test
    void should_publish_error_when_use_case_fails() {
        String payload = """
            {
              "videoId": 1002,
              "videoS3Path": "s3://bucket/videos/in.mp4",
              "frameS3Path": "s3://bucket/frames/out.png"
            }
            """;
        ConsumerRecord<String, String> record = new ConsumerRecord<>("request-topic", 1, 20L, "1002", payload);
        final var processingError = new RuntimeException("processing failed");

        when(useCase.execute(any(ExtractFrameCommand.class))).thenReturn(CompletableFuture.failedFuture(processingError));
        when(kafkaProducer.publishError(any(ExtractFrameCommand.class), any(Throwable.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        consumer.processRecordAsync(record).join();

        ArgumentCaptor<ExtractFrameCommand> commandCaptor = ArgumentCaptor.forClass(ExtractFrameCommand.class);
        ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(kafkaProducer).publishError(commandCaptor.capture(), throwableCaptor.capture());
        assertThat(commandCaptor.getValue().videoId()).isEqualTo(1002L);
        assertThat(throwableCaptor.getValue()).isEqualTo(processingError);
    }

    @Test
    void should_fail_when_payload_is_invalid_json() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("request-topic", 0, 30L, "1003", "not-json");

        assertThatThrownBy(() -> consumer.processRecordAsync(record).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(IllegalArgumentException.class);
        verify(useCase, never()).execute(any());
        verify(kafkaProducer, never()).publishSuccess(any());
        verify(kafkaProducer, never()).publishError(any(), any());
    }

}
