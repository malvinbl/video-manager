package es.mblcu.videomanager;

import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaConsumer;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaConsumerConfig;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaProducer;
import es.mblcu.videomanager.infrastructure.redis.JobStateRepositoryRedisAdapter;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ApplicationStartupTest {

    @Mock
    private ExtractFrameKafkaConsumer consumer;

    @Mock
    private ExtractFrameKafkaProducer producer;

    @Mock
    private JobStateRepositoryRedisAdapter repository;

    @Test
    void shouldStartApplicationFlowWithoutShutdownHookInSmokeMode() {
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

        when(repository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(CompletableFuture.completedFuture(List.of()));

        Application.run(config, consumer, producer, repository, false);

        verify(repository).findJobsByStatus(JobStatus.RUNNING);
        verify(consumer).start();
    }

}
