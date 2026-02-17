package es.mblcu.videomanager;

import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaConsumer;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaConsumerConfig;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaProducer;
import es.mblcu.videomanager.infrastructure.redis.JobStateRepositoryRedisAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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

        Application.run(config, consumer, producer, repository, false);

        verify(consumer).start();
        verifyNoInteractions(producer, repository);
    }

}
