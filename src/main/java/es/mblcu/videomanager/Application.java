package es.mblcu.videomanager;

import es.mblcu.videomanager.application.usecase.ExtractFrameUseCase;
import es.mblcu.videomanager.application.service.LocalWorkspaceService;
import es.mblcu.videomanager.application.usecase.StartupRecoveryUseCase;
import es.mblcu.videomanager.infrastructure.ffmpeg.FfmpegFrameExtractionAdapter;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaConsumer;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaConsumerConfig;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaProducer;
import es.mblcu.videomanager.infrastructure.redis.RedisConfig;
import es.mblcu.videomanager.infrastructure.redis.JobStateRepositoryRedisAdapter;
import es.mblcu.videomanager.infrastructure.s3.FileRepositoryS3Adapter;
import es.mblcu.videomanager.infrastructure.s3.S3StorageConfig;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.time.Duration;

@Slf4j
public final class Application {

    public static void main(String[] args) {
        final var config = ExtractFrameKafkaConsumerConfig.fromEnvironment();
        final var ffmpegAdapter =
            new FfmpegFrameExtractionAdapter(config.ffmpegBinary(), Duration.ofSeconds(config.ffmpegTimeoutSeconds()));
        final var s3Adapter = new FileRepositoryS3Adapter(S3StorageConfig.fromEnvironment());
        final var localWorkspaceService = new LocalWorkspaceService(Path.of(config.localWorkspaceDir()));
        final var jobStateRepositoryRedisAdapter = new JobStateRepositoryRedisAdapter(RedisConfig.fromProperties());
        final var useCase = new ExtractFrameUseCase(ffmpegAdapter, s3Adapter, localWorkspaceService,
            jobStateRepositoryRedisAdapter);
        final var producer = new ExtractFrameKafkaProducer(config);
        final var consumer = new ExtractFrameKafkaConsumer(config, useCase, producer);

        run(config, consumer, producer, jobStateRepositoryRedisAdapter, true);
    }

    static void run(
        ExtractFrameKafkaConsumerConfig config,
        ExtractFrameKafkaConsumer consumer,
        ExtractFrameKafkaProducer producer,
        JobStateRepositoryRedisAdapter jobStateRepositoryRedisAdapter,
        boolean registerShutdownHook
    ) {
        if (registerShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Application shutdown requested");
                consumer.stop();
                producer.close();
                jobStateRepositoryRedisAdapter.close();
            }));
        }

        final var recovered = new StartupRecoveryUseCase(jobStateRepositoryRedisAdapter).recoverRunningJobs().join();
        if (recovered > 0) {
            log.warn("Startup recovery completed. jobsRecovered={}", recovered);
        }

        log.info(
            "=== Application started === requestTopic={}, responseTopic={}, bootstrapServers={}",
            config.requestTopic(),
            config.responseTopic(),
            config.bootstrapServers()
        );

        consumer.start();
    }

}
