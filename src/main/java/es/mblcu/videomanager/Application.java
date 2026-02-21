package es.mblcu.videomanager;

import es.mblcu.videomanager.application.usecase.ExtractFrameUseCase;
import es.mblcu.videomanager.application.service.LocalWorkspaceService;
import es.mblcu.videomanager.application.usecase.StartupRecoveryUseCase;
import es.mblcu.videomanager.application.usecase.TranscodeStartupRecoveryUseCase;
import es.mblcu.videomanager.application.usecase.TranscodeVideoUseCase;
import es.mblcu.videomanager.infrastructure.ffmpeg.FfmpegFrameExtractionAdapter;
import es.mblcu.videomanager.infrastructure.ffmpeg.FfmpegVideoTranscodingAdapter;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaConsumer;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaConsumerConfig;
import es.mblcu.videomanager.infrastructure.kafka.ExtractFrameKafkaProducer;
import es.mblcu.videomanager.infrastructure.kafka.TranscodeKafkaConfig;
import es.mblcu.videomanager.infrastructure.kafka.TranscodeKafkaConsumer;
import es.mblcu.videomanager.infrastructure.kafka.TranscodeKafkaProducer;
import es.mblcu.videomanager.infrastructure.observability.Observability;
import es.mblcu.videomanager.infrastructure.observability.ObservabilityConfig;
import es.mblcu.videomanager.infrastructure.redis.RedisConfig;
import es.mblcu.videomanager.infrastructure.redis.JobStateRepositoryRedisAdapter;
import es.mblcu.videomanager.infrastructure.redis.TranscodeJobStateRepositoryRedisAdapter;
import es.mblcu.videomanager.infrastructure.s3.FileRepositoryS3Adapter;
import es.mblcu.videomanager.infrastructure.s3.S3StorageConfig;
import es.mblcu.videomanager.infrastructure.transcode.TranscodeProfileCatalogFactory;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.time.Duration;

@Slf4j
public final class Application {

    public static void main(String[] args) {
        final var observabilityConfig = ObservabilityConfig.fromProperties();
        Observability.initialize(observabilityConfig);
        Observability.markLive(true);

        final var config = ExtractFrameKafkaConsumerConfig.fromEnvironment();
        final var transcodeKafkaConfig = TranscodeKafkaConfig.fromEnvironment();
        final var ffmpegAdapter =
            new FfmpegFrameExtractionAdapter(config.ffmpegBinary(), Duration.ofSeconds(config.ffmpegTimeoutSeconds()));
        final var ffmpegTranscodingAdapter =
            new FfmpegVideoTranscodingAdapter(config.ffmpegBinary(), Duration.ofSeconds(config.ffmpegTimeoutSeconds()));
        final var s3Adapter = new FileRepositoryS3Adapter(S3StorageConfig.fromEnvironment());
        final var localWorkspaceService = new LocalWorkspaceService(Path.of(config.localWorkspaceDir()));
        final var jobStateRepositoryRedisAdapter = new JobStateRepositoryRedisAdapter(RedisConfig.fromProperties());
        final var transcodeJobStateRepositoryRedisAdapter = new TranscodeJobStateRepositoryRedisAdapter(RedisConfig.fromProperties());
        final var useCase = new ExtractFrameUseCase(ffmpegAdapter, s3Adapter, localWorkspaceService,
            jobStateRepositoryRedisAdapter);
        final var transcodeUseCase = new TranscodeVideoUseCase(
            ffmpegTranscodingAdapter,
            s3Adapter,
            localWorkspaceService,
            jobStateRepositoryRedisAdapter,
            transcodeJobStateRepositoryRedisAdapter,
            TranscodeProfileCatalogFactory.fromProperties()
        );
        final var producer = new ExtractFrameKafkaProducer(config);
        final var consumer = new ExtractFrameKafkaConsumer(config, useCase, producer);
        final var transcodeProducer = new TranscodeKafkaProducer(transcodeKafkaConfig);
        final var transcodeConsumer = new TranscodeKafkaConsumer(transcodeKafkaConfig, transcodeUseCase, transcodeProducer);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Observability.markReady(false);
            Observability.markLive(false);
            transcodeConsumer.stop();
            transcodeProducer.close();
            transcodeJobStateRepositoryRedisAdapter.close();
            Observability.shutdown();
        }));

        final var recoveredTranscode = new TranscodeStartupRecoveryUseCase(transcodeJobStateRepositoryRedisAdapter)
            .recoverRunningJobs()
            .join();
        Observability.incrementStartupRecoveredJobs("transcode", recoveredTranscode);
        if (recoveredTranscode > 0) {
            log.warn("Startup recovery completed for transcode. jobsRecovered={}", recoveredTranscode);
        }

        Thread.startVirtualThread(transcodeConsumer::start);

        run(config, transcodeKafkaConfig, consumer, producer, jobStateRepositoryRedisAdapter, true);
    }

    static void run(
        ExtractFrameKafkaConsumerConfig config,
        TranscodeKafkaConfig transcodeConfig,
        ExtractFrameKafkaConsumer consumer,
        ExtractFrameKafkaProducer producer,
        JobStateRepositoryRedisAdapter jobStateRepositoryRedisAdapter,
        boolean registerShutdownHook
    ) {
        if (registerShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Application shutdown requested");
                Observability.markReady(false);
                Observability.markLive(false);
                consumer.stop();
                producer.close();
                jobStateRepositoryRedisAdapter.close();
            }));
        }

        final var recovered = new StartupRecoveryUseCase(jobStateRepositoryRedisAdapter).recoverRunningJobs().join();
        Observability.incrementStartupRecoveredJobs("frame", recovered);
        if (recovered > 0) {
            log.warn("Startup recovery completed. jobsRecovered={}", recovered);
        }

        log.info(
            "=== Application started ===\n "
                + "frameRequestTopic = {},\n "
                + "frameResponseTopic = {},\n "
                + "transcodeRequestTopic = {},\n "
                + "transcodeResponseTopic = {},\n "
                + "bootstrapServers = {}",
            config.requestTopic(),
            config.responseTopic(),
            transcodeConfig.requestTopic(),
            transcodeConfig.responseTopic(),
            config.bootstrapServers()
        );

        Observability.markStarted(true);
        Observability.markReady(true);

        consumer.start();
    }

}
