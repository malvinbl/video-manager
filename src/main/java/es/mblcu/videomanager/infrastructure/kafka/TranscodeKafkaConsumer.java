package es.mblcu.videomanager.infrastructure.kafka;

import es.mblcu.videomanager.application.usecase.TranscodeVideoUseCase;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoResult;
import es.mblcu.videomanager.infrastructure.observability.Observability;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TranscodeKafkaConsumer {

    private final TranscodeKafkaConfig config;
    private final TranscodeVideoUseCase useCase;
    private final TranscodeKafkaProducer kafkaProducer;
    private final TranscodeMessageMapper messageMapper;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final Queue<OffsetAck> offsetAcks = new ConcurrentLinkedQueue<>();
    private final Map<TopicPartition, CompletableFuture<Void>> partitionChains = new ConcurrentHashMap<>();

    public TranscodeKafkaConsumer(
        TranscodeKafkaConfig config,
        TranscodeVideoUseCase useCase,
        TranscodeKafkaProducer kafkaProducer) {

        this(config, useCase, kafkaProducer, new TranscodeMessageMapper());
    }

    TranscodeKafkaConsumer(
        TranscodeKafkaConfig config,
        TranscodeVideoUseCase useCase,
        TranscodeKafkaProducer kafkaProducer,
        TranscodeMessageMapper messageMapper) {

        this.config = config;
        this.useCase = useCase;
        this.kafkaProducer = kafkaProducer;
        this.messageMapper = messageMapper;
    }

    public void start() {
        var props = new java.util.Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetReset());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(config.requestTopic()));

            while (running.get() || inFlight.get() > 0) {
                ConsumerRecords<String, String> records = running.get()
                    ? consumer.poll(Duration.ofMillis(config.pollMillis()))
                    : ConsumerRecords.empty();

                for (ConsumerRecord<String, String> record : records) {
                    dispatchRecord(record);
                }

                commitCompletedOffsets(consumer);
            }

            commitCompletedOffsets(consumer);
        }
    }

    public void stop() {
        running.set(false);
    }

    private void dispatchRecord(ConsumerRecord<String, String> record) {
        final var topicPartition = new TopicPartition(record.topic(), record.partition());

        partitionChains.compute(topicPartition, (tp, previous) -> {
            CompletableFuture<Void> chainBase = previous == null
                ? CompletableFuture.completedFuture(null)
                : previous.handle((ignored, throwable) -> null);

            inFlight.incrementAndGet();

            return chainBase
                .thenCompose(ignored -> processRecordAsync(record))
                .whenComplete((ignored, throwable) -> inFlight.decrementAndGet());
        });
    }

    CompletableFuture<Void> processRecordAsync(ConsumerRecord<String, String> record) {
        final var startedAt = Instant.now();
        final TranscodeVideoCommand command;
        try {
            command = messageMapper.toCommand(record.value());
        } catch (Exception ex) {
            Observability.incrementKafkaConsumed("transcode", record.topic(), "parse_error");
            Observability.recordProcessingDuration(
                "transcode",
                "transcode_video",
                "error",
                Duration.between(startedAt, Instant.now())
            );
            final var fallback = new TranscodeVideoCommand(
                parseKey(record.key()),
                "unknown",
                "unknown",
                1,
                1
            );
            return onError(record, fallback, ex, startedAt);
        }

        return useCase.execute(command)
            .thenCompose(result -> onSuccess(record, result, startedAt))
            .exceptionallyCompose(ex -> onError(record, command, ex, startedAt));
    }

    private CompletableFuture<Void> onSuccess(ConsumerRecord<String, String> record, TranscodeVideoResult result, Instant startedAt) {
        return kafkaProducer.publishSuccess(result)
            .thenRun(() -> {
                Observability.incrementKafkaConsumed("transcode", record.topic(), "success");
                Observability.incrementJobs("transcode", "success");
                Observability.recordProcessingDuration(
                    "transcode",
                    "transcode_video",
                    "success",
                    Duration.between(startedAt, Instant.now())
                );
                offsetAcks.add(new OffsetAck(record.topic(), record.partition(), record.offset() + 1));
            });
    }

    private CompletableFuture<Void> onError(ConsumerRecord<String, String> record, TranscodeVideoCommand command, Throwable ex, Instant startedAt) {
        final var cause = ex.getCause() != null ? ex.getCause() : ex;

        return kafkaProducer.publishError(command, cause)
            .thenRun(() -> {
                Observability.incrementKafkaConsumed("transcode", record.topic(), "error");
                Observability.incrementJobs("transcode", "error");
                Observability.recordProcessingDuration(
                    "transcode",
                    "transcode_video",
                    "error",
                    Duration.between(startedAt, Instant.now())
                );
                offsetAcks.add(new OffsetAck(record.topic(), record.partition(), record.offset() + 1));
            });
    }

    private void commitCompletedOffsets(KafkaConsumer<String, String> consumer) {
        final var commitMap = new HashMap<TopicPartition, OffsetAndMetadata>();

        OffsetAck ack;
        while ((ack = offsetAcks.poll()) != null) {
            final var tp = new TopicPartition(ack.topic(), ack.partition());
            final var current = commitMap.get(tp);

            if (current == null || ack.nextOffset() > current.offset()) {
                commitMap.put(tp, new OffsetAndMetadata(ack.nextOffset()));
            }
        }

        if (!commitMap.isEmpty()) {
            consumer.commitAsync(commitMap, null);
        }
    }

    private record OffsetAck(String topic, int partition, long nextOffset) {
    }

    private long parseKey(String key) {
        if (key == null) {
            return -1L;
        }
        try {
            return Long.parseLong(key);
        } catch (NumberFormatException ignored) {
            return -1L;
        }
    }

}
