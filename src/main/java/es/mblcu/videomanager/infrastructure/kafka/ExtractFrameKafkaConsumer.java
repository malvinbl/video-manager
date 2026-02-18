package es.mblcu.videomanager.infrastructure.kafka;

import es.mblcu.videomanager.application.usecase.ExtractFrameUseCase;
import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor
public class ExtractFrameKafkaConsumer {

    private final ExtractFrameKafkaConsumerConfig config;
    private final ExtractFrameUseCase useCase;
    private final ExtractFrameKafkaProducer kafkaProducer;
    private final ExtractFrameMessageMapper messageMapper = new ExtractFrameMessageMapper();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final Queue<OffsetAck> offsetAcks = new ConcurrentLinkedQueue<>();
    private final Map<TopicPartition, CompletableFuture<Void>> partitionChains = new ConcurrentHashMap<>();

    public void start() {
        var props = new Properties();
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
            CompletableFuture<Void> chainBase = previous == null ? CompletableFuture.completedFuture(null) : previous;
            inFlight.incrementAndGet();

            return chainBase
                .thenCompose(ignored -> processRecordAsync(record))
                .whenComplete((ignored, throwable) -> inFlight.decrementAndGet());
        });
    }

    private CompletableFuture<Void> processRecordAsync(ConsumerRecord<String, String> record) {
        final ExtractFrameCommand command;
        try {
            command = messageMapper.toCommand(record.value());
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }

        return useCase.execute(command)
            .thenCompose(result -> onSuccess(record, result))
            .exceptionallyCompose(ex -> onError(record, command, ex));
    }

    private CompletableFuture<Void> onSuccess(ConsumerRecord<String, String> record, ExtractFrameResult result) {
        return kafkaProducer.publishSuccess(result)
            .thenRun(() -> {
                System.out.printf(
                    "Frame generated for key=%s, topic=%s, partition=%d, offset=%d, output=%s, elapsed=%dms%n",
                    record.key(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    result.frameS3Path(),
                    result.elapsed().toMillis()
                );

                offsetAcks.add(new OffsetAck(record.topic(), record.partition(), record.offset() + 1));
            });
    }

    private CompletableFuture<Void> onError(ConsumerRecord<String, String> record, ExtractFrameCommand command, Throwable ex) {
        final var cause = ex.getCause() != null ? ex.getCause() : ex;

        return kafkaProducer.publishError(command, cause)
            .thenRun(() -> {
                System.err.printf(
                    "Error processing topic=%s partition=%d offset=%d videoId=%d error=%s%n",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    command.videoId(),
                    cause.getMessage()
                );

                offsetAcks.add(new OffsetAck(record.topic(), record.partition(), record.offset() + 1));
            });
    }

    private void commitCompletedOffsets(KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();

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

}
