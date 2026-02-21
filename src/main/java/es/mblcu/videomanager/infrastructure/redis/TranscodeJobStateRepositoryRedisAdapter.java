package es.mblcu.videomanager.infrastructure.redis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoResult;
import es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobState;
import es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobStateRepository;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class TranscodeJobStateRepositoryRedisAdapter implements TranscodeJobStateRepository, AutoCloseable {

    private final String keyPrefix;
    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisAsyncCommands<String, String> commands;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TranscodeJobStateRepositoryRedisAdapter(RedisConfig config) {
        this(config.keyPrefix() + ":transcode", RedisClient.create(config.uri()));
    }

    TranscodeJobStateRepositoryRedisAdapter(String keyPrefix, RedisClient redisClient) {
        this.keyPrefix = keyPrefix;
        this.redisClient = redisClient;
        this.connection = redisClient.connect();
        this.commands = this.connection.async();
    }

    TranscodeJobStateRepositoryRedisAdapter(String keyPrefix, RedisAsyncCommands<String, String> commands) {
        this.keyPrefix = keyPrefix;
        this.redisClient = null;
        this.connection = null;
        this.commands = commands;
    }

    @Override
    public CompletableFuture<Optional<TranscodeJobState>> findJob(String jobId) {
        return commands.hgetall(jobKey(jobId)).toCompletableFuture()
            .thenApply(map -> map == null || map.isEmpty() ? Optional.empty() : Optional.of(mapToState(jobId, map)));
    }

    @Override
    public CompletableFuture<List<TranscodeJobState>> findJobsByStatus(JobStatus status) {
        return commands.keys(keyPrefix + ":job:*").toCompletableFuture()
            .thenCompose(keys -> {
                if (keys == null || keys.isEmpty()) {
                    return CompletableFuture.completedFuture(List.of());
                }

                List<CompletableFuture<Optional<TranscodeJobState>>> futures = new ArrayList<>(keys.size());
                for (String key : keys) {
                    String jobId = keyToJobId(key);
                    CompletableFuture<Optional<TranscodeJobState>> future = commands.hgetall(key).toCompletableFuture()
                        .thenApply(map -> map == null || map.isEmpty() ? Optional.<TranscodeJobState>empty() : Optional.of(mapToState(jobId, map)));
                    futures.add(future);
                }

                CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));

                return all.thenApply(v -> futures.stream()
                    .map(CompletableFuture::join)
                    .flatMap(Optional::stream)
                    .filter(job -> job.status() == status)
                    .toList());
            });
    }

    @Override
    public CompletableFuture<Void> markRunning(String jobId, TranscodeVideoCommand command) {
        Map<String, String> fields = Map.of(
            "videoId", String.valueOf(command.videoId()),
            "videoS3Path", command.videoS3Path(),
            "outputS3Prefix", command.outputS3Prefix(),
            "inputWidth", String.valueOf(command.width()),
            "inputHeight", String.valueOf(command.height()),
            "status", JobStatus.RUNNING.name()
        );

        return commands.hset(jobKey(jobId), fields).toCompletableFuture().thenApply(ignore -> null);
    }

    @Override
    public CompletableFuture<Void> markSuccess(String jobId, TranscodeVideoResult result) {
        final String outputsJson;
        try {
            outputsJson = objectMapper.writeValueAsString(result.outputs());
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }

        Map<String, String> fields = Map.of(
            "videoId", String.valueOf(result.videoId()),
            "outputs", outputsJson,
            "status", JobStatus.SUCCESS.name(),
            "elapsedMillis", String.valueOf(result.elapsed().toMillis()),
            "errorDescription", ""
        );

        return commands.hset(jobKey(jobId), fields).toCompletableFuture().thenApply(ignore -> null);
    }

    @Override
    public CompletableFuture<Void> markError(String jobId, TranscodeVideoCommand command, String errorDescription) {
        Map<String, String> fields = Map.of(
            "videoId", String.valueOf(command.videoId()),
            "videoS3Path", command.videoS3Path(),
            "outputS3Prefix", command.outputS3Prefix(),
            "inputWidth", String.valueOf(command.width()),
            "inputHeight", String.valueOf(command.height()),
            "status", JobStatus.ERROR.name(),
            "errorDescription", errorDescription == null ? "" : errorDescription
        );

        return commands.hset(jobKey(jobId), fields).toCompletableFuture().thenApply(ignore -> null);
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    private TranscodeJobState mapToState(String jobId, Map<String, String> map) {
        return new TranscodeJobState(
            jobId,
            parseLong(map.get("videoId")),
            map.get("videoS3Path"),
            map.get("outputS3Prefix"),
            parseInt(map.get("inputWidth")),
            parseInt(map.get("inputHeight")),
            JobStatus.valueOf(map.getOrDefault("status", JobStatus.ERROR.name())),
            parseLong(map.get("elapsedMillis")),
            emptyToNull(map.get("errorDescription")),
            parseOutputs(map.get("outputs"))
        );
    }

    private Map<String, String> parseOutputs(String raw) {
        if (StringUtils.isEmpty(raw)) {
            return Map.of();
        }

        try {
            return objectMapper.readValue(raw, new TypeReference<>() {
            });
        } catch (Exception ex) {
            return Map.of();
        }
    }

    private Long parseLong(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        return Long.parseLong(value);
    }

    private Integer parseInt(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        return Integer.parseInt(value);
    }

    private String emptyToNull(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        return value;
    }

    private String jobKey(String jobId) {
        return keyPrefix + ":job:" + jobId;
    }

    private String keyToJobId(String fullKey) {
        String prefix = keyPrefix + ":job:";
        return fullKey.startsWith(prefix) ? fullKey.substring(prefix.length()) : fullKey;
    }

}
