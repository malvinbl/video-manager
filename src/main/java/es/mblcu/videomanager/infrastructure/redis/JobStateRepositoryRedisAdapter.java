package es.mblcu.videomanager.infrastructure.redis;

import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import es.mblcu.videomanager.domain.jobs.JobState;
import es.mblcu.videomanager.domain.jobs.JobStateRepository;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.StatefulRedisConnection;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class JobStateRepositoryRedisAdapter implements JobStateRepository, AutoCloseable {

    private final String keyPrefix;
    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisAsyncCommands<String, String> commands;

    public JobStateRepositoryRedisAdapter(RedisConfig config) {
        this(config.keyPrefix(), RedisClient.create(config.uri()));
    }

    JobStateRepositoryRedisAdapter(String keyPrefix, RedisClient redisClient) {
        this.keyPrefix = keyPrefix;
        this.redisClient = redisClient;
        this.connection = redisClient.connect();
        this.commands = this.connection.async();
    }

    JobStateRepositoryRedisAdapter(String keyPrefix, RedisAsyncCommands<String, String> commands) {
        this.keyPrefix = keyPrefix;
        this.redisClient = null;
        this.connection = null;
        this.commands = commands;
    }

    @Override
    public CompletableFuture<Optional<JobState>> findJob(String jobId) {
        return commands.hgetall(jobKey(jobId)).toCompletableFuture()
            .thenApply(map -> map == null || map.isEmpty() ? Optional.empty() : Optional.of(mapToState(jobId, map)));
    }

    @Override
    public CompletableFuture<Void> markRunning(String jobId, ExtractFrameCommand command) {
        Map<String, String> fields = Map.of(
            "videoId", String.valueOf(command.videoId()),
            "videoS3Path", command.videoS3Path(),
            "frameS3Path", command.frameS3Path(),
            "status", JobStatus.RUNNING.name()
        );

        return commands.hset(jobKey(jobId), fields).toCompletableFuture().thenApply(ignore -> null);
    }

    @Override
    public CompletableFuture<Void> markSuccess(String jobId, ExtractFrameResult result) {
        Map<String, String> fields = Map.of(
            "videoId", String.valueOf(result.videoId()),
            "frameS3Path", result.frameS3Path(),
            "status", JobStatus.SUCCESS.name(),
            "elapsedMillis", String.valueOf(result.elapsed().toMillis()),
            "errorDescription", ""
        );

        return commands.hset(jobKey(jobId), fields).toCompletableFuture().thenApply(ignore -> null);
    }

    @Override
    public CompletableFuture<Void> markError(String jobId, ExtractFrameCommand command, String errorDescription) {
        Map<String, String> fields = Map.of(
            "videoId", String.valueOf(command.videoId()),
            "videoS3Path", command.videoS3Path(),
            "frameS3Path", command.frameS3Path(),
            "status", JobStatus.ERROR.name(),
            "errorDescription", errorDescription == null ? "" : errorDescription
        );

        return commands.hset(jobKey(jobId), fields).toCompletableFuture().thenApply(ignore -> null);
    }

    @Override
    public CompletableFuture<Long> incrementVideoRef(String videoS3Path) {
        return commands.incr(videoRefKey(videoS3Path)).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Long> decrementVideoRef(String videoS3Path) {
        String key = videoRefKey(videoS3Path);

        return commands.decr(key).toCompletableFuture()
            .thenCompose(value -> {
                if (value <= 0) {
                    return commands.del(key).toCompletableFuture().thenApply(ignored -> 0L);
                }
                return CompletableFuture.completedFuture(value);
            });
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

    private JobState mapToState(String jobId, Map<String, String> map) {
        Long videoId = parseLong(map.get("videoId"));
        String videoS3Path = map.get("videoS3Path");
        String frameS3Path = map.get("frameS3Path");
        JobStatus status = JobStatus.valueOf(map.getOrDefault("status", JobStatus.ERROR.name()));
        Long elapsedMillis = parseLong(map.get("elapsedMillis"));
        String errorDescription = emptyToNull(map.get("errorDescription"));

        return new JobState(jobId, videoId, videoS3Path, frameS3Path, status, elapsedMillis, errorDescription);
    }

    private Long parseLong(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return Long.parseLong(value);
    }

    private String emptyToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value;
    }

    private String jobKey(String jobId) {
        return keyPrefix + ":job:" + jobId;
    }

    private String videoRefKey(String videoS3Path) {
        return keyPrefix + ":video:ref:" + videoS3Path;
    }

}
