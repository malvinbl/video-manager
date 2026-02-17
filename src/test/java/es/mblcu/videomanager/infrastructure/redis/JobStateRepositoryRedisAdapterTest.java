package es.mblcu.videomanager.infrastructure.redis;

import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import es.mblcu.videomanager.domain.jobs.JobState;
import es.mblcu.videomanager.domain.jobs.RedisOps;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobStateRepositoryRedisAdapterTest {

    private FakeRedisOps redisOps;
    private JobStateRepositoryRedisAdapter adapter;

    @BeforeEach
    void setUp() {
        redisOps = new FakeRedisOps();
        adapter = new JobStateRepositoryRedisAdapter("vm", redisOps);
    }

    @Test
    void shouldReturnEmptyWhenJobDoesNotExist() {
        Optional<JobState> state = adapter.findJob("job-1").join();
        assertTrue(state.isEmpty());
    }

    @Test
    void shouldMapJobStateFromRedisHash() {
        redisOps.hashes.put("vm:job:job-1", Map.of(
            "videoId", "101",
            "videoS3Path", "s3://bucket/videos/video.mp4",
            "frameS3Path", "s3://bucket/frames/frame.png",
            "status", "SUCCESS",
            "elapsedMillis", "12",
            "errorDescription", ""
        ));

        Optional<JobState> state = adapter.findJob("job-1").join();

        assertTrue(state.isPresent());
        assertEquals("job-1", state.get().jobId());
        assertEquals(101L, state.get().videoId());
        assertEquals(JobStatus.SUCCESS, state.get().status());
        assertEquals(12L, state.get().elapsedMillis());
        assertNull(state.get().errorDescription());
    }

    @Test
    void shouldStoreSuccessJobState() {
        final var result = new ExtractFrameResult(77L, "s3://bucket/frames/frame.png", Duration.ofMillis(30));

        adapter.markSuccess("job-77", result).join();

        Map<String, String> fields = redisOps.hashes.get("vm:job:job-77");

        assertEquals("77", fields.get("videoId"));
        assertEquals("s3://bucket/frames/frame.png", fields.get("frameS3Path"));
        assertEquals("SUCCESS", fields.get("status"));
        assertEquals("30", fields.get("elapsedMillis"));
    }

    @Test
    void shouldDeleteVideoRefKeyWhenDecrementGoesToZero() {
        redisOps.counters.put("vm:video:ref:s3://bucket/videos/video.mp4", 1L);

        long value = adapter.decrementVideoRef("s3://bucket/videos/video.mp4").join();

        assertEquals(0L, value);
        assertFalse(redisOps.counters.containsKey("vm:video:ref:s3://bucket/videos/video.mp4"));
    }

    @Test
    void shouldKeepVideoRefKeyWhenStillPositive() {
        redisOps.counters.put("vm:video:ref:s3://bucket/videos/video.mp4", 3L);

        long value = adapter.decrementVideoRef("s3://bucket/videos/video.mp4").join();

        assertEquals(2L, value);
        assertEquals(2L, redisOps.counters.get("vm:video:ref:s3://bucket/videos/video.mp4"));
    }

    @Test
    void shouldStoreRunningAndErrorStates() {
        final var command = new ExtractFrameCommand(55L, "s3://bucket/videos/video.mp4", "s3://bucket/frames/frame.png", 1.0);

        adapter.markRunning("job-55", command).join();
        adapter.markError("job-55", command, "boom").join();

        Map<String, String> fields = redisOps.hashes.get("vm:job:job-55");

        assertEquals("ERROR", fields.get("status"));
        assertEquals("boom", fields.get("errorDescription"));
        assertEquals("55", fields.get("videoId"));
    }

    private static class FakeRedisOps implements RedisOps {

        private final Map<String, Map<String, String>> hashes = new ConcurrentHashMap<>();
        private final Map<String, Long> counters = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<Map<String, String>> hgetall(String key) {
            Map<String, String> value = hashes.get(key);
            if (value == null) {
                return CompletableFuture.completedFuture(Map.of());
            }
            return CompletableFuture.completedFuture(value);
        }

        @Override
        public CompletableFuture<Long> hset(String key, Map<String, String> fields) {
            hashes.put(key, new HashMap<>(fields));
            return CompletableFuture.completedFuture(1L);
        }

        @Override
        public CompletableFuture<Long> incr(String key) {
            long value = counters.getOrDefault(key, 0L) + 1L;
            counters.put(key, value);
            return CompletableFuture.completedFuture(value);
        }

        @Override
        public CompletableFuture<Long> decr(String key) {
            long value = counters.getOrDefault(key, 0L) - 1L;
            counters.put(key, value);
            return CompletableFuture.completedFuture(value);
        }

        @Override
        public CompletableFuture<Long> del(String key) {
            counters.remove(key);
            hashes.remove(key);
            return CompletableFuture.completedFuture(1L);
        }
    }

}
