package es.mblcu.videomanager.infrastructure.redis;

import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import es.mblcu.videomanager.domain.jobs.JobState;
import es.mblcu.videomanager.domain.jobs.RedisOps;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobStateRepositoryRedisAdapterTest {

    @Mock
    private RedisOps redisOps;

    private JobStateRepositoryRedisAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new JobStateRepositoryRedisAdapter("vm", redisOps);
    }

    @Test
    void shouldReturnEmptyWhenJobDoesNotExist() {
        when(redisOps.hgetall("vm:job:job-1")).thenReturn(CompletableFuture.completedFuture(Map.of()));

        Optional<JobState> state = adapter.findJob("job-1").join();

        assertTrue(state.isEmpty());
        verify(redisOps).hgetall("vm:job:job-1");
    }

    @Test
    void shouldMapJobStateFromRedisHash() {
        when(redisOps.hgetall("vm:job:job-1")).thenReturn(CompletableFuture.completedFuture(Map.of(
            "videoId", "101",
            "videoS3Path", "s3://bucket/videos/video.mp4",
            "frameS3Path", "s3://bucket/frames/frame.png",
            "status", "SUCCESS",
            "elapsedMillis", "12",
            "errorDescription", ""
        )));

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

        when(redisOps.hset(anyString(), anyMap())).thenReturn(CompletableFuture.completedFuture(1L));

        adapter.markSuccess("job-77", result).join();

        final var captor = ArgumentCaptor.forClass(Map.class);

        verify(redisOps).hset(anyString(), captor.capture());

        @SuppressWarnings("unchecked")
        Map<String, String> fields = (Map<String, String>) captor.getValue();

        assertEquals("77", fields.get("videoId"));
        assertEquals("s3://bucket/frames/frame.png", fields.get("frameS3Path"));
        assertEquals("SUCCESS", fields.get("status"));
        assertEquals("30", fields.get("elapsedMillis"));
    }

    @Test
    void shouldDeleteVideoRefKeyWhenDecrementGoesToZero() {
        String key = "vm:video:ref:s3://bucket/videos/video.mp4";

        when(redisOps.decr(key)).thenReturn(CompletableFuture.completedFuture(0L));
        when(redisOps.del(key)).thenReturn(CompletableFuture.completedFuture(1L));

        long value = adapter.decrementVideoRef("s3://bucket/videos/video.mp4").join();

        assertEquals(0L, value);
        verify(redisOps).decr(key);
        verify(redisOps).del(key);
    }

    @Test
    void shouldKeepVideoRefKeyWhenStillPositive() {
        String key = "vm:video:ref:s3://bucket/videos/video.mp4";

        when(redisOps.decr(key)).thenReturn(CompletableFuture.completedFuture(2L));

        long value = adapter.decrementVideoRef("s3://bucket/videos/video.mp4").join();

        assertEquals(2L, value);
        verify(redisOps).decr(key);
        verify(redisOps, never()).del(key);
    }

    @Test
    void shouldStoreRunningAndErrorStates() {
        final var command = new ExtractFrameCommand(
            55L,
            "s3://bucket/videos/video.mp4",
            "s3://bucket/frames/frame.png",
            1.0
        );

        when(redisOps.hset(anyString(), anyMap())).thenReturn(CompletableFuture.completedFuture(1L));

        adapter.markRunning("job-55", command).join();
        adapter.markError("job-55", command, "boom").join();

        final var captor = ArgumentCaptor.forClass(Map.class);

        verify(redisOps, times(2)).hset(anyString(), captor.capture());

        @SuppressWarnings("unchecked")
        Map<String, String> fields = (Map<String, String>) captor.getAllValues().get(1);

        assertEquals("ERROR", fields.get("status"));
        assertEquals("boom", fields.get("errorDescription"));
        assertEquals("55", fields.get("videoId"));
    }

}
