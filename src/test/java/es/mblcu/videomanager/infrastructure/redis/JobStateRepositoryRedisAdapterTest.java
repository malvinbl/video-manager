package es.mblcu.videomanager.infrastructure.redis;

import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import es.mblcu.videomanager.domain.jobs.JobState;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobStateRepositoryRedisAdapterTest {

    @Mock
    private RedisAsyncCommands<String, String> commands;

    private JobStateRepositoryRedisAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new JobStateRepositoryRedisAdapter("vm", commands);
    }

    @Test
    void shouldReturnEmptyWhenJobDoesNotExist() {
        RedisFuture<Map<String, String>> future = redisFuture(Map.of());

        when(commands.hgetall("vm:job:job-1")).thenReturn(future);

        Optional<JobState> state = adapter.findJob("job-1").join();

        assertThat(state).isEmpty();
        verify(commands).hgetall("vm:job:job-1");
    }

    @Test
    void shouldMapJobStateFromRedisHash() {
        RedisFuture<Map<String, String>> future = redisFuture(Map.of(
            "videoId", "101",
            "videoS3Path", "s3://bucket/videos/video.mp4",
            "frameS3Path", "s3://bucket/frames/frame.png",
            "status", "SUCCESS",
            "elapsedMillis", "12",
            "errorDescription", ""
        ));

        when(commands.hgetall("vm:job:job-1")).thenReturn(future);

        Optional<JobState> state = adapter.findJob("job-1").join();

        assertThat(state).isPresent();
        assertThat(state.get().jobId()).isEqualTo("job-1");
        assertThat(state.get().videoId()).isEqualTo(101L);
        assertThat(state.get().status()).isEqualTo(JobStatus.SUCCESS);
        assertThat(state.get().elapsedMillis()).isEqualTo(12L);
        assertThat(state.get().errorDescription()).isNull();
    }

    @Test
    void shouldStoreSuccessJobState() {
        final var result = new ExtractFrameResult(77L, "s3://bucket/frames/frame.png", Duration.ofMillis(30));
        RedisFuture<Long> future = redisFuture(1L);

        when(commands.hset(anyString(), anyMap())).thenReturn(future);

        adapter.markSuccess("job-77", result).join();

        final var captor = ArgumentCaptor.forClass(Map.class);

        verify(commands).hset(anyString(), captor.capture());

        @SuppressWarnings("unchecked")
        Map<String, String> fields = (Map<String, String>) captor.getValue();

        assertThat(fields.get("videoId")).isEqualTo("77");
        assertThat(fields.get("frameS3Path")).isEqualTo("s3://bucket/frames/frame.png");
        assertThat(fields.get("status")).isEqualTo("SUCCESS");
        assertThat(fields.get("elapsedMillis")).isEqualTo("30");
    }

    @Test
    void shouldDeleteVideoRefKeyWhenDecrementGoesToZero() {
        String key = "vm:video:ref:s3://bucket/videos/video.mp4";
        RedisFuture<Long> decrFuture = redisFuture(0L);
        RedisFuture<Long> delFuture = redisFuture(1L);

        when(commands.decr(key)).thenReturn(decrFuture);
        when(commands.del(key)).thenReturn(delFuture);

        long value = adapter.decrementVideoRef("s3://bucket/videos/video.mp4").join();

        assertThat(value).isEqualTo(0L);
        verify(commands).decr(key);
        verify(commands).del(key);
    }

    @Test
    void shouldKeepVideoRefKeyWhenStillPositive() {
        String key = "vm:video:ref:s3://bucket/videos/video.mp4";
        RedisFuture<Long> future = redisFuture(2L);

        when(commands.decr(key)).thenReturn(future);

        long value = adapter.decrementVideoRef("s3://bucket/videos/video.mp4").join();

        assertThat(value).isEqualTo(2L);
        verify(commands).decr(key);
        verify(commands, never()).del(key);
    }

    @Test
    void shouldStoreRunningAndErrorStates() {
        final var command = new ExtractFrameCommand(
            55L,
            "s3://bucket/videos/video.mp4",
            "s3://bucket/frames/frame.png",
            1.0
        );
        RedisFuture<Long> future = redisFuture(1L);

        when(commands.hset(anyString(), anyMap())).thenReturn(future);

        adapter.markRunning("job-55", command).join();
        adapter.markError("job-55", command, "boom").join();

        final var captor = ArgumentCaptor.forClass(Map.class);

        verify(commands, times(2)).hset(anyString(), captor.capture());

        @SuppressWarnings("unchecked")
        Map<String, String> fields = (Map<String, String>) captor.getAllValues().get(1);

        assertThat(fields.get("status")).isEqualTo("ERROR");
        assertThat(fields.get("errorDescription")).isEqualTo("boom");
        assertThat(fields.get("videoId")).isEqualTo("55");
    }

    @Test
    void shouldFindJobsByStatus() {
        RedisFuture<List<String>> keysFuture = redisFuture(List.of("vm:job:job-1", "vm:job:job-2"));
        RedisFuture<Map<String, String>> job1Future = redisFuture(Map.of(
            "videoId", "1",
            "videoS3Path", "s3://bucket/videos/v1.mp4",
            "frameS3Path", "s3://bucket/frames/f1.png",
            "status", "RUNNING"
        ));
        RedisFuture<Map<String, String>> job2Future = redisFuture(Map.of(
            "videoId", "2",
            "videoS3Path", "s3://bucket/videos/v2.mp4",
            "frameS3Path", "s3://bucket/frames/f2.png",
            "status", "SUCCESS"
        ));

        when(commands.keys("vm:job:*")).thenReturn(keysFuture);
        when(commands.hgetall("vm:job:job-1")).thenReturn(job1Future);
        when(commands.hgetall("vm:job:job-2")).thenReturn(job2Future);

        List<JobState> jobs = adapter.findJobsByStatus(JobStatus.RUNNING).join();

        assertThat(jobs).hasSize(1);
        assertThat(jobs.get(0).jobId()).isEqualTo("job-1");
        assertThat(jobs.get(0).status()).isEqualTo(JobStatus.RUNNING);
    }

    private static <T> RedisFuture<T> redisFuture(T value) {
        @SuppressWarnings("unchecked")
        RedisFuture<T> future = mock(RedisFuture.class);
        when(future.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(value));
        return future;
    }

}
