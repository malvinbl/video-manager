package es.mblcu.videomanager.infrastructure.redis;

import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoResult;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TranscodeJobStateRepositoryRedisAdapterTest {

    @Mock
    private RedisAsyncCommands<String, String> commands;

    private TranscodeJobStateRepositoryRedisAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new TranscodeJobStateRepositoryRedisAdapter("vm:transcode", commands);
    }

    @Test
    void shouldReturnEmptyWhenJobDoesNotExist() {
        RedisFuture<Map<String, String>> future = redisFuture(Map.of());

        when(commands.hgetall("vm:transcode:job:job-1")).thenReturn(future);

        Optional<es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobState> state = adapter.findJob("job-1").join();

        assertThat(state).isEmpty();
    }

    @Test
    void shouldMapJobStateFromRedisHash() {
        RedisFuture<Map<String, String>> future = redisFuture(Map.of(
            "videoId", "101",
            "videoS3Path", "s3://bucket/videos/video.mp4",
            "outputS3Prefix", "s3://bucket/transcoded/101",
            "inputWidth", "1280",
            "inputHeight", "720",
            "status", "SUCCESS",
            "elapsedMillis", "12",
            "outputs", "{\"854x480\":\"s3://bucket/transcoded/101/854x480.mp4\"}",
            "errorDescription", ""
        ));

        when(commands.hgetall("vm:transcode:job:job-1")).thenReturn(future);

        Optional<es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobState> state = adapter.findJob("job-1").join();

        assertThat(state).isPresent();
        assertThat(state.get().videoId()).isEqualTo(101L);
        assertThat(state.get().status()).isEqualTo(JobStatus.SUCCESS);
        assertThat(state.get().outputs()).containsEntry("854x480", "s3://bucket/transcoded/101/854x480.mp4");
    }

    @Test
    void shouldStoreRunningAndSuccessStates() {
        final var command = new TranscodeVideoCommand(77L, "s3://bucket/videos/video.mp4", "s3://bucket/transcoded/77", 1280, 720);
        final var result = new TranscodeVideoResult(
            77L,
            Map.of("854x480", "s3://bucket/transcoded/77/854x480.mp4"),
            Duration.ofMillis(30)
        );
        RedisFuture<Long> future = redisFuture(1L);

        when(commands.hset(anyString(), anyMap())).thenReturn(future);

        adapter.markRunning("job-77", command).join();
        adapter.markSuccess("job-77", result).join();

        final var captor = ArgumentCaptor.forClass(Map.class);
        verify(commands, times(2)).hset(anyString(), captor.capture());

        @SuppressWarnings("unchecked")
        Map<String, String> successFields = (Map<String, String>) captor.getAllValues().get(1);

        assertThat(successFields.get("status")).isEqualTo("SUCCESS");
        assertThat(successFields.get("elapsedMillis")).isEqualTo("30");
        assertThat(successFields.get("outputs")).contains("854x480");
    }

    @Test
    void shouldFindJobsByStatus() {
        RedisFuture<List<String>> keysFuture = redisFuture(List.of("vm:transcode:job:job-1", "vm:transcode:job:job-2"));
        RedisFuture<Map<String, String>> job1Future = redisFuture(Map.of(
            "videoId", "1",
            "videoS3Path", "s3://bucket/videos/v1.mp4",
            "outputS3Prefix", "s3://bucket/transcoded/1",
            "inputWidth", "1280",
            "inputHeight", "720",
            "status", "RUNNING"
        ));
        RedisFuture<Map<String, String>> job2Future = redisFuture(Map.of(
            "videoId", "2",
            "videoS3Path", "s3://bucket/videos/v2.mp4",
            "outputS3Prefix", "s3://bucket/transcoded/2",
            "inputWidth", "1280",
            "inputHeight", "720",
            "status", "SUCCESS"
        ));

        when(commands.keys("vm:transcode:job:*")).thenReturn(keysFuture);
        when(commands.hgetall("vm:transcode:job:job-1")).thenReturn(job1Future);
        when(commands.hgetall("vm:transcode:job:job-2")).thenReturn(job2Future);

        var jobs = adapter.findJobsByStatus(JobStatus.RUNNING).join();

        assertThat(jobs).hasSize(1);
        assertThat(jobs.get(0).jobId()).isEqualTo("job-1");
    }

    private static <T> RedisFuture<T> redisFuture(T value) {
        @SuppressWarnings("unchecked")
        RedisFuture<T> future = mock(RedisFuture.class);
        when(future.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(value));
        return future;
    }

}
