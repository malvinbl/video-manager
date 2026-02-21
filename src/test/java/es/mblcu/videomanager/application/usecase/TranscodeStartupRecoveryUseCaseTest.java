package es.mblcu.videomanager.application.usecase;

import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobState;
import es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobStateRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TranscodeStartupRecoveryUseCaseTest {

    @Mock
    private TranscodeJobStateRepository repository;

    private TranscodeStartupRecoveryUseCase useCase;

    @BeforeEach
    void setUp() {
        useCase = new TranscodeStartupRecoveryUseCase(repository);
    }

    @Test
    void shouldReturnZeroWhenNoRunningJobs() {
        when(repository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(CompletableFuture.completedFuture(List.of()));

        int recovered = useCase.recoverRunningJobs().join();

        assertThat(recovered).isZero();
        verify(repository, never()).markError(any(), any(), any());
    }

    @Test
    void shouldMarkRunningJobsAsError() {
        final var running = new TranscodeJobState(
            "100|s3://bucket/transcoded/100|1280x720",
            100L,
            "s3://bucket/videos/video.mp4",
            "s3://bucket/transcoded/100",
            1280,
            720,
            JobStatus.RUNNING,
            null,
            null,
            Map.of()
        );

        when(repository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(CompletableFuture.completedFuture(List.of(running)));
        when(repository.markError(eq(running.jobId()), any(TranscodeVideoCommand.class), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        int recovered = useCase.recoverRunningJobs().join();

        assertThat(recovered).isEqualTo(1);

        final var captor = ArgumentCaptor.forClass(TranscodeVideoCommand.class);
        verify(repository).markError(eq(running.jobId()), captor.capture(), eq("Recovered at startup after previous unclean shutdown"));

        final var command = captor.getValue();
        assertThat(command.videoId()).isEqualTo(100L);
        assertThat(command.videoS3Path()).isEqualTo("s3://bucket/videos/video.mp4");
        assertThat(command.outputS3Prefix()).isEqualTo("s3://bucket/transcoded/100");
        assertThat(command.width()).isEqualTo(1280);
        assertThat(command.height()).isEqualTo(720);
    }

}
